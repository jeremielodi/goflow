package api

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/dmn"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/parser"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type ProcessDefinitionController struct {
	db           *sqlx.DB
	util         common.Util
	processRepo  *repository.ProcessRepository
	dispatcher   *events.TaskEventDispatcher
	auditService *service.AuditService
}

func NewProcessDefinitionController(db *sqlx.DB, rootDirPath *string) *ProcessDefinitionController {
	return &ProcessDefinitionController{
		db:           db,
		util:         *common.NewUtil(rootDirPath),
		processRepo:  repository.NewProcessRepository(db),
		auditService: service.NewAuditService(db),
	}
}

func (ctrl *ProcessDefinitionController) SetDispatcher(d *events.TaskEventDispatcher) {
	ctrl.dispatcher = d
}

type ProcessDefinition struct {
	ID         string
	Key        string
	Name       string
	BPMN       string
	TenantID   *string
	EngineType string
	Graph      *engine.ProcessGraph
	CreatedAt  time.Time
}

func extractProcessKeys(xmlBytes []byte) ([]string, error) {
	type Process struct {
		ID string `xml:"id,attr"`
	}
	type Definitions struct {
		Processes []Process `xml:"process"`
	}
	var def Definitions
	err := xml.Unmarshal(xmlBytes, &def)
	if err != nil {
		return nil, err
	}
	if len(def.Processes) == 0 {
		return nil, fmt.Errorf("no process elements found in BPMN")
	}
	keys := make([]string, len(def.Processes))
	for i, p := range def.Processes {
		keys[i] = p.ID
	}
	return keys, nil
}

func (processDef ProcessDefinitionController) DeployBPMN(c *fiber.Ctx) error {
	// ------------------------------------------------------------
	// 1. Parse multipart form
	// ------------------------------------------------------------
	form, err := c.MultipartForm()
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "not a valid multipart request"})
	}

	// ------------------------------------------------------------
	// 2. Get uploaded file (supports Camunda 7 dynamic field & Camunda 8 "resources")
	// ------------------------------------------------------------
	var fileHeaders []*multipart.FileHeader
	if headers, ok := form.File["resources"]; ok && len(headers) > 0 {
		fileHeaders = headers
	} else {
		for _, headers := range form.File {
			if len(headers) > 0 {
				fileHeaders = headers
				break
			}
		}
	}
	if len(fileHeaders) == 0 {
		return c.Status(400).JSON(fiber.Map{
			"error":   "no BPMN file uploaded",
			"details": "expected field 'resources' or a file field",
		})
	}

	// ------------------------------------------------------------
	// 3. Extract deployment metadata
	// ------------------------------------------------------------
	deploymentName := c.FormValue("deployment-name")
	if deploymentName == "" {
		deploymentName = fileHeaders[0].Filename
	}
	tenantID := c.FormValue("tenant-id") // optional; defaults to the caller's own tenant
	if tenantID == "" {
		tenantID = common.GetTenantId(c)
	}

	// ------------------------------------------------------------
	// 4. Create deployment record in DB
	// ------------------------------------------------------------
	deploymentRepo := repository.NewProcessRepository(processDef.db)
	_, err = deploymentRepo.CreateDeployment(models.DeploymentCreateModel{
		Name:       deploymentName,
		DeployedBy: nil,
		Status:     "active",
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error":  "failed to create deployment",
			"detail": err.Error(),
		})
	}

	deploy, err := deploymentRepo.FindDeploymentByName(deploymentName)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": "deployment created but not found",
		})
	}

	// ------------------------------------------------------------
	// 5. Process every uploaded resource — .bpmn files become process
	//    definitions, .dmn files become decisions (both can be deployed
	//    together in one request, as Camunda 8 allows).
	// ------------------------------------------------------------
	type ProcessDefInfo struct {
		ID           string `json:"id"`
		Key          string `json:"key"`
		Version      int    `json:"version"`
		ResourceName string `json:"resourceName"`
	}
	type DecisionInfo struct {
		ID           string `json:"id"`
		Key          string `json:"key"`
		Version      int    `json:"version"`
		ResourceName string `json:"resourceName"`
	}
	type FormInfo struct {
		ID           string `json:"id"`
		Key          string `json:"key"`
		Version      int    `json:"version"`
		ResourceName string `json:"resourceName"`
	}
	deployedProcesses := make(map[string]ProcessDefInfo)
	deployedDecisions := make(map[string]DecisionInfo)
	deployedForms := make(map[string]FormInfo)
	dmnRepo := repository.NewDMNRepository(processDef.db)
	formRepo := repository.NewFormRepository(processDef.db)

	for _, fh := range fileHeaders {
		file, err := fh.Open()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "failed to open file " + fh.Filename})
		}
		fileBytes, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "failed to read file " + fh.Filename})
		}

		if strings.HasSuffix(strings.ToLower(fh.Filename), ".form") {
			var schema struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(fileBytes, &schema); err != nil || schema.ID == "" {
				return c.Status(400).JSON(fiber.Map{"error": fmt.Sprintf("failed to parse form %s: expected JSON with an \"id\" field", fh.Filename)})
			}
			formID, version, err := formRepo.CreateForm(models.FormCreateModel{
				DeploymentID: deploy.ID,
				FormId:       schema.ID,
				Schema:       fileBytes,
			})
			if err != nil {
				return c.Status(500).JSON(fiber.Map{"error": fmt.Sprintf("failed to store form %s: %v", schema.ID, err)})
			}
			deployedForms[schema.ID] = FormInfo{
				ID:           formID.String(),
				Key:          schema.ID,
				Version:      version,
				ResourceName: fh.Filename,
			}
			continue
		}

		if strings.HasSuffix(strings.ToLower(fh.Filename), ".dmn") {
			defs, err := dmn.ParseDMN(fileBytes)
			if err != nil {
				return c.Status(400).JSON(fiber.Map{"error": fmt.Sprintf("failed to parse DMN %s: %v", fh.Filename, err)})
			}
			for _, decision := range defs.Decisions {
				if decision.DecisionTable == nil {
					continue
				}
				decisionJSON, err := json.Marshal(decision)
				if err != nil {
					return c.Status(500).JSON(fiber.Map{"error": fmt.Sprintf("failed to marshal decision %s", decision.ID)})
				}
				decisionID, version, err := dmnRepo.CreateDecision(models.DMNDecisionCreateModel{
					DeploymentID: deploy.ID,
					DecisionKey:  decision.ID,
					DecisionName: &decision.Name,
					DmnXML:       string(fileBytes),
					ParsedTable:  decisionJSON,
				})
				if err != nil {
					return c.Status(500).JSON(fiber.Map{"error": fmt.Sprintf("failed to store decision %s: %v", decision.ID, err)})
				}
				deployedDecisions[decision.ID] = DecisionInfo{
					ID:           decisionID.String(),
					Key:          decision.ID,
					Version:      version,
					ResourceName: fh.Filename,
				}
			}
			continue
		}

		// ---- BPMN resource ----
		bpmnBytes := fileBytes
		engineType := parser.DetectEngine(bpmnBytes)

		processKeys, err := extractProcessKeys(bpmnBytes)
		if err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		// Deploying a brand-new process key still requires the global
		// CAN_MANAGE_DEPLOY_PROCESS action (same as always — this is not
		// resource-scoped since the resource doesn't exist yet). Deploying
		// a new version of an EXISTING key requires MANAGE on that key,
		// which a per-resource grant can satisfy without the global
		// action — see CanAccessProcess's bypass-or-grant logic.
		if deployerID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
			actionRepo := repository.NewActionRepository(processDef.db)
			for _, procKey := range processKeys {
				if _, findErr := deploymentRepo.FindLatestProcessDefinitionByKey(procKey); findErr != nil {
					hasGlobal, _ := actionRepo.UserHasPermission(deployerID, "CAN_MANAGE_DEPLOY_PROCESS")
					if !hasGlobal {
						return c.Status(403).JSON(fiber.Map{"error": fmt.Sprintf("you do not have permission to deploy new process %s", procKey)})
					}
					continue
				}
				allowed, permErr := service.CanAccessProcess(processDef.db, deployerID, procKey, "MANAGE")
				if permErr == nil && !allowed {
					return c.Status(403).JSON(fiber.Map{"error": fmt.Sprintf("you do not have MANAGE access to process %s", procKey)})
				}
			}
		}

		for _, procKey := range processKeys {
			graph, err := engine.BuildGraphForProcess(bpmnBytes, procKey)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{
					"error": fmt.Sprintf("failed to build graph for process %s: %v", procKey, err),
				})
			}

			graphJSON, err := json.Marshal(graph)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{
					"error": fmt.Sprintf("failed to marshal graph for %s", procKey),
				})
			}

			defID, version, err := deploymentRepo.CreateProcessDefinition(
				models.ProcessDefinitionCreateModel{
					DeploymentID: deploy.ID,
					ProcessKey:   procKey,
					ProcessName:  &procKey,
					IsActive:     true,
					BpmnXML:      string(bpmnBytes),
					ParsedGraph:  graphJSON,
					TenantID:     &tenantID,
					EngineType:   string(engineType),
				},
			)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{
					"error": fmt.Sprintf("failed to store definition %s: %v", procKey, err),
				})
			}

			deployedProcesses[procKey] = ProcessDefInfo{
				ID:           defID,
				Key:          procKey,
				Version:      version,
				ResourceName: fmt.Sprintf("%s.bpmn", procKey),
			}
		}
	}

	// ------------------------------------------------------------
	// 6. Return Camunda‑style response
	// ------------------------------------------------------------
	return c.Status(200).JSON(fiber.Map{
		"id":                         deploy.ID,
		"name":                       deploymentName,
		"deployedAt":                 time.Now().Format(time.RFC3339),
		"deployedProcessDefinitions": deployedProcesses,
		"deployedDecisions":          deployedDecisions,
		"deployedForms":              deployedForms,
	})
}

// GET /engine-rest/process-definition
// Query params: key, latestVersion (bool)
func (ctrl *ProcessDefinitionController) ListDefinitions(c *fiber.Ctx) error {
	key := c.Query("key")
	latestOnly := c.Query("latestVersion") == "true"
	tenantID := common.GetTenantId(c)

	var defs []models.ProcessDefinition
	var err error

	if key != "" && latestOnly {
		def, e := ctrl.processRepo.FindLatestProcessDefinitionByKey(key)
		if e != nil || (tenantID != "" && def.TenantID != nil && *def.TenantID != tenantID) {
			return c.Status(404).JSON(fiber.Map{"message": "not found"})
		}
		defs = []models.ProcessDefinition{def}
	} else if key != "" {
		defs, err = ctrl.processRepo.ListProcessDefinitionsByKey(key, tenantID)
	} else if latestOnly {
		defs, err = ctrl.processRepo.ListLatestProcessDefinitions(tenantID)
	} else {
		defs, err = ctrl.processRepo.ListProcessDefinitions(tenantID)
	}

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		visible := defs[:0]
		for _, def := range defs {
			if allowed, _ := service.CanAccessProcess(ctrl.db, userID, def.ProcessKey, "VIEW"); allowed {
				visible = append(visible, def)
			}
		}
		defs = visible
	}

	return c.JSON(defs)
}

// GET /engine-rest/process-definition/:id
func (ctrl *ProcessDefinitionController) GetDefinition(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"message": "invalid id"})
	}
	def, err := ctrl.processRepo.FindProcessDefinitionByID(id)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"message": "not found"})
	}

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(ctrl.db, userID, def.ProcessKey, "VIEW"); permErr == nil && !allowed {
			return c.Status(404).JSON(fiber.Map{"message": "not found"})
		}
	}

	return c.JSON(def)
}

// POST /engine-rest/process-definition/:id/start
// Starts a specific version of a process definition by its UUID.
func (ctrl *ProcessDefinitionController) StartByDefinitionID(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"message": "invalid definition id"})
	}

	var body struct {
		Variables map[string]interface{} `json:"variables"`
	}
	if parseErr := c.BodyParser(&body); parseErr != nil {
		body.Variables = map[string]interface{}{}
	}

	def, err := ctrl.processRepo.FindProcessDefinitionByID(id)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"message": "process definition not found"})
	}

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(ctrl.db, userID, def.ProcessKey, "START"); permErr == nil && !allowed {
			return c.Status(403).JSON(fiber.Map{"message": "you do not have START access to this process"})
		}
	}

	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to parse process graph"})
	}

	instanceID, execID, err := service.StartProcessInstance(ctrl.db, &graph, def.ID, common.InstanceTenant(def.TenantID, common.GetTenantId(c)), body.Variables)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
	}

	rt := runtime.NewRuntime(&graph, ctrl.db, ctrl.dispatcher)
	if err := rt.ExecuteExecution(c.Context(), execID); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "execution failed", "error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"processInstanceId": instanceID,
		"executionId":       execID,
		"processKey":        def.ProcessKey,
		"version":           def.Version,
		"definitionId":      def.ID,
	})
}
