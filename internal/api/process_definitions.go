package api

import (
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
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

func (processDef ProcessDefinitionController) DeployBPMN(c *fiber.Ctx) error {
	// 1. Parse multipart form.
	form, err := c.MultipartForm()
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "not a valid multipart request"})
	}

	// 2. Get uploaded files (supports Camunda 7 dynamic field & Camunda 8 "resources").
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

	// 3. Extract deployment metadata.
	deploymentName := c.FormValue("deployment-name")
	if deploymentName == "" {
		deploymentName = fileHeaders[0].Filename
	}
	tenantID := c.FormValue("tenant-id") // optional; defaults to the caller's own tenant
	if tenantID == "" {
		tenantID = common.GetTenantId(c)
	}

	// 4. Read each uploaded file into a RawResource.
	resources := make([]service.RawResource, 0, len(fileHeaders))
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
		resources = append(resources, service.RawResource{Filename: fh.Filename, Content: fileBytes})
	}

	// 5. Deploy (shared with the gRPC DeployResource RPC).
	deployerID, _ := uuid.Parse(common.GetUserUuid(c))
	result, err := service.DeployResources(processDef.db, deploymentName, tenantID, resources, deployerID)
	if err != nil {
		if errors.Is(err, service.ErrDeployPermissionDenied) {
			return c.Status(403).JSON(fiber.Map{"error": err.Error()})
		}
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// 6. Return Camunda-style response.
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
	for _, d := range result.ProcessDefinitions {
		deployedProcesses[d.Key] = ProcessDefInfo{ID: d.ID.String(), Key: d.Key, Version: d.Version, ResourceName: d.ResourceName}
	}
	deployedDecisions := make(map[string]DecisionInfo)
	for _, d := range result.Decisions {
		deployedDecisions[d.Key] = DecisionInfo{ID: d.ID.String(), Key: d.Key, Version: d.Version, ResourceName: d.ResourceName}
	}
	deployedForms := make(map[string]FormInfo)
	for _, d := range result.Forms {
		deployedForms[d.Key] = FormInfo{ID: d.ID.String(), Key: d.Key, Version: d.Version, ResourceName: d.ResourceName}
	}

	return c.Status(200).JSON(fiber.Map{
		"id":                         result.DeploymentID,
		"name":                       result.DeploymentName,
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
