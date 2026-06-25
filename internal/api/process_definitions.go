package api

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/parser"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type ProcessDefinitionController struct {
	db          *sqlx.DB
	util        common.Util
	processRepo *repository.ProcessRepository
	dispatcher  *events.TaskEventDispatcher
}

func NewProcessDefinitionController(db *sqlx.DB, rootDirPath *string) *ProcessDefinitionController {
	return &ProcessDefinitionController{
		db:          db,
		util:        *common.NewUtil(rootDirPath),
		processRepo: repository.NewProcessRepository(db),
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

	fh := fileHeaders[0]
	file, err := fh.Open()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to open file"})
	}
	defer file.Close()

	bpmnBytes, err := io.ReadAll(file)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to read file"})
	}

	// ------------------------------------------------------------
	// 3. Extract deployment metadata
	// ------------------------------------------------------------
	deploymentName := c.FormValue("deployment-name")
	if deploymentName == "" {
		deploymentName = fh.Filename
	}
	tenantID := c.FormValue("tenant-id") // optional

	// ------------------------------------------------------------
	// 4. Detect engine type (Camunda 7 / Camunda 8)
	// ------------------------------------------------------------
	engineType := parser.DetectEngine(bpmnBytes)

	// ------------------------------------------------------------
	// 5. Create deployment record in DB
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
	// 6. Extract all process keys from the BPMN file
	// ------------------------------------------------------------
	processKeys, err := extractProcessKeys(bpmnBytes)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	// ------------------------------------------------------------
	// 7. Store each process definition
	// ------------------------------------------------------------
	type ProcessDefInfo struct {
		ID           string `json:"id"`
		Key          string `json:"key"`
		Version      int    `json:"version"`
		ResourceName string `json:"resourceName"`
	}
	deployedProcesses := make(map[string]ProcessDefInfo)

	for _, procKey := range processKeys {
		// Build graph for this specific process
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

		resourceName := fmt.Sprintf("%s.bpmn", procKey)

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
			ResourceName: resourceName,
		}
	}

	// ------------------------------------------------------------
	// 8. Return Camunda‑style response
	// ------------------------------------------------------------
	return c.Status(200).JSON(fiber.Map{
		"id":                         deploy.ID,
		"name":                       deploymentName,
		"deployedAt":                 time.Now().Format(time.RFC3339),
		"deployedProcessDefinitions": deployedProcesses,
	})
}

// GET /engine-rest/process-definition
// Query params: key, latestVersion (bool)
func (ctrl *ProcessDefinitionController) ListDefinitions(c *fiber.Ctx) error {
	key := c.Query("key")
	latestOnly := c.Query("latestVersion") == "true"

	var defs []models.ProcessDefinition
	var err error

	if key != "" && latestOnly {
		def, e := ctrl.processRepo.FindLatestProcessDefinitionByKey(key)
		if e != nil {
			return c.Status(404).JSON(fiber.Map{"message": "not found"})
		}
		defs = []models.ProcessDefinition{def}
	} else if key != "" {
		defs, err = ctrl.processRepo.ListProcessDefinitionsByKey(key)
	} else if latestOnly {
		defs, err = ctrl.processRepo.ListLatestProcessDefinitions()
	} else {
		defs, err = ctrl.processRepo.ListProcessDefinitions()
	}

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": err.Error()})
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

	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to parse process graph"})
	}

	var startNodeID string
	for _, node := range graph.Nodes {
		if node.Type == engine.StartEventType {
			startNodeID = node.ID
			break
		}
	}
	if startNodeID == "" {
		return c.Status(500).JSON(fiber.Map{"message": "no start event in process definition"})
	}

	instanceRepo := repository.NewProcessInstanceRepository(ctrl.db)
	tx, err := ctrl.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to begin transaction"})
	}
	defer tx.Rollback()

	instanceID := uuid.New()
	now := time.Now()

	if err := instanceRepo.CreateProcessInstanceTx(tx, instanceID, def.ID, now); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to create process instance", "error": err.Error()})
	}
	if len(body.Variables) > 0 {
		if err := instanceRepo.CreateVariablesTx(tx, instanceID, body.Variables, now); err != nil {
			return c.Status(500).JSON(fiber.Map{"message": "failed to save variables", "error": err.Error()})
		}
	}

	execID := uuid.New()
	if err := instanceRepo.CreateExecutionTx(tx, execID, instanceID, startNodeID, now); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to create execution", "error": err.Error()})
	}
	if err := tx.Commit(); err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to commit"})
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
