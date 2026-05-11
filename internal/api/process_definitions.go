package api

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/parser"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type ProcessDefinitionController struct {
	db   *sqlx.DB
	util common.Util
}

func NewProcessDefinitionController(db *sqlx.DB, rootDirPath *string) *ProcessDefinitionController {
	return &ProcessDefinitionController{
		db:   db,
		util: *common.NewUtil(rootDirPath),
	}
}

// Simple in-memory repo (replace later with PostgreSQL repo)
var processDefinitions = map[string]ProcessDefinition{}

type ProcessDefinition struct {
	ID        string
	Key       string
	Name      string
	BPMN      string
	TenantID  *string
	Graph     *engine.ProcessGraph
	CreatedAt time.Time
}

func extractProcessKey(xmlBytes []byte) (string, error) {
	type Process struct {
		ID string `xml:"id,attr"`
	}
	type Definitions struct {
		Process Process `xml:"process"`
	}
	var def Definitions
	err := xml.Unmarshal(xmlBytes, &def)
	if err != nil {
		return "", err
	}
	if def.Process.ID == "" {
		return "", fmt.Errorf("no process id found in BPMN")
	}
	return def.Process.ID, nil
}

func (processDef ProcessDefinitionController) DeployBPMN(c *fiber.Ctx) error {
	// 1. Parse multipart form
	form, err := c.MultipartForm()
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error": "not a valid multipart request",
		})
	}

	// 2. Get files under field "resources"
	fileHeaders, ok := form.File["resources"]
	if !ok || len(fileHeaders) == 0 {
		return c.Status(400).JSON(fiber.Map{
			"error":   "no BPMN file uploaded",
			"details": "expected field name 'resources' (sent by Camunda Modeler)",
		})
	}

	// 3. Process first file (or loop if you support multiple)
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

	// 4. Extract metadata from other form fields (optional)
	deploymentName := c.FormValue("deployment-name")
	if deploymentName == "" {
		deploymentName = fh.Filename
	}
	tenantID := c.FormValue("tenant-id") // Camunda may send this

	// 5. Parse and store locally (your existing logic)
	defs, err := parser.ParseBPMN(bpmnBytes)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error":  "invalid BPMN XML",
			"detail": err.Error(),
		})
	}

	graph, err := engine.BuildGraph(defs)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": "failed to build process graph",
		})
	}

	definitionID := uuid.New().String()
	processDefinitions[definitionID] = ProcessDefinition{
		ID:        definitionID,
		Key:       deploymentName,
		Name:      deploymentName,
		BPMN:      string(bpmnBytes),
		TenantID:  &tenantID,
		Graph:     graph,
		CreatedAt: time.Now(),
	}

	deploymentRepo := repository.NewProcessRepository(processDef.db)
	_, err = deploymentRepo.CreateDeployment(models.DeploymentCreateModel{
		Name:       deploymentName,
		DeployedBy: nil, // or get from auth context
		Status:     "active",
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Error",
			"content": "Failed to create deployement in db",
		})
	}

	// Get the deployment ID (you may retrieve from result or re‑fetch)
	// For simplicity, use FindDeploymentByName
	deploy, _ := deploymentRepo.FindDeploymentByName(deploymentName)

	// Parse BPMN and build graph (as before)
	graphJSON, _ := json.Marshal(graph) // store parsed graph as JSONB

	extractedKey, err := extractProcessKey(bpmnBytes)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"title": "Error", "content": "Failed to extract the proceess key"})
	}

	// Save process definition
	definitionID, version, err := deploymentRepo.CreateProcessDefinition(
		models.ProcessDefinitionCreateModel{
			DeploymentID: deploy.ID,
			ProcessKey:   extractedKey,
			ProcessName:  &deploymentName,
			IsActive:     true,
			BpmnXML:      string(bpmnBytes),
			ParsedGraph:  graphJSON,
		},
	)

	return c.JSON(fiber.Map{
		"definitionId": definitionID,
		"processKey":   extractedKey,
		"version":      version,
	})
}
