package api

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"time"

	"github.com/gofiber/fiber/v2"
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
