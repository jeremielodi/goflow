// Package v2 exposes GoFlow's engine through the Camunda 8 "Orchestration
// Cluster" REST API shapes (POST /v2/process-instances, /v2/jobs/activation,
// /v2/user-tasks/..., etc.), in parallel with the existing Camunda 7 style
// /engine-rest/... surface. It reuses the same repositories/services as the
// v1 controllers in internal/api — this package only translates request and
// response shapes, it does not duplicate engine logic.
package v2

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jmoiron/sqlx"
)

type ProcessInstanceController struct {
	db                  *sqlx.DB
	dispatcher          *events.TaskEventDispatcher
	processRepo         *repository.ProcessRepository
	processInstanceRepo *repository.ProcessInstanceRepository
}

func NewProcessInstanceController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *ProcessInstanceController {
	return &ProcessInstanceController{
		db:                  db,
		dispatcher:          dispatcher,
		processRepo:         repository.NewProcessRepository(db),
		processInstanceRepo: repository.NewProcessInstanceRepository(db),
	}
}

// processInstanceState maps GoFlow's internal status to the uppercase state
// names used by the Camunda 8 REST API.
func processInstanceState(status string) string {
	switch status {
	case "active", "running":
		return "ACTIVE"
	case "completed":
		return "COMPLETED"
	case "terminated":
		return "TERMINATED"
	case "suspended":
		return "SUSPENDED"
	default:
		return strings.ToUpper(status)
	}
}

func toV2ProcessInstance(inst *models.ProcessInstance) fiber.Map {
	return fiber.Map{
		"processInstanceKey":  inst.ID,
		"processDefinitionId": inst.ProcessKey,
		"processDefinitionKey": inst.ProcessDefinitionID,
		"processDefinitionVersion": inst.Version,
		"state":                    processInstanceState(inst.Status),
		"startDate":                inst.StartedAt,
		"endDate":                  inst.EndedAt,
	}
}

// CreateProcessInstanceRequest matches Camunda 8's CreateProcessInstance body.
type CreateProcessInstanceRequest struct {
	ProcessDefinitionId string                 `json:"processDefinitionId"`
	Variables            map[string]interface{} `json:"variables"`
}

// CreateProcessInstance handles POST /v2/process-instances.
func (pc *ProcessInstanceController) CreateProcessInstance(c *fiber.Ctx) error {
	var req CreateProcessInstanceRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title":  "INVALID_ARGUMENT",
			"detail": err.Error(),
		})
	}
	if req.ProcessDefinitionId == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title":  "INVALID_ARGUMENT",
			"detail": "processDefinitionId is required",
		})
	}

	def, err := pc.processRepo.FindLatestProcessDefinitionByKey(req.ProcessDefinitionId)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"title":  "NOT_FOUND",
			"detail": "no process definition found for id " + req.ProcessDefinitionId,
		})
	}

	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"title":  "INTERNAL_ERROR",
			"detail": "failed to parse process graph",
		})
	}

	var startNodeID string
	for _, node := range graph.Nodes {
		if node.Type == engine.StartEventType {
			startNodeID = node.ID
			break
		}
	}
	if startNodeID == "" {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"title":  "INTERNAL_ERROR",
			"detail": "no start event found in process definition",
		})
	}

	tx, err := pc.db.Beginx()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}
	defer tx.Rollback()

	instanceID := uuid.New()
	now := time.Now()

	if err := pc.processInstanceRepo.CreateProcessInstanceTx(tx, instanceID, def.ID, now); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}
	if err := pc.processInstanceRepo.CreateVariablesTx(tx, instanceID, req.Variables, now); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	execID := uuid.New()
	if err := pc.processInstanceRepo.CreateExecutionTx(tx, execID, instanceID, startNodeID, now); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}
	if err := tx.Commit(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	rt := runtime.NewRuntime(&graph, pc.db, pc.dispatcher)
	if err := rt.ExecuteExecution(c.Context(), execID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"title":  "INTERNAL_ERROR",
			"detail": "failed to execute process: " + err.Error(),
		})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"processInstanceKey":      instanceID,
		"processDefinitionId":     def.ProcessKey,
		"processDefinitionKey":    def.ID,
		"processDefinitionVersion": def.Version,
	})
}

// GetProcessInstance handles GET /v2/process-instances/:id.
func (pc *ProcessInstanceController) GetProcessInstance(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"title":  "INVALID_ARGUMENT",
			"detail": "invalid process instance key",
		})
	}

	inst, err := pc.processInstanceRepo.FindByID(instanceID)
	if err != nil {
		// Fall back to history for completed/terminated instances.
		hist, histErr := pc.processInstanceRepo.FindHistoricByID(instanceID)
		if histErr != nil || hist == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"title":  "NOT_FOUND",
				"detail": "process instance not found",
			})
		}
		return c.JSON(toV2ProcessInstance(hist))
	}

	return c.JSON(toV2ProcessInstance(inst))
}

// SearchProcessInstancesRequest matches the (simplified) Camunda 8 search body.
type SearchProcessInstancesRequest struct {
	Filter struct {
		ProcessDefinitionId string `json:"processDefinitionId"`
		State                string `json:"state"`
	} `json:"filter"`
}

// SearchProcessInstances handles POST /v2/process-instances/search.
func (pc *ProcessInstanceController) SearchProcessInstances(c *fiber.Ctx) error {
	var req SearchProcessInstancesRequest
	// A missing/empty body means "no filter" — ignore parse errors on empty body.
	_ = c.BodyParser(&req)

	status := ""
	switch strings.ToUpper(req.Filter.State) {
	case "ACTIVE":
		status = "active"
	case "COMPLETED":
		status = "completed"
	case "TERMINATED":
		status = "terminated"
	case "SUSPENDED":
		status = "suspended"
	}

	instances, err := pc.processInstanceRepo.FindAll(status, req.Filter.ProcessDefinitionId)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	items := make([]fiber.Map, 0, len(instances))
	for i := range instances {
		items = append(items, toV2ProcessInstance(&instances[i]))
	}

	return c.JSON(fiber.Map{"items": items})
}
