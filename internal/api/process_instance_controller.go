package api

import (
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/internal/worker"
	"github.com/jmoiron/sqlx"
)

type ProcessInstanceController struct {
	db                  *sqlx.DB
	taskService         *service.TaskService
	processRepo         *repository.ProcessRepository
	processInstanceRepo *repository.ProcessInstanceRepository
	workerPool          *worker.WorkerPool // Add this field
}

func NewProcessInstanceController(db *sqlx.DB, taskService *service.TaskService, workerPool *worker.WorkerPool) *ProcessInstanceController {
	return &ProcessInstanceController{
		db:                  db,
		taskService:         taskService,
		processRepo:         repository.NewProcessRepository(db),
		processInstanceRepo: repository.NewProcessInstanceRepository(db),
		workerPool:          workerPool,
	}
}

// Variable represents a typed variable from Camunda
type Variable struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

// normalizeVariables converts both simple and typed formats to simple map[string]interface{}
func normalizeVariables(variables map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range variables {
		// Check if it's the typed format: { value: xxx, type: "string" }
		if varMap, ok := value.(map[string]interface{}); ok {
			// Check if it has a "value" field (Camunda typed format)
			if val, hasValue := varMap["value"]; hasValue {
				result[key] = val
				continue
			}
		}
		// Check if it's a Variable struct (if passed as struct)
		if varStruct, ok := value.(Variable); ok {
			result[key] = varStruct.Value
			continue
		}
		// Simple format - use as is
		result[key] = value
	}

	return result
}

// ============================================================
// START PROCESS
// POST /process-definitions/:key/start
// ============================================================
type StartProcessRequest struct {
	Variables map[string]interface{} `json:"variables"`
}

func (pc *ProcessInstanceController) StartProcess(c *fiber.Ctx) error {
	processKey := c.Params("key")
	if processKey == "" {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "process key required",
		})
	}

	var body StartProcessRequest
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid request body",
			"error":   err.Error(),
		})
	}

	// Normalize variables (supports both simple and typed formats)
	normalizedVars := normalizeVariables(body.Variables)

	// Load definition and graph using repository
	def, err := pc.processRepo.FindLatestProcessDefinitionByKey(processKey)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{
			"title":   "Not Found",
			"message": "process definition not found",
			"error":   err.Error(),
		})
	}

	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Internal Server Error",
			"message": "failed to parse process graph",
			"error":   err.Error(),
		})
	}

	// Find start node
	var startNodeID string
	for _, node := range graph.Nodes {
		if node.Type == engine.StartEventType {
			startNodeID = node.ID
			break
		}
	}
	if startNodeID == "" {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Configuration Error",
			"message": "no start event found in process definition",
		})
	}

	// Begin transaction
	tx, err := pc.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to begin database transaction",
			"error":   err.Error(),
		})
	}
	defer tx.Rollback()

	instanceID := uuid.New()
	now := time.Now()

	// Create process instance using repository
	if err := pc.processInstanceRepo.CreateProcessInstanceTx(tx, instanceID, def.ID, now); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to create process instance",
			"error":   err.Error(),
		})
	}

	// Save normalized variables using repository
	if err := pc.processInstanceRepo.CreateVariablesTx(tx, instanceID, normalizedVars, now); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to save process variables",
			"error":   err.Error(),
		})
	}

	// Create initial execution using repository
	execID := uuid.New()
	if err := pc.processInstanceRepo.CreateExecutionTx(tx, execID, instanceID, startNodeID, now); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to create execution record",
			"error":   err.Error(),
		})
	}

	if err := tx.Commit(); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to commit transaction",
			"error":   err.Error(),
		})
	}

	// Run the runtime
	rt := runtime.NewRuntime(&graph, pc.db)
	ctx := c.Context()
	err = rt.ExecuteExecution(ctx, execID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Execution Error",
			"message": "failed to execute process",
			"error":   err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"processInstanceId": instanceID,
		"executionId":       execID,
		"processKey":        def.ProcessKey,
		"version":           def.Version,
		"message":           "process started successfully",
	})
}

// ============================================================
// COMPLETE USER TASK
// POST /tasks/:id/complete
// ============================================================

func (pc *ProcessInstanceController) CompleteTask(c *fiber.Ctx) error {
	taskID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid task ID",
			"error":   err.Error(),
		})
	}

	var req struct {
		Variables map[string]interface{} `json:"variables"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid request body",
			"error":   err.Error(),
		})
	}

	// Normalize variables (supports both simple and typed formats)
	normalizedVars := normalizeVariables(req.Variables)

	err = pc.taskService.CompleteTask(c.Context(), taskID, normalizedVars)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Error",
			"message": "Task not completed",
			"error":   err.Error(),
		})
	}
	return c.JSON(fiber.Map{
		"message": "task completed successfully",
	})
}

// Add this to process_instance_controller.go

// StartProcessAsync - Non-blocking version using worker pool
func (pc *ProcessInstanceController) StartProcessAsync(c *fiber.Ctx) error {
	processKey := c.Params("key")

	var body StartProcessRequest
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid request body",
			"error":   err.Error(),
		})
	}

	normalizedVars := normalizeVariables(body.Variables)

	job := worker.Job{
		Type:       "start_process",
		ProcessKey: processKey,
		Variables:  normalizedVars,
	}

	if err := pc.workerPool.Submit(job); err != nil {
		return c.Status(503).JSON(fiber.Map{
			"title":   "Server Busy",
			"message": "Server is under heavy load",
			"error":   err.Error(),
		})
	}

	return c.Status(202).JSON(fiber.Map{
		"title":      "Accepted",
		"message":    "Process start request accepted",
		"requestId":  job.ID,
		"status":     "queued",
		"queue_size": pc.workerPool.GetQueueLength(),
	})
}
