package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
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
	workerPool          *worker.WorkerPool
	dispatcher          *events.TaskEventDispatcher
}

func NewProcessInstanceController(db *sqlx.DB, taskService *service.TaskService, workerPool *worker.WorkerPool, dispatcher *events.TaskEventDispatcher) *ProcessInstanceController {
	return &ProcessInstanceController{
		db:                  db,
		taskService:         taskService,
		processRepo:         repository.NewProcessRepository(db),
		processInstanceRepo: repository.NewProcessInstanceRepository(db),
		workerPool:          workerPool,
		dispatcher:          dispatcher,
	}
}

// Variable represents a typed variable from Camunda
type Variable struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

// VariableResponse represents a single variable in Camunda format
type VariableResponse struct {
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

// formatVariablesToCamunda converts internal variables to Camunda typed format
func formatVariablesToCamunda(variables map[string]interface{}) map[string]VariableResponse {
	result := make(map[string]VariableResponse)
	for k, v := range variables {
		result[k] = VariableResponse{
			Value: v,
			Type:  getVariableType(v),
		}
	}
	return result
}

// getVariableType determines the Camunda variable type
func getVariableType(v interface{}) string {
	switch v.(type) {
	case string:
		return "String"
	case int, int32, int64, float32, float64:
		return "Double"
	case bool:
		return "Boolean"
	case time.Time:
		return "Date"
	case nil:
		return "Null"
	default:
		return "Object"
	}
}

// ============================================================
// PROCESS VARIABLES
// ============================================================

// GetProcessVariables handles GET /process-instance/:id/variables
// Returns all variables of a process instance in Camunda format
func (pc *ProcessInstanceController) GetProcessVariables(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	variables, err := pc.processInstanceRepo.GetVariables(instanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	// Format to Camunda typed format
	response := formatVariablesToCamunda(variables)

	return c.JSON(response)
}

// GetProcessVariable handles GET /process-instance/:id/variables/:varName
// Returns a single variable from a process instance in Camunda format
func (pc *ProcessInstanceController) GetProcessVariable(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	varName := c.Params("varName")
	if varName == "" {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Variable name is required",
		})
	}

	variables, err := pc.processInstanceRepo.GetVariables(instanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	value, exists := variables[varName]
	if !exists {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Variable not found",
		})
	}

	return c.JSON(VariableResponse{
		Value: value,
		Type:  getVariableType(value),
	})
}

// SetProcessVariables handles POST /process-instance/:id/variables
// Sets multiple variables for a process instance
func (pc *ProcessInstanceController) SetProcessVariables(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	var variables map[string]interface{}
	if err := c.BodyParser(&variables); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	// Normalize variables
	normalizedVars := normalizeVariables(variables)

	// Get existing variables
	existingVars, err := pc.processInstanceRepo.GetVariables(instanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	// Merge variables
	for k, v := range normalizedVars {
		existingVars[k] = v
	}

	// Save variables
	if err := pc.processInstanceRepo.UpsertVariables(instanceID, existingVars); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.Status(204).Send(nil)
}

// SetProcessVariable handles PUT /process-instance/:id/variables/:varName
// Sets a single variable for a process instance
func (pc *ProcessInstanceController) SetProcessVariable(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	varName := c.Params("varName")
	if varName == "" {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Variable name is required",
		})
	}

	var req Variable
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	// Get existing variables
	existingVars, err := pc.processInstanceRepo.GetVariables(instanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	// Set variable
	existingVars[varName] = req.Value

	// Save variables
	if err := pc.processInstanceRepo.UpsertVariables(instanceID, existingVars); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.Status(204).Send(nil)
}

// DeleteProcessVariable handles DELETE /process-instance/:id/variables/:varName
// Deletes a single variable from a process instance
func (pc *ProcessInstanceController) DeleteProcessVariable(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	varName := c.Params("varName")
	if varName == "" {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Variable name is required",
		})
	}

	// Get existing variables
	existingVars, err := pc.processInstanceRepo.GetVariables(instanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	// Delete variable
	delete(existingVars, varName)

	// Save variables
	if err := pc.processInstanceRepo.UpsertVariables(instanceID, existingVars); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.Status(204).Send(nil)
}

// DeleteProcessVariables handles DELETE /process-instance/:id/variables
// Deletes all variables from a process instance
func (pc *ProcessInstanceController) DeleteProcessVariables(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	// Save empty variables
	if err := pc.processInstanceRepo.UpsertVariables(instanceID, make(map[string]interface{})); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.Status(204).Send(nil)
}

// ============================================================
// START PROCESS
// POST /process-definitions/:key/start
// ============================================================
type StartProcessRequest struct {
	Variables   map[string]interface{} `json:"variables"`
	BusinessKey string                 `json:"businessKey,omitempty"`
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
	rt := runtime.NewRuntime(&graph, pc.db, pc.dispatcher)
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

// ============================================================
// ASYNC PROCESS START
// ============================================================

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

// ============================================================
// PROCESS INSTANCE QUERIES
// ============================================================

// GetProcessInstance handles GET /process-instance/:id
func (pc *ProcessInstanceController) GetProcessInstance(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	instance, err := pc.processInstanceRepo.FindByID(instanceID)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	return c.JSON(instance)
}

// GetProcessInstanceList handles GET /process-instance
func (pc *ProcessInstanceController) GetProcessInstanceList(c *fiber.Ctx) error {
	status := c.Query("status")
	processKey := c.Query("processKey")

	instances, err := pc.processInstanceRepo.FindAll(status, processKey)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.JSON(instances)
}

// DeleteProcessInstance handles DELETE /process-instance/:id
func (pc *ProcessInstanceController) DeleteProcessInstance(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	if err := pc.processInstanceRepo.Delete(instanceID); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.Status(204).Send(nil)
}

// internal/api/process_instance_controller.go
// Add these methods to the ProcessInstanceController

// ============================================================
// PROCESS INSTANCE STATE MANAGEMENT
// ============================================================

// SuspendProcess handles PUT /process-instance/:id/suspended
// Suspends or resumes a process instance
func (pc *ProcessInstanceController) SuspendProcess(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	var req struct {
		Suspended bool `json:"suspended"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	status := "active"
	if req.Suspended {
		status = "suspended"
	}

	// Update process instance status
	if err := pc.processInstanceRepo.UpdateStatus(instanceID, status); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	message := "resumed"
	if req.Suspended {
		message = "suspended"
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Process instance " + message + " successfully",
	})
}

// TerminateProcess handles DELETE /process-instance/:id
// Terminates a process instance with an optional reason
func (pc *ProcessInstanceController) TerminateProcess(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	reason := c.Query("deleteReason")
	if reason == "" {
		reason = "Process terminated by user"
	}

	// Get process instance to check if it exists
	_, err = pc.processInstanceRepo.FindByID(instanceID)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	// Add termination reason as a variable
	variables := map[string]interface{}{
		"terminated":        true,
		"terminatedAt":      time.Now(),
		"terminationReason": reason,
	}

	if err := pc.processInstanceRepo.UpsertVariables(instanceID, variables); err != nil {
		// Log error but continue with deletion
		fmt.Printf("Warning: Failed to add termination variables: %v\n", err)
	}

	// Delete the process instance
	if err := pc.processInstanceRepo.Delete(instanceID); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.Status(204).Send(nil)
}

// EndProcess handles POST /process-instance/:id/end
// Ends a process instance with optional variables
func (pc *ProcessInstanceController) EndProcess(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	var req struct {
		Variables map[string]interface{} `json:"variables"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	// Normalize variables
	normalizedVars := normalizeVariables(req.Variables)

	// Add end process variables
	normalizedVars["ended"] = true
	normalizedVars["endedAt"] = time.Now()

	// Update process instance status
	if err := pc.processInstanceRepo.UpdateStatus(instanceID, "completed"); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	// Update variables
	existingVars, err := pc.processInstanceRepo.GetVariables(instanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	for k, v := range normalizedVars {
		existingVars[k] = v
	}

	if err := pc.processInstanceRepo.UpsertVariables(instanceID, existingVars); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Process instance ended successfully",
	})
}

// GetProcessState handles GET /process-instance/:id/state
// Returns the current state of a process instance
func (pc *ProcessInstanceController) GetProcessState(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	// Try to get active process instance
	instance, err := pc.processInstanceRepo.FindByID(instanceID)
	if err != nil {
		// Check if it's in history (completed/terminated)
		history, err := pc.processInstanceRepo.FindHistoricByID(instanceID)
		if err != nil || history == nil {
			return c.Status(404).JSON(fiber.Map{
				"type":    "NotFound",
				"message": "Process instance not found",
			})
		}
		return c.JSON(fiber.Map{
			"id":        history.ID,
			"status":    history.Status,
			"startedAt": history.StartedAt,
			"endedAt":   history.EndedAt,
			"ended":     true,
		})
	}

	// Get active executions count
	executions, err := pc.processInstanceRepo.GetActiveExecutions(instanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"id":               instance.ID,
		"status":           instance.Status,
		"startedAt":        instance.StartedAt,
		"activeExecutions": len(executions),
		"ended":            false,
	})
}

// StopProcessWithReason handles POST /process-instance/:id/stop
// Stops a process with a specific reason
func (pc *ProcessInstanceController) StopProcessWithReason(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	var req struct {
		Reason    string                 `json:"reason"`
		Variables map[string]interface{} `json:"variables"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	if req.Reason == "" {
		req.Reason = "Process stopped by user"
	}

	// Add stop variables
	stopVariables := map[string]interface{}{
		"processStopped": true,
		"stopReason":     req.Reason,
		"stoppedAt":      time.Now(),
	}

	for k, v := range req.Variables {
		stopVariables[k] = v
	}

	normalizedVars := normalizeVariables(stopVariables)

	// Update variables
	existingVars, err := pc.processInstanceRepo.GetVariables(instanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	for k, v := range normalizedVars {
		existingVars[k] = v
	}

	if err := pc.processInstanceRepo.UpsertVariables(instanceID, existingVars); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	// Terminate the process
	if err := pc.processInstanceRepo.UpdateStatus(instanceID, "terminated"); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Process stopped successfully",
		"reason":  req.Reason,
	})
}

// GetHistoricProcessInstance handles GET /history/process-instance/:id
// Retrieves a historic process instance
func (pc *ProcessInstanceController) GetHistoricProcessInstance(c *fiber.Ctx) error {
	instanceID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": "Invalid process instance id",
		})
	}

	instance, err := pc.processInstanceRepo.FindHistoricByID(instanceID)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(404).JSON(fiber.Map{
				"type":    "NotFound",
				"message": "Process instance not found",
			})
		}
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.JSON(instance)
}
