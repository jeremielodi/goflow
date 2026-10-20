package api

import (
	"errors"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/cmdbus"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/internal/worker"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type ProcessInstanceController struct {
	db                  *sqlx.DB
	taskService         *service.TaskService
	processRepo         *repository.ProcessRepository
	processInstanceRepo *repository.ProcessInstanceRepository
	workerPool          *worker.WorkerPool
	dispatcher          *events.TaskEventDispatcher
	auditService        *service.AuditService
}

func NewProcessInstanceController(db *sqlx.DB, taskService *service.TaskService, workerPool *worker.WorkerPool, dispatcher *events.TaskEventDispatcher) *ProcessInstanceController {
	registerInstanceCommands(db, dispatcher)
	return &ProcessInstanceController{
		db:                  db,
		taskService:         taskService,
		processRepo:         repository.NewProcessRepository(db),
		processInstanceRepo: repository.NewProcessInstanceRepository(db),
		workerPool:          workerPool,
		dispatcher:          dispatcher,
		auditService:        service.NewAuditService(db),
	}
}

// checkInstanceAccess verifies the caller has at least `level` access to
// instanceID via service.CanAccessProcessInstance. Returns true (fail
// open) if there's no authenticated user in context — the handler's own
// not-found/error logic already covers an unresolvable instance.
func (pc *ProcessInstanceController) checkInstanceAccess(c *fiber.Ctx, instanceID uuid.UUID, level string) bool {
	userID, err := uuid.Parse(common.GetUserUuid(c))
	if err != nil {
		return true
	}
	allowed, _ := service.CanAccessProcessInstance(pc.db, userID, instanceID, level)
	return allowed
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

	if !pc.checkInstanceAccess(c, instanceID, "VIEW") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	variables, err := pc.getVariablesLiveOrHistoric(instanceID)
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

// getVariablesLiveOrHistoric reads a process instance's variables from the
// live table, falling back to the historic archive (historic_variables) if
// the instance no longer exists live — see archive_service.go. Distinguishes
// "no variables were ever set" from "this instance is archived" by checking
// whether the instance is still live at all, rather than guessing from an
// empty variables map (which is ambiguous either way).
func (pc *ProcessInstanceController) getVariablesLiveOrHistoric(instanceID uuid.UUID) (map[string]interface{}, error) {
	instance, err := pc.processInstanceRepo.FindByID(instanceID)
	if err != nil {
		return nil, err
	}
	if instance != nil {
		return pc.processInstanceRepo.GetVariables(instanceID)
	}
	return repository.NewHistoricArchiveRepository(pc.db).FindVariablesByProcessInstance(instanceID)
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

	if !pc.checkInstanceAccess(c, instanceID, "VIEW") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	variables, err := pc.getVariablesLiveOrHistoric(instanceID)
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

	if !pc.checkInstanceAccess(c, instanceID, "MANAGE") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	var variables map[string]interface{}
	if err := c.BodyParser(&variables); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	err = cmdbus.Do(c.Context(), cmdbus.Key(instanceID), cmdEditVariables,
		editVariablesArgs{InstanceID: instanceID, Set: normalizeVariables(variables), AuditAll: true}, nil)
	if err != nil {
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

	if !pc.checkInstanceAccess(c, instanceID, "MANAGE") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	var req Variable
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	err = cmdbus.Do(c.Context(), cmdbus.Key(instanceID), cmdEditVariables,
		editVariablesArgs{InstanceID: instanceID, Set: map[string]interface{}{varName: req.Value}}, nil)
	if err != nil {
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

	if !pc.checkInstanceAccess(c, instanceID, "MANAGE") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	err = cmdbus.Do(c.Context(), cmdbus.Key(instanceID), cmdEditVariables,
		editVariablesArgs{InstanceID: instanceID, Unset: []string{varName}}, nil)
	if err != nil {
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

	if !pc.checkInstanceAccess(c, instanceID, "MANAGE") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	err = cmdbus.Do(c.Context(), cmdbus.Key(instanceID), cmdEditVariables,
		editVariablesArgs{InstanceID: instanceID, Clear: true}, nil)
	if err != nil {
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

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(pc.db, userID, processKey, "START"); permErr == nil && !allowed {
			return c.Status(403).JSON(fiber.Map{
				"title":   "Forbidden",
				"message": "you do not have START access to this process",
			})
		}
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

	tenantID := common.InstanceTenant(def.TenantID, common.GetTenantId(c))

	instanceID, execID, err := runtime.StartInstance(c.Context(), def.ID, tenantID, normalizedVars)
	if errors.Is(err, runtime.ErrExecutionFailed) {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Execution Error",
			"message": "failed to execute process",
			"error":   err.Error(),
		})
	}
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": err.Error(),
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

	err = runtime.CompleteUserTask(c.Context(), pc.db, taskID, normalizedVars)
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

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		if allowed, permErr := service.CanAccessProcess(pc.db, userID, processKey, "START"); permErr == nil && !allowed {
			return c.Status(403).JSON(fiber.Map{
				"title":   "Forbidden",
				"message": "you do not have START access to this process",
			})
		}
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

	if !pc.checkInstanceAccess(c, instanceID, "VIEW") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	instance, err := pc.processInstanceRepo.FindByID(instanceID)
	if err != nil || instance == nil {
		// Not live — fall back to the historic archive for a
		// completed/terminated instance (see archive_service.go).
		hist, histErr := pc.processInstanceRepo.FindHistoricByID(instanceID)
		if histErr != nil || hist == nil || !common.TenantMatches(hist.TenantID, common.GetTenantId(c)) {
			return c.Status(404).JSON(fiber.Map{
				"type":    "NotFound",
				"message": "Process instance not found",
			})
		}
		return c.JSON(hist)
	}
	if !common.TenantMatches(instance.TenantID, common.GetTenantId(c)) {
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

	instances, err := pc.processInstanceRepo.FindAll(status, processKey, common.GetTenantId(c))
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	if userID, parseErr := uuid.Parse(common.GetUserUuid(c)); parseErr == nil {
		visible := make([]models.ProcessInstance, 0, len(instances))
		for _, inst := range instances {
			if allowed, _ := service.CanAccessProcess(pc.db, userID, inst.ProcessKey, "VIEW"); allowed {
				visible = append(visible, inst)
			}
		}
		instances = visible
	}

	// A nil slice serializes as JSON null, which crashes the frontend list pages.
	if instances == nil {
		instances = []models.ProcessInstance{}
	}
	return c.JSON(instances)
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

	if !pc.checkInstanceAccess(c, instanceID, "MANAGE") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
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

	err = cmdbus.Do(c.Context(), cmdbus.Key(instanceID), cmdSuspendInstance,
		suspendArgs{InstanceID: instanceID, Suspended: req.Suspended}, nil)
	if err != nil {
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

	if !pc.checkInstanceAccess(c, instanceID, "MANAGE") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	reason := c.Query("deleteReason")
	if reason == "" {
		reason = "Process terminated by user"
	}

	if err := runtime.Terminate(c.Context(), instanceID, reason); err != nil {
		if errors.Is(err, service.ErrProcessInstanceNotFound) {
			return c.Status(404).JSON(fiber.Map{
				"type":    "NotFound",
				"message": "Process instance not found",
			})
		}
		return c.Status(500).JSON(fiber.Map{"type": "InternalError", "message": err.Error()})
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

	if !pc.checkInstanceAccess(c, instanceID, "MANAGE") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
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

	err = cmdbus.Do(c.Context(), cmdbus.Key(instanceID), cmdEndInstance,
		endArgs{InstanceID: instanceID, Variables: normalizedVars}, nil)
	if err != nil {
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

	if !pc.checkInstanceAccess(c, instanceID, "VIEW") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
		})
	}

	// Try to get active process instance
	instance, err := pc.processInstanceRepo.FindByID(instanceID)
	if err != nil || instance == nil {
		// Not live (or already archived) — check history.
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

	if !pc.checkInstanceAccess(c, instanceID, "MANAGE") {
		return c.Status(404).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "Process instance not found",
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

	err = cmdbus.Do(c.Context(), cmdbus.Key(instanceID), cmdStopInstance,
		stopArgs{InstanceID: instanceID, Variables: normalizeVariables(stopVariables)}, nil)
	if err != nil {
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
