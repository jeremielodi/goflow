package api

import (
	"database/sql"
	"errors"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

type ExternalTaskController struct {
	db                  *sqlx.DB
	externalTaskRepo    *repository.ExternalTaskRepository
	processInstanceRepo *repository.ProcessInstanceRepository
	incidentRepo        *repository.IncidentRepository
	dispatcher          *events.TaskEventDispatcher
	auditService        *service.AuditService
}

func NewExternalTaskController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *ExternalTaskController {
	return &ExternalTaskController{
		db:                  db,
		externalTaskRepo:    repository.NewExternalTaskRepository(db),
		processInstanceRepo: repository.NewProcessInstanceRepository(db),
		incidentRepo:        repository.NewIncidentRepository(db),
		dispatcher:          dispatcher,
		auditService:        service.NewAuditService(db),
	}
}

// FetchAndLockRequest matches Camunda 7 fetchAndLock request body
type FetchAndLockRequest struct {
	WorkerId string `json:"workerId"`
	MaxTasks int    `json:"maxTasks"`
	// AsyncResponseTimeout (ms): wait that long for a task when none is
	// available (Camunda 7 long polling).
	AsyncResponseTimeout int64 `json:"asyncResponseTimeout"`
	Topics               []struct {
		TopicName    string `json:"topicName"`
		LockDuration int    `json:"lockDuration"` // milliseconds
		// Variables, when given, are the only variables returned.
		Variables []string `json:"variables"`
	} `json:"topics"`
}

type CamundaVariable struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

type FetchAndLockResponse struct {
	ID                string                     `json:"id"`
	TopicName         string                     `json:"topicName"`
	ProcessInstanceID string                     `json:"processInstanceId"`
	Variables         map[string]CamundaVariable `json:"variables"`
	Retries           int                        `json:"retries"`
	WorkerId          string                     `json:"workerId"`
	Headers           map[string]string          `json:"headers,omitempty"` // zeebe:taskHeaders
	// Camunda 7 LockedExternalTaskDto fields.
	ActivityID           string  `json:"activityId"`
	ActivityInstanceID   string  `json:"activityInstanceId"`
	ExecutionID          string  `json:"executionId"`
	ProcessDefinitionID  string  `json:"processDefinitionId"`
	ProcessDefinitionKey string  `json:"processDefinitionKey"`
	BusinessKey          *string `json:"businessKey"`
	TenantID             *string `json:"tenantId"`
	LockExpirationTime   *string `json:"lockExpirationTime"`
	Priority             int     `json:"priority"`
	ErrorMessage         *string `json:"errorMessage"`
	ErrorDetails         *string `json:"errorDetails"`
}

type CompleteRequest struct {
	Variables map[string]interface{} `json:"variables"`
}

type FailureRequest struct {
	ErrorMessage string `json:"errorMessage"`
	ErrorCode    string `json:"errorCode"`
	Retries      int    `json:"retries"`
	RetryTimeout int    `json:"retryTimeout"`
}

// normalizeVariables converts both simple and typed formats to simple map[string]interface{}
func normalizeVariablesForExternal(variables map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range variables {
		// Check if it's the typed format: { value: xxx, type: "string" }
		if varMap, ok := value.(map[string]interface{}); ok {
			if val, hasValue := varMap["value"]; hasValue {
				// Typed format
				result[key] = typedValue(val, varMap["type"])
				continue
			}
		}
		// Simple format
		result[key] = value
	}

	return result
}

// formatVariablesForResponse converts internal variables to Camunda typed format
func formatVariablesForResponse(variables map[string]interface{}) map[string]CamundaVariable {
	result := make(map[string]CamundaVariable)
	for k, v := range variables {
		result[k] = CamundaVariable{
			Value: v,
			Type:  getCamundaVariableType(v),
		}
	}
	return result
}


func (ctrl *ExternalTaskController) CompleteTask(c *fiber.Ctx) error {
	jobID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid task id format",
			"error":   err.Error(),
		})
	}

	var req CompleteRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid request body",
			"error":   err.Error(),
		})
	}

	normalizedVars := normalizeVariablesForExternal(req.Variables)

	result, err := runtime.CompleteJob(c.Context(), ctrl.db, jobID, normalizedVars)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrJobNotFound):
			return c.Status(404).JSON(fiber.Map{"title": "Not Found", "message": "job not found"})
		case errors.Is(err, service.ErrJobInvalidState):
			return c.Status(400).JSON(fiber.Map{"title": "Invalid State", "message": err.Error()})
		case errors.Is(err, runtime.ErrExecutionFailed):
			log.Printf("Execution resume error: %v", err)
			return c.Status(500).JSON(fiber.Map{
				"title":   "Execution Error",
				"message": "failed to resume process execution",
				"error":   err.Error(),
			})
		default:
			return c.Status(500).JSON(fiber.Map{"title": "Error", "message": err.Error()})
		}
	}

	if result.ProcessCompleted {
		return c.JSON(fiber.Map{
			"title":   "Success",
			"message": "process completed",
			"jobId":   jobID,
		})
	}

	return c.JSON(fiber.Map{
		"title":   "Success",
		"message": "job completed successfully",
	})
}

func (ctrl *ExternalTaskController) HandleFailure(c *fiber.Ctx) error {
	jobID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid task id format",
			"error":   err.Error(),
		})
	}

	var req FailureRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid request body",
			"error":   err.Error(),
		})
	}

	// retries is what remains after this failure, in Camunda 7 (0: an
	// incident) as in Camunda 8 — service.FailJob's own convention.
	newRetries := req.Retries

	result, err := runtime.FailJob(c.Context(), ctrl.db, runtime.FailJobArgs{
		JobID:            jobID,
		RetriesRemaining: newRetries,
		ErrorMessage:     req.ErrorMessage,
		ErrorCode:        req.ErrorCode,
	})
	if errors.Is(err, service.ErrJobNotFound) {
		return c.Status(404).JSON(fiber.Map{"title": "Not Found", "message": "job not found"})
	}
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"title": "Error", "message": err.Error()})
	}

	if result.BoundaryTriggered {
		if result.ContinueError != "" {
			log.Printf("Error boundary execution failed: %s", result.ContinueError)
		}
		return c.JSON(fiber.Map{
			"title":   "Success",
			"message": "error boundary event triggered",
		})
	}

	if result.PropagatedToCaller {
		if result.ContinueError != "" {
			log.Printf("Propagating BPMN error %s to the calling process failed: %s", req.ErrorCode, result.ContinueError)
		}
		return c.JSON(fiber.Map{
			"title":   "Success",
			"message": "error caught by the calling process",
		})
	}

	message := "failure recorded, job will be retried"
	if result.PermanentlyFailed {
		message = "job marked as failed permanently"
	}

	return c.JSON(fiber.Map{
		"title":   "Success",
		"message": message,
	})
}

// Helper to map Go types to Camunda variable types
func getCamundaVariableType(v interface{}) string {
	switch v.(type) {
	case bool:
		return "Boolean"
	case float64, int, int64, float32:
		return "Double"
	case string:
		return "String"
	default:
		return "Object"
	}
}

// GetJobs handles GET /jobs - returns jobs for a process instance
func (ctrl *ExternalTaskController) GetJobs(c *fiber.Ctx) error {
	processInstanceID := c.Query("processInstanceId")
	topic := c.Query("topic")

	var jobs []models.Job
	query := `
		SELECT 
			id, process_instance_id, execution_id, job_type, status, 
			payload, retries, created_at, updated_at, completed_at 
		FROM public.jobs WHERE 1=1`

	if processInstanceID != "" {
		pid, err := uuid.Parse(processInstanceID)
		if err == nil {
			query += " AND process_instance_id = $1"
			if topic != "" {
				query += " AND job_type = $2 ORDER BY created_at DESC"
				ctrl.db.Select(&jobs, query, pid, topic)
			} else {
				query += " ORDER BY created_at DESC"
				ctrl.db.Select(&jobs, query, pid)
			}
		}
	} else if topic != "" {
		query += " AND job_type = $1 ORDER BY created_at DESC"
		ctrl.db.Select(&jobs, query, topic)
	} else {
		query += " ORDER BY created_at DESC"
		ctrl.db.Select(&jobs, query)
	}

	return c.JSON(jobs)
}

// GetJobs handles GET /jobs - returns jobs for a process instance
func (ctrl *ExternalTaskController) GetJob(c *fiber.Ctx) error {
	id := c.Params("id")
	// If ID is provided, return single job
	if id == "" {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   true,
			"message": "Failed to fetch job",
		})
	}
	var job models.Job
	query := `
			SELECT 
				id, process_instance_id, execution_id, job_type, status, 
				payload, retries, created_at, updated_at, completed_at 
			FROM public.jobs WHERE id = $1`

	err := ctrl.db.Get(&job, query, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   true,
				"message": "Job not found",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   true,
			"message": "Failed to fetch job",
			"details": err.Error(),
		})
	}

	return c.JSON(job)

}
