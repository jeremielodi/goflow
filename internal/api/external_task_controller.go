package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jmoiron/sqlx"
)

type ExternalTaskController struct {
	db                  *sqlx.DB
	externalTaskRepo    *repository.ExternalTaskRepository
	processInstanceRepo *repository.ProcessInstanceRepository
}

func NewExternalTaskController(db *sqlx.DB) *ExternalTaskController {
	return &ExternalTaskController{
		db:                  db,
		externalTaskRepo:    repository.NewExternalTaskRepository(db),
		processInstanceRepo: repository.NewProcessInstanceRepository(db),
	}
}

// FetchAndLockRequest matches Camunda 7 fetchAndLock request body
type FetchAndLockRequest struct {
	WorkerId string `json:"workerId"`
	MaxTasks int    `json:"maxTasks"`
	Topics   []struct {
		TopicName    string `json:"topicName"`
		LockDuration int    `json:"lockDuration"` // milliseconds
	} `json:"topics"`
}

type CamundaVariable struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

type FetchAndLockResponse struct {
	ID        string                     `json:"id"`
	TopicName string                     `json:"topicName"`
	Variables map[string]CamundaVariable `json:"variables"`
	Retries   int                        `json:"retries"`
	WorkerId  string                     `json:"workerId"`
}

type CompleteRequest struct {
	Variables map[string]interface{} `json:"variables"`
}

type FailureRequest struct {
	ErrorMessage string `json:"errorMessage"`
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
				result[key] = val
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

func (ctrl *ExternalTaskController) FetchAndLock(c *fiber.Ctx) error {
	var req FetchAndLockRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "invalid request body",
			"error":   err.Error(),
		})
	}

	if req.MaxTasks <= 0 {
		req.MaxTasks = 10 // default
	}

	// Prepare topic configs
	topics := make([]repository.TopicConfig, len(req.Topics))
	for i, topic := range req.Topics {
		topics[i] = repository.TopicConfig{
			TopicName:    topic.TopicName,
			LockDuration: topic.LockDuration,
		}
	}

	// Begin transaction
	tx, err := ctrl.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to begin transaction",
			"error":   err.Error(),
		})
	}
	defer tx.Rollback()

	// Fetch and lock jobs using repository
	topicJobs, err := ctrl.externalTaskRepo.FetchAndLockJobsTx(tx, topics, req.WorkerId, req.MaxTasks)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to fetch jobs",
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

	// Build response
	response := []FetchAndLockResponse{}
	for _, topicJob := range topicJobs {
		for _, job := range topicJob.Jobs {
			// Parse variables from payload
			rawVars := make(map[string]interface{})
			if len(job.Payload) > 0 {
				if err := json.Unmarshal(job.Payload, &rawVars); err != nil {
					log.Printf("Warning: failed to parse job payload for job %s: %v", job.ID, err)
				}
			}

			// Convert to Camunda variable format (typed)
			convertedVars := formatVariablesForResponse(rawVars)

			response = append(response, FetchAndLockResponse{
				ID:        job.ID.String(),
				TopicName: topicJob.TopicName,
				Variables: convertedVars,
				Retries:   job.Retries,
				WorkerId:  req.WorkerId,
			})
		}
	}

	return c.JSON(response)
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

	tx, err := ctrl.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to begin transaction",
			"error":   err.Error(),
		})
	}
	defer tx.Rollback()

	// Get job with execution context
	jobWithExec, err := ctrl.externalTaskRepo.GetJobWithExecutionTx(tx, jobID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to fetch job details",
			"error":   err.Error(),
		})
	}

	if jobWithExec == nil {
		return c.Status(404).JSON(fiber.Map{
			"title":   "Not Found",
			"message": "job not found",
		})
	}

	if jobWithExec.Status != "locked" {
		return c.Status(400).JSON(fiber.Map{
			"title":   "Invalid State",
			"message": fmt.Sprintf("job cannot be completed, current status: %s", jobWithExec.Status),
		})
	}

	// Merge variables
	existingVars, err := ctrl.processInstanceRepo.GetVariables(jobWithExec.ProcessInstanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to fetch variables",
			"error":   err.Error(),
		})
	}

	for k, v := range normalizedVars {
		existingVars[k] = v
	}

	if err := ctrl.processInstanceRepo.UpsertVariablesTx(tx, jobWithExec.ProcessInstanceID, existingVars); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to update variables",
			"error":   err.Error(),
		})
	}

	// Mark job as completed
	if err := ctrl.externalTaskRepo.CompleteJobTx(tx, jobID); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to complete job",
			"error":   err.Error(),
		})
	}

	// Store variables for goroutine before potential commit
	processInstanceID := jobWithExec.ProcessInstanceID
	executionID := jobWithExec.ExecutionID
	currentElementID := jobWithExec.CurrentElementID

	// Check if this is a multi-instance child
	var isChild bool
	var parentExecID uuid.UUID

	err = tx.Get(&isChild, `
		SELECT EXISTS(SELECT 1 FROM public.multi_instance_children WHERE child_execution_id = $1)
	`, executionID)

	if err == nil && isChild {
		log.Printf("📦 Multi-instance child completed: %s", executionID)

		// Mark child as completed
		_, err = tx.Exec(`
			UPDATE public.multi_instance_children 
			SET status = 'completed', completed_at = NOW()
			WHERE child_execution_id = $1
		`, executionID)
		if err != nil {
			return err
		}

		// Get parent execution ID
		err = tx.Get(&parentExecID, `
			SELECT parent_execution_id FROM public.multi_instance_children 
			WHERE child_execution_id = $1
		`, executionID)
		if err != nil {
			return err
		}

		// Update completed count
		_, err = tx.Exec(`
			UPDATE public.multi_instance_executions 
			SET completed_count = (
				SELECT COUNT(*) FROM public.multi_instance_children 
				WHERE parent_execution_id = $1 AND status = 'completed'
			),
			updated_at = NOW()
			WHERE execution_id = $1
		`, parentExecID)
		if err != nil {
			log.Printf("Warning: failed to update completed count: %v", err)
		}

		// Get current progress
		var completedCount, totalCount int
		tx.Get(&completedCount, `
			SELECT COUNT(*) FROM public.multi_instance_children 
			WHERE parent_execution_id = $1 AND status = 'completed'
		`, parentExecID)
		tx.Get(&totalCount, `
			SELECT total_count FROM public.multi_instance_executions 
			WHERE execution_id = $1
		`, parentExecID)

		log.Printf("📊 Multi-instance progress: %d/%d completed", completedCount, totalCount)

		// Check if this is sequential
		var isSequential bool
		tx.Get(&isSequential, `
			SELECT is_sequential FROM public.multi_instance_executions 
			WHERE execution_id = $1
		`, parentExecID)

		if isSequential && completedCount < totalCount {
			log.Printf("🔄 Sequential: Creating next child %d/%d", completedCount+1, totalCount)

			// Get the activity_id
			var activityID string
			tx.Get(&activityID, `
        SELECT activity_id FROM public.multi_instance_executions 
        WHERE execution_id = $1
    `, parentExecID)

			// Set parent to active to create next child
			_, err = tx.Exec(`
        UPDATE public.executions 
        SET current_element_id = $1, status = 'active', updated_at = NOW()
        WHERE id = $2
    `, activityID, parentExecID)
			if err != nil {
				log.Printf("Warning: failed to update parent: %v", err)
			}

			// Update loop counter
			_, err = tx.Exec(`
        UPDATE public.multi_instance_executions 
        SET loop_counter = $1, updated_at = NOW()
        WHERE execution_id = $2
    `, completedCount+1, parentExecID)
			if err != nil {
				log.Printf("Warning: failed to update loop counter: %v", err)
			}

			// Continue with the rest of the function - don't return early
			// The parent will be processed when the runtime continues
		}

		// Check if all children are complete
		if completedCount >= totalCount && totalCount > 0 {
			log.Printf("✅ All %d children completed", totalCount)

			// Get next node for parent
			var parentNodeID string
			tx.Get(&parentNodeID, `SELECT current_element_id FROM public.executions WHERE id = $1`, parentExecID)

			// Load graph to find next node
			graphJSON, err := ctrl.externalTaskRepo.GetProcessDefinitionGraphTx(tx, jobWithExec.ProcessInstanceID)
			if err == nil {
				var graph engine.ProcessGraph
				json.Unmarshal(graphJSON, &graph)

				if parentNode, ok := graph.Nodes[parentNodeID]; ok && len(parentNode.Outgoing) > 0 {
					nextNodeID := parentNode.Outgoing[0].TargetRef
					_, err = tx.Exec(`
						UPDATE public.executions 
						SET current_element_id = $1, status = 'active', updated_at = NOW()
						WHERE id = $2
					`, nextNodeID, parentExecID)
					if err != nil {
						log.Printf("Warning: failed to move parent: %v", err)
					}
				}
			}
		}
	}

	// Load process graph for normal flow
	graphJSON, err := ctrl.externalTaskRepo.GetProcessDefinitionGraphTx(tx, processInstanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to load process definition",
			"error":   err.Error(),
		})
	}

	var graph engine.ProcessGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Processing Error",
			"message": "failed to parse process graph",
			"error":   err.Error(),
		})
	}

	currentNode, err := ctrl.externalTaskRepo.GetNodeByID(&graph, currentElementID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Configuration Error",
			"message": "current node not found",
			"error":   err.Error(),
		})
	}

	var nextNodeID string

	switch currentNode.Type {
	case engine.ServiceTaskType:
		if len(currentNode.Outgoing) == 0 {
			if err := ctrl.externalTaskRepo.CompleteProcessInstanceTx(tx, processInstanceID); err != nil {
				return err
			}
			if err := ctrl.externalTaskRepo.CompleteExecutionTx(tx, executionID); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			return c.JSON(fiber.Map{
				"title":   "Success",
				"message": "process completed",
				"jobId":   jobID,
			})
		}
		nextNodeID, err = common.ResolveNext(currentNode, existingVars)
		if err != nil {
			return err
		}

	case engine.BoundaryTimerEventType:
		if len(currentNode.Outgoing) == 0 {
			return c.Status(500).JSON(fiber.Map{
				"title":   "Configuration Error",
				"message": "boundary event has no outgoing flow",
				"error":   "no outgoing flow",
			})
		}
		nextNodeID = currentNode.Outgoing[0].TargetRef

	default:
		return c.Status(500).JSON(fiber.Map{
			"title":   "Configuration Error",
			"message": fmt.Sprintf("node %s is not a service task or boundary event, type: %s", currentNode.ID, currentNode.Type),
			"error":   "invalid node type",
		})
	}

	if err := ctrl.externalTaskRepo.UpdateExecutionCurrentNodeTx(tx, executionID, nextNodeID); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Resume current execution
	rt := runtime.NewRuntime(&graph, ctrl.db)
	if err := rt.ExecuteExecution(c.Context(), executionID); err != nil {
		log.Printf("Execution resume error: %v", err)
		return c.Status(500).JSON(fiber.Map{
			"title":   "Execution Error",
			"message": "failed to resume process execution",
			"error":   err.Error(),
		})
	}

	engineRepo := repository.NewEngineRepository(ctrl.db)
	// Get the execution that owns this job
	exec, err := engineRepo.GetExecutionByID(executionID)
	if exec != nil && exec.ParentExecutionID != nil {
		// This is a multi-instance child - trigger progression
		rt.OnMultiInstanceChildCompleted(context.Background(), exec.ID, map[string]interface{}{})
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

	tx, err := ctrl.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to begin transaction",
			"error":   err.Error(),
		})
	}
	defer tx.Rollback()

	// Decrement retries
	newRetries := req.Retries - 1

	if newRetries <= 0 {
		// No more retries - mark as failed permanently
		if err := ctrl.externalTaskRepo.FailJobPermanentlyTx(tx, jobID, req.ErrorMessage); err != nil {
			return c.Status(500).JSON(fiber.Map{
				"title":   "Database Error",
				"message": "failed to mark job as failed",
				"error":   err.Error(),
			})
		}
	} else {
		// Reset job for retry
		if err := ctrl.externalTaskRepo.ResetJobForRetryTx(tx, jobID, newRetries, req.ErrorMessage); err != nil {
			return c.Status(500).JSON(fiber.Map{
				"title":   "Database Error",
				"message": "failed to reset job for retry",
				"error":   err.Error(),
			})
		}
	}

	if err := tx.Commit(); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to commit transaction",
			"error":   err.Error(),
		})
	}

	message := "failure recorded, job will be retried"
	if newRetries <= 0 {
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
