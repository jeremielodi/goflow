package api

import (
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

	// Normalize variables (supports both simple and typed formats)
	normalizedVars := normalizeVariablesForExternal(req.Variables)

	// Begin transaction
	tx, err := ctrl.db.Beginx()
	if err != nil {
		fmt.Println("2. ", err.Error())
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
		fmt.Println("3. ", err.Error())
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
		fmt.Println("5. ", err.Error())
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to fetch variables",
			"error":   err.Error(),
		})
	}

	// Merge normalized variables
	for k, v := range normalizedVars {
		existingVars[k] = v
	}

	// Update variables
	if err := ctrl.processInstanceRepo.UpsertVariablesTx(tx, jobWithExec.ProcessInstanceID, existingVars); err != nil {
		fmt.Println("6. ", err.Error())
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to update variables",
			"error":   err.Error(),
		})
	}

	// Mark job as completed
	if err := ctrl.externalTaskRepo.CompleteJobTx(tx, jobID); err != nil {
		fmt.Println("7. ", err.Error())
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to complete job",
			"error":   err.Error(),
		})
	}

	// Load process graph
	graphJSON, err := ctrl.externalTaskRepo.GetProcessDefinitionGraphTx(tx, jobWithExec.ProcessInstanceID)
	if err != nil {
		fmt.Println("8. ", err.Error())
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to load process definition",
			"error":   err.Error(),
		})
	}

	var graph engine.ProcessGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		fmt.Println("9. ", err.Error())
		return c.Status(500).JSON(fiber.Map{
			"title":   "Processing Error",
			"message": "failed to parse process graph",
			"error":   err.Error(),
		})
	}

	// Get the current node (could be service task OR boundary event)
	currentNode, err := ctrl.externalTaskRepo.GetNodeByID(&graph, jobWithExec.CurrentElementID)
	if err != nil {
		fmt.Println("10. ", err.Error())
		return c.Status(500).JSON(fiber.Map{
			"title":   "Configuration Error",
			"message": "current node not found",
			"error":   err.Error(),
		})
	}

	var nextNodeID string

	// Handle different node types
	switch currentNode.Type {
	case engine.ServiceTaskType:
		// This is a regular service task
		if len(currentNode.Outgoing) == 0 {
			// No outgoing flows - complete the process
			if err := ctrl.externalTaskRepo.CompleteProcessInstanceTx(tx, jobWithExec.ProcessInstanceID); err != nil {
				return c.Status(500).JSON(fiber.Map{
					"title":   "Database Error",
					"message": "failed to complete process instance",
					"error":   err.Error(),
				})
			}
			if err := ctrl.externalTaskRepo.CompleteExecutionTx(tx, jobWithExec.ExecutionID); err != nil {
				fmt.Println("12. ", err.Error())
				return c.Status(500).JSON(fiber.Map{
					"title":   "Database Error",
					"message": "failed to complete execution",
					"error":   err.Error(),
				})
			}
			if err := tx.Commit(); err != nil {
				fmt.Println("14. ", err.Error())
				return c.Status(500).JSON(fiber.Map{
					"title":   "Database Error",
					"message": "failed to commit transaction",
					"error":   err.Error(),
				})
			}
			return c.JSON(fiber.Map{
				"title":   "Success",
				"message": "process completed",
				"jobId":   jobID,
			})
		}
		nextNodeID, err = common.ResolveNext(currentNode, existingVars)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"title":   "Execution Error",
				"message": "failed to resolve next node",
				"error":   err.Error(),
			})
		}

	case engine.BoundaryTimerEventType:
		// This is a boundary timer event - follow its outgoing flow
		if len(currentNode.Outgoing) == 0 {
			return c.Status(500).JSON(fiber.Map{
				"title":   "Configuration Error",
				"message": "boundary event has no outgoing flow",
				"error":   "no outgoing flow",
			})
		}
		nextNodeID = currentNode.Outgoing[0].TargetRef
		// log.Printf("Boundary event %s completed, moving to node %s", currentNode.ID, nextNodeID)

	default:
		return c.Status(500).JSON(fiber.Map{
			"title":   "Configuration Error",
			"message": fmt.Sprintf("node %s is not a service task or boundary event, type: %s", currentNode.ID, currentNode.Type),
			"error":   "invalid node type",
		})
	}

	// Move execution to next node
	if err := ctrl.externalTaskRepo.UpdateExecutionCurrentNodeTx(tx, jobWithExec.ExecutionID, nextNodeID); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to update execution",
			"error":   err.Error(),
		})
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "failed to commit transaction",
			"error":   err.Error(),
		})
	}

	// Resume execution outside transaction
	rt := runtime.NewRuntime(&graph, ctrl.db)
	if err := rt.ExecuteExecution(c.Context(), jobWithExec.ExecutionID); err != nil {
		log.Printf("Execution resume error: %v", err)
		return c.Status(500).JSON(fiber.Map{
			"title":   "Execution Error",
			"message": "failed to resume process execution",
			"error":   err.Error(),
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

	// process_order
	// process_order
	var jobs []models.Job
	query := `SELECT id, process_instance_id, execution_id, job_type, status, payload, retries, created_at, updated_at FROM public.jobs WHERE status = 'pending'`

	if processInstanceID != "" {
		pid, err := uuid.Parse(processInstanceID)
		if err == nil {
			query += " AND process_instance_id = $1"
			if topic != "" {
				query += " AND job_type = $2"
				ctrl.db.Select(&jobs, query, pid, topic)
			} else {
				ctrl.db.Select(&jobs, query, pid)
			}
		}
	} else if topic != "" {
		query += " AND job_type = $1"
		ctrl.db.Select(&jobs, query, topic)
	} else {
		ctrl.db.Select(&jobs, query)
	}

	return c.JSON(jobs)
}
