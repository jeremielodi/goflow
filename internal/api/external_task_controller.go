package api

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
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

			// Convert to Camunda variable format
			convertedVars := make(map[string]CamundaVariable)
			for k, v := range rawVars {
				convertedVars[k] = CamundaVariable{
					Value: v,
					Type:  getCamundaVariableType(v),
				}
			}

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

	// Merge new variables
	for k, v := range req.Variables {
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

	// Get service task node
	serviceNode, err := ctrl.externalTaskRepo.GetServiceTaskNode(&graph, jobWithExec.CurrentElementID)
	if err != nil {
		fmt.Println("10. ", err.Error())
		return c.Status(500).JSON(fiber.Map{
			"title":   "Configuration Error",
			"message": "service task node not found",
			"error":   err.Error(),
		})
	}

	// If no outgoing flows, complete the process
	if len(serviceNode.Outgoing) == 0 {

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

	// Resolve next node
	rt := runtime.NewRuntime(&graph, ctrl.db)
	nextNodeID, err := rt.ResolveNext(c.Context(), serviceNode, existingVars)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"title":   "Execution Error",
			"message": "failed to resolve next node",
			"error":   err.Error(),
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
	rt = &runtime.Runtime{Graph: &graph, DB: ctrl.db}
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
