package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jmoiron/sqlx"
)

type ExternalTaskController struct {
	db *sqlx.DB
}

func NewExternalTaskController(db *sqlx.DB) *ExternalTaskController {
	return &ExternalTaskController{db: db}
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

// FetchAndLockResponse is a slice of external tasks
type FetchAndLockResponse struct {
	ID        string                     `json:"id"`
	TopicName string                     `json:"topicName"`
	Variables map[string]CamundaVariable `json:"variables"`
	Retries   int                        `json:"retries"`
	WorkerId  string                     `json:"workerId"`
}

// HandleFailureRequest matches Camunda 7 failure request

type FailureRequest struct {
	ErrorMessage string `json:"errorMessage"`
	Retries      int    `json:"retries"`
	RetryTimeout int    `json:"retryTimeout"`
}

func (ctrl *ExternalTaskController) FetchAndLock(c *fiber.Ctx) error {
	var req FetchAndLockRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	response := []FetchAndLockResponse{}

	tx, err := ctrl.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	defer tx.Rollback()

	for _, topic := range req.Topics {
		var jobs []struct {
			ID      uuid.UUID       `db:"id"`
			Payload json.RawMessage `db:"payload"`
			Retries int             `db:"retries"`
			ExecID  uuid.UUID       `db:"execution_id"`
		}

		query := `
			SELECT id, payload, retries, execution_id
			FROM public.jobs
			WHERE job_type = $1 AND status = 'pending'
			  AND (locked_by IS NULL OR locked_until < NOW())
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		`
		err = tx.Select(&jobs, query, topic.TopicName, req.MaxTasks)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		for _, job := range jobs {
			lockedUntil := time.Now().Add(time.Duration(topic.LockDuration) * time.Millisecond)

			_, err = tx.Exec(`
				UPDATE public.jobs
				SET status = 'locked', locked_by = $1, locked_until = $2
				WHERE id = $3
			`, req.WorkerId, lockedUntil, job.ID)
			if err != nil {
				continue
			}

			rawVars := make(map[string]interface{})
			if len(job.Payload) > 0 {
				if err := json.Unmarshal(job.Payload, &rawVars); err != nil {
					continue
				}
			}

			convertedVars := make(map[string]CamundaVariable)
			for k, v := range rawVars {
				convertedVars[k] = CamundaVariable{
					Value: v,
					Type:  getCamundaVariableType(v),
				}
			}
			type ExternalTaskItem struct {
				ID        string                     `json:"id"`
				TopicName string                     `json:"topicName"`
				Variables map[string]CamundaVariable `json:"variables"`
				Retries   int                        `json:"retries"`
				WorkerId  string                     `json:"workerId"`
			}

			response = append(response, FetchAndLockResponse{
				ID:        job.ID.String(),
				TopicName: topic.TopicName,
				Variables: convertedVars,
				Retries:   job.Retries,
				WorkerId:  req.WorkerId,
			})
		}
	}

	if err := tx.Commit(); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(response)
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

// CompleteTask handles POST /engine-rest/external-task/:id/complete
type CompleteRequest struct {
	Variables map[string]interface{} `json:"variables"`
}

func (ctrl *ExternalTaskController) CompleteTask(c *fiber.Ctx) error {
	jobID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid task id"})
	}

	var req CompleteRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	// Start explicit transaction
	tx, err := ctrl.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to start transaction"})
	}
	defer tx.Rollback() // will rollback if we don't commit

	// 1. Get job, execution, process instance, and current status
	var execID, instanceID uuid.UUID
	var serviceTaskID string
	var jobStatus string
	err = tx.QueryRow(`
        SELECT j.execution_id, j.process_instance_id, e.current_element_id, j.status
        FROM public.jobs j
        JOIN public.executions e ON e.id = j.execution_id
        WHERE j.id = $1
    `, jobID).Scan(&execID, &instanceID, &serviceTaskID, &jobStatus)
	if err != nil {
		log.Printf("Error fetching job: %v", err)
		return c.Status(404).JSON(fiber.Map{"error": "job not found"})
	}

	if jobStatus != "locked" {
		return c.Status(400).JSON(fiber.Map{"error": fmt.Sprintf("job not locked, status=%s", jobStatus)})
	}

	// 2. Merge variables
	var existingVars map[string]interface{}
	var data []byte
	err = tx.Get(&data, `SELECT data FROM public.variables WHERE process_instance_id = $1`, instanceID)
	if err != nil && err != sql.ErrNoRows {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch variables"})
	}
	if len(data) > 0 {
		json.Unmarshal(data, &existingVars)
	} else {
		existingVars = make(map[string]interface{})
	}
	for k, v := range req.Variables {
		existingVars[k] = v
	}

	newVarsJSON, _ := json.Marshal(existingVars)
	// 3. Update variables (delete then insert – works without unique constraint)
	_, err = tx.Exec(`DELETE FROM public.variables WHERE process_instance_id = $1`, instanceID)
	if err != nil {
		log.Printf("Error deleting old variables: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "failed to delete old variables"})
	}
	_, err = tx.Exec(`
    INSERT INTO public.variables (id, process_instance_id, data, updated_at)
    VALUES ($1, $2, $3, NOW())
	`, uuid.New(), instanceID, newVarsJSON)
	if err != nil {
		log.Printf("Error inserting new variables: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "failed to insert variables"})
	}

	// 4. Mark job as completed
	_, err = tx.Exec(`
        UPDATE public.jobs
        SET status = 'completed', completed_at = NOW()
        WHERE id = $1
    `, jobID)
	if err != nil {
		log.Printf("Error updating job: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "failed to complete job"})
	}

	// 5. Load graph
	var defID uuid.UUID
	err = tx.Get(&defID, `SELECT process_definition_id FROM public.process_instances WHERE id = $1`, instanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "process instance not found"})
	}
	var graphJSON []byte
	err = tx.Get(&graphJSON, `SELECT parsed_graph FROM public.process_definitions WHERE id = $1`, defID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "process definition not found"})
	}
	var graph engine.ProcessGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to parse graph"})
	}

	serviceNode, ok := graph.Nodes[serviceTaskID]
	if !ok {
		return c.Status(500).JSON(fiber.Map{"error": "service task node not found"})
	}

	// 6. If no outgoing flows, end process instance
	if len(serviceNode.Outgoing) == 0 {
		_, err = tx.Exec(`UPDATE public.process_instances SET status = 'completed', ended_at = NOW() WHERE id = $1`, instanceID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "failed to end process"})
		}
		_, err = tx.Exec(`UPDATE public.executions SET status = 'completed', is_active = false WHERE id = $1`, execID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "failed to complete execution"})
		}
		if err := tx.Commit(); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "commit failed"})
		}

		return c.JSON(fiber.Map{"message": "process completed", "jobId": jobID})
	}

	// 7. Resolve next node
	nextID, err := resolveNextNode(&graph, serviceNode, existingVars)
	if err != nil {
		log.Printf("Error resolving next node: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// 8. Move execution token
	_, err = tx.Exec(`
        UPDATE public.executions
        SET current_element_id = $1, status = 'active', updated_at = NOW()
        WHERE id = $2
    `, nextID, execID)
	if err != nil {
		log.Printf("Error moving execution: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "failed to move execution"})
	}

	// 9. Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Commit failed: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "commit failed"})
	}

	// 10. Resume execution (outside transaction)
	rt := &runtime.Runtime{Graph: &graph, DB: ctrl.db}
	if err := rt.ExecuteExecution(c.Context(), execID); err != nil {
		log.Printf("Execution resume error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	log.Printf("Job %s completed and execution moved to %s", jobID, nextID)
	return c.JSON(fiber.Map{"message": "job completed"})
}

func (ctrl *ExternalTaskController) HandleFailure(c *fiber.Ctx) error {
	jobID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid task id"})
	}

	var req FailureRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}

	// Decrement retries
	newRetries := req.Retries - 1
	if newRetries <= 0 {
		// No more retries → mark job as failed permanently
		_, err = ctrl.db.Exec(`
            UPDATE public.jobs
            SET status = 'failed', error_message = $1
            WHERE id = $2
        `, req.ErrorMessage, jobID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		// Optionally set execution to failed or terminate process instance
		// For now, just return success
		return c.JSON(fiber.Map{"message": "job marked as failed"})
	}

	// Reset job to pending with decremented retries
	_, err = ctrl.db.Exec(`
        UPDATE public.jobs
        SET retries = $1, status = 'pending', locked_by = NULL, locked_until = NULL, error_message = $2
        WHERE id = $3
    `, newRetries, req.ErrorMessage, jobID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"message": "failure recorded, job will be retried"})
}

// Helper resolveNextNode (copied from earlier)
func resolveNextNode(graph *engine.ProcessGraph, node *engine.Node, vars map[string]interface{}) (string, error) {
	if len(node.Outgoing) == 0 {
		return "", fmt.Errorf("no outgoing flows")
	}
	if len(node.Outgoing) == 1 {
		flow := node.Outgoing[0]
		if flow.Condition != "" {
			ok, err := runtime.EvaluateCondition(flow.Condition, vars)
			if err != nil {
				return "", err
			}
			if !ok {
				return "", fmt.Errorf("condition false on single outgoing flow")
			}
		}
		return flow.TargetRef, nil
	}
	// exclusive gateway
	var selected *engine.Flow
	for _, flow := range node.Outgoing {
		ok, err := runtime.EvaluateCondition(flow.Condition, vars)
		if err != nil {
			return "", err
		}
		if ok {
			if selected != nil {
				return "", fmt.Errorf("multiple conditions true")
			}
			selected = &flow
		}
	}
	if selected == nil {
		return "", fmt.Errorf("no matching condition")
	}
	return selected.TargetRef, nil
}
