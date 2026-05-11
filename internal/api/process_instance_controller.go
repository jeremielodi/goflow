package api

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jmoiron/sqlx"
)

type ProcessInstanceController struct {
	db *sqlx.DB
}

func NewProcessInstanceController(db *sqlx.DB) *ProcessInstanceController {
	return &ProcessInstanceController{db: db}
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
		return c.Status(400).JSON(fiber.Map{"error": "process key required"})
	}

	var body StartProcessRequest
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
	}

	// Load definition and graph
	processRepo := repository.NewProcessRepository(pc.db)
	def, err := processRepo.FindLatestProcessDefinitionByKey(processKey)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "process definition not found"})
	}

	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to parse process graph"})
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
		return c.Status(500).JSON(fiber.Map{"error": "no start event found"})
	}

	// Begin transaction
	tx, err := pc.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "db transaction error"})
	}
	defer tx.Rollback()

	// Create process instance
	instanceID := uuid.New()
	now := time.Now()

	_, err = tx.Exec(`
        INSERT INTO public.process_instances
            (id, process_definition_id, status, started_at)
        VALUES ($1, $2, 'running', $3)
    `, instanceID, def.ID, now)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"erro":    err.Error(),
			"message": "failed to create instance"})
	}

	// Save variables
	varData, _ := json.Marshal(body.Variables)
	_, err = tx.Exec(`
        INSERT INTO public.variables
            (id, process_instance_id, data, updated_at)
        VALUES ($1, $2, $3, $4)
    `, uuid.New(), instanceID, varData, now)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to save variables"})
	}

	// Create initial execution
	execID := uuid.New()
	_, err = tx.Exec(`
        INSERT INTO public.executions
            (id, process_instance_id, current_element_id, status, is_active, created_at, updated_at)
        VALUES ($1, $2, $3, 'active', true, $4, $5)
    `, execID, instanceID, startNodeID, now, now)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to create execution"})
	}

	if err := tx.Commit(); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to commit transaction"})
	}

	// Run the runtime
	rt := &runtime.Runtime{
		Graph: &graph,
		DB:    pc.db,
	}
	ctx := c.Context()
	err = rt.ExecuteExecution(ctx, execID)
	if err != nil {
		// Optionally mark instance as failed
		return c.Status(500).JSON(fiber.Map{"content": "Execution error",
			"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"processInstanceId": instanceID,
		"executionId":       execID,
		"processKey":        def.ProcessKey,
		"version":           def.Version,
	})
}

// ============================================================
// COMPLETE USER TASK
// POST /tasks/:id/complete
// ============================================================
type CompleteTaskRequest struct {
	Variables map[string]interface{} `json:"variables"`
}

func (pc *ProcessInstanceController) CompleteTask(c *fiber.Ctx) error {
	taskID, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid task id"})
	}

	var body CompleteTaskRequest
	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
	}

	// Load task and linked information
	var task struct {
		ID                uuid.UUID `db:"id"`
		ProcessInstanceID uuid.UUID `db:"process_instance_id"`
		ExecutionID       uuid.UUID `db:"execution_id"`
		TaskDefinitionKey string    `db:"task_definition_key"`
		Status            string    `db:"status"`
	}
	err = pc.db.Get(&task, `
        SELECT id, process_instance_id, execution_id, task_definition_key, status
        FROM public.tasks
        WHERE id = $1
    `, taskID)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "task not found"})
	}
	if task.Status != "created" {
		return c.Status(400).JSON(fiber.Map{"error": "task already completed or claimed"})
	}

	// Begin transaction to update task and variables
	tx, err := pc.db.Beginx()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "transaction error"})
	}
	defer tx.Rollback()

	// Mark task completed
	_, err = tx.Exec(`
        UPDATE public.tasks
        SET status = 'completed', completed_at = $1
        WHERE id = $2
    `, time.Now(), task.ID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to update task"})
	}

	// Merge variables
	var existingData []byte
	err = tx.Get(&existingData, `SELECT data FROM public.variables WHERE process_instance_id = $1`, task.ProcessInstanceID)
	if err != nil && err != sql.ErrNoRows {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch variables"})
	}
	vars := make(map[string]interface{})
	if len(existingData) > 0 {
		json.Unmarshal(existingData, &vars)
	}
	for k, v := range body.Variables {
		vars[k] = v
	}
	newData, _ := json.Marshal(vars)
	_, err = tx.Exec(`
        INSERT INTO public.variables (id, process_instance_id, data, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (process_instance_id) DO UPDATE
        SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at
    `, uuid.New(), task.ProcessInstanceID, newData, time.Now())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to update variables"})
	}

	// Resume the execution (set status back to active)
	_, err = tx.Exec(`
        UPDATE public.executions
        SET status = 'active', updated_at = $1
        WHERE id = $2
    `, time.Now(), task.ExecutionID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to resume execution"})
	}

	if err := tx.Commit(); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "commit failed"})
	}

	// Now run the runtime again, starting from the execution
	// Load the process graph again
	var defID uuid.UUID
	err = pc.db.Get(&defID, `
        SELECT process_definition_id FROM public.process_instances WHERE id = $1
    `, task.ProcessInstanceID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "definition not found"})
	}
	processRepo := repository.NewProcessRepository(pc.db)
	def, err := processRepo.FindProcessDefinitionByID(defID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to load process definition"})
	}
	var graph engine.ProcessGraph
	json.Unmarshal(def.ParsedGraph, &graph)

	rt := &runtime.Runtime{
		Graph: &graph,
		DB:    pc.db,
	}
	ctx := c.Context()
	err = rt.ExecuteExecution(ctx, task.ExecutionID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"message": "task completed and process resumed",
		"taskId":  task.ID,
	})
}
