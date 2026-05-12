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
func (pc *ProcessInstanceController) CompleteTask(c *fiber.Ctx) error {
	taskID, _ := uuid.Parse(c.Params("id"))
	var req struct {
		Variables map[string]interface{} `json:"variables"`
	}
	c.BodyParser(&req)

	svc := service.NewTaskService(pc.db)
	err := svc.CompleteTask(c.Context(), taskID, req.Variables)
	if err != nil {
		return c.JSON(fiber.Map{"title": "Error", "message": "Task not completed", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"message": "completed"})
}
