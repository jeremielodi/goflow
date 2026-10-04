// internal/api/task_controller.go
package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

// checkTaskAccess verifies the caller has at least `level` access to the
// process instance owning instanceID (see service.CanAccessProcessInstance)
// — tasks aren't a separate ACL surface, they inherit their owning
// process's per-user grants. Returns true (fail open) if there's no
// authenticated user in context.
func (tc *TaskController) checkTaskAccess(c *fiber.Ctx, instanceID uuid.UUID, level string) bool {
	userID, err := uuid.Parse(common.GetUserUuid(c))
	if err != nil {
		return true
	}
	allowed, _ := service.CanAccessProcessInstance(tc.db, userID, instanceID, level)
	return allowed
}

type TaskController struct {
	db           *sqlx.DB
	util         common.Util
	dispatcher   *events.TaskEventDispatcher
	auditService *service.AuditService
}

func NewTaskController(db *sqlx.DB, rootDirPath *string, dispatcher *events.TaskEventDispatcher) *TaskController {
	return &TaskController{
		db:           db,
		util:         *common.NewUtil(rootDirPath),
		dispatcher:   dispatcher,
		auditService: service.NewAuditService(db),
	}
}

func (tc *TaskController) GetTasks(c *fiber.Ctx) error {
	assignee := c.Query("assignee")
	id := c.Query("id")
	group := c.Query("candidateGroup")
	status := c.Query("status")
	processInstanceId := c.Query("processInstanceId")

	repo := repository.NewTaskRepository(tc.db, tc.dispatcher)
	var params = map[string]interface{}{}
	var tasks []models.Task
	var err error

	if assignee != "" {
		params["assignee"] = assignee
	}
	if id != "" {
		params["id"] = id
	}
	if group != "" {
		params["candidate_group"] = group
	}
	if status != "" {
		params["status"] = status
	}
	if processInstanceId != "" {
		params["process_instance_id"] = processInstanceId
	}

	// Matches Camunda 7's GET /task semantics: only currently-open tasks are
	// returned by default. Completed/cancelled tasks belong to the History
	// API, unless the caller explicitly asks for them via ?status=.
	tasks, err = repo.FindAll(params, "completed", "cancelled")
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// Resolve each task's linked form (if any) from its process graph, so
	// the frontend can render a real form instead of a generic JSON blob.
	// Also filter out tasks belonging to a process the caller lacks VIEW
	// access to — mirrors ListDefinitions/ListHistoricProcessInstances'
	// per-row filtering, since a task list can span many process keys.
	engineRepo := repository.NewEngineRepository(tc.db, tc.dispatcher)
	userID, userErr := uuid.Parse(common.GetUserUuid(c))
	type taskWithFormKey struct {
		models.Task
		FormKey string `json:"formKey,omitempty"`
	}
	result := make([]taskWithFormKey, 0, len(tasks))
	for _, t := range tasks {
		if userErr == nil {
			if allowed, _ := service.CanAccessProcessInstance(tc.db, userID, t.ProcessInstanceID, "VIEW"); !allowed {
				continue
			}
		}
		formKey := ""
		if graph, err := engineRepo.GetProcessGraphByInstanceID(t.ProcessInstanceID); err == nil && graph != nil {
			if n, ok := graph.Nodes[t.TaskDefinitionKey]; ok {
				formKey = n.FormKey
			}
		}
		result = append(result, taskWithFormKey{Task: t, FormKey: formKey})
	}

	return c.JSON(result)
}

func (tc *TaskController) Claim(c *fiber.Ctx) error {
	type ClaimBody struct {
		Assignee string `json:"assignee"`
		UserId   string `json:"userId"`
	}

	var req ClaimBody
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid request body",
			"message": err.Error(),
		})
	}

	assignee := req.Assignee
	id := c.Params("id")

	if req.UserId != "" {
		assignee = req.UserId
	}

	if assignee == "" {
		return c.Status(400).JSON(fiber.Map{"success": false, "message": "Assignee not defined"})
	}

	taskId, err := uuid.Parse(id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	repo := repository.NewTaskRepository(tc.db, tc.dispatcher)
	task, err := repo.FindTaskByID(taskId)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	if task.Assignee != nil && *task.Assignee != "" {
		return c.Status(400).JSON(fiber.Map{
			"success": false,
			"message": fmt.Sprintf("Task already assigned to %s", *task.Assignee),
		})
	}

	if !tc.checkTaskAccess(c, task.ProcessInstanceID, "VIEW") {
		return c.Status(404).JSON(fiber.Map{"error": "task not found"})
	}

	_, err = repo.ClaimTask(taskId, assignee)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"success": false, "error": err.Error()})
	}
	service.LogAuditErr("task claimed", tc.auditService.LogTaskClaimed(taskId, task.ProcessInstanceID, assignee))
	if p := eventlog.Global(); p != nil {
		p.TaskClaimed(task.ProcessInstanceID.String(), taskId.String(), assignee)
	}

	return c.Status(200).JSON(fiber.Map{
		"success": true,
		"message": "task assigned successfully",
	})
}

func (tc *TaskController) UnClaim(c *fiber.Ctx) error {
	id := c.Params("id")

	taskId, err := uuid.Parse(id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	repo := repository.NewTaskRepository(tc.db, tc.dispatcher)
	task, err := repo.FindTaskByID(taskId)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	if task.Assignee == nil {
		return c.Status(400).JSON(fiber.Map{
			"success": false,
			"message": "Task not assigned",
		})
	}

	if !tc.checkTaskAccess(c, task.ProcessInstanceID, "VIEW") {
		return c.Status(404).JSON(fiber.Map{"error": "task not found"})
	}

	_, err = repo.UnClaimTask(taskId)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"success": false, "error": err.Error()})
	}
	service.LogAuditErr("task unclaimed", tc.auditService.LogTaskUnclaimed(taskId, task.ProcessInstanceID))
	if p := eventlog.Global(); p != nil {
		p.TaskUnclaimed(task.ProcessInstanceID.String(), taskId.String())
	}

	return c.Status(200).JSON(fiber.Map{
		"success": true,
		"message": "task unclaimed successfully",
	})
}

// GetTask returns a single task by ID
func (tc *TaskController) GetTask(c *fiber.Ctx) error {
	id := c.Params("id")

	taskId, err := uuid.Parse(id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	repo := repository.NewTaskRepository(tc.db, tc.dispatcher)
	task, err := repo.FindTaskByID(taskId)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	if !tc.checkTaskAccess(c, task.ProcessInstanceID, "VIEW") {
		return c.Status(404).JSON(fiber.Map{"error": "task not found"})
	}

	return c.JSON(task)
}
