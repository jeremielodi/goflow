package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type TaskController struct {
	db   *sqlx.DB
	util common.Util
}

func NewTaskController(db *sqlx.DB, rootDirPath *string) *TaskController {
	return &TaskController{
		db:   db,
		util: *common.NewUtil(rootDirPath),
	}
}

func (tc *TaskController) GetTasks(c *fiber.Ctx) error {
	assignee := c.Query("assignee")
	id := c.Query("id")
	group := c.Query("candidateGroup")
	status := c.Query("status")
	processInstanceId := c.Query("processInstanceId")
	repo := repository.NewTaskRepository(tc.db)
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

	tasks, err = repo.FindAll(params)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(tasks)
}

func (tc *TaskController) Claim(c *fiber.Ctx) error {

	type ClaimBody struct {
		Assignee string `json:"assignee"`
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

	if assignee == "" {
		return c.Status(400).JSON(fiber.Map{"success": false, "message": "Assignee not defined"})
	}

	taskId, err := uuid.Parse(id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	repo := repository.NewTaskRepository(tc.db)
	task, err := repo.FindTaskByID(taskId)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if task.Assignee != nil && *task.Assignee != "" {
		return c.Status(400).JSON(fiber.Map{"success": false, "message": fmt.Sprintf("Task already assigned to %s", *task.Assignee)})
	}

	_, err = repo.ClaimTask(taskId, assignee)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"success": false, "error": err.Error()})
	}
	return c.Status(200).JSON(fiber.Map{"success": true, "message": "task assigned successfully"})
}

func (tc *TaskController) UnClaim(c *fiber.Ctx) error {
	id := c.Params("id")

	taskId, err := uuid.Parse(id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	repo := repository.NewTaskRepository(tc.db)
	task, err := repo.FindTaskByID(taskId)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if task.Assignee == nil {
		return c.Status(400).JSON(fiber.Map{"success": false, "message": "Task not assigned"})
	}

	_, err = repo.UnClaimTask(taskId)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{"success": false, "error": err.Error()})
	}
	return c.Status(200).JSON(fiber.Map{"success": true, "message": "task unclaimed successfully"})
}
