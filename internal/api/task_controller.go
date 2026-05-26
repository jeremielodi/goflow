package api

import (
	"github.com/gofiber/fiber/v2"
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
