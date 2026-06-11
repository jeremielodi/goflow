// internal/api/historic_task_controller.go
package api

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type HistoricTaskController struct {
	db *sqlx.DB
}

func NewHistoricTaskController(db *sqlx.DB) *HistoricTaskController {
	return &HistoricTaskController{db: db}
}

// HistoricTaskQuery represents Camunda historic task query parameters
type HistoricTaskQuery struct {
	TaskId               string  `query:"taskId"`
	TaskName             string  `query:"taskName"`
	TaskNameLike         string  `query:"taskNameLike"`
	Assignee             *string `query:"assignee"`
	AssigneeLike         string  `query:"assigneeLike"`
	ProcessInstanceId    string  `query:"processInstanceId"`
	ProcessDefinitionId  string  `query:"processDefinitionId"`
	ProcessDefinitionKey string  `query:"processDefinitionKey"`
	Status               string  `query:"status"`
	Finished             bool    `query:"finished"`
	Unfinished           bool    `query:"unfinished"`
	StartedBefore        string  `query:"startedBefore"`
	StartedAfter         string  `query:"startedAfter"`
	FinishedBefore       string  `query:"finishedBefore"`
	FinishedAfter        string  `query:"finishedAfter"`
	SortBy               string  `query:"sortBy"`
	SortOrder            string  `query:"sortOrder"`
	FirstResult          int     `query:"firstResult"`
	MaxResults           int     `query:"maxResults"`
}

// HistoricTaskResponse represents Camunda historic task response
type HistoricTaskResponse struct {
	Id                   string     `json:"id"`
	ProcessInstanceId    string     `json:"processInstanceId"`
	ProcessDefinitionId  string     `json:"processDefinitionId"`
	ProcessDefinitionKey string     `json:"processDefinitionKey"`
	TaskDefinitionKey    string     `json:"taskDefinitionKey"`
	TaskName             string     `json:"taskName"`
	Assignee             *string    `json:"assignee"`
	Owner                string     `json:"owner"`
	Status               string     `json:"status"`
	StartTime            *time.Time `json:"startTime"`
	EndTime              *time.Time `json:"endTime"`
	DurationInMillis     *int64     `json:"durationInMillis"`
	DeleteReason         *string    `json:"deleteReason"`
	FormKey              *string    `json:"formKey"`
	ParentTaskId         *string    `json:"parentTaskId"`
	ClaimTime            *time.Time `json:"claimTime"`
	TenantId             string     `json:"tenantId"`
}

// GetHistoricTasks handles GET /history/task
// Camunda compatible: /engine-rest/history/task
func (hc *HistoricTaskController) GetHistoricTasks(c *fiber.Ctx) error {
	var query HistoricTaskQuery
	if err := c.QueryParser(&query); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	repo := repository.NewHistoricTaskRepository(hc.db)

	params := repository.HistoricTaskQueryParams{
		TaskId:               query.TaskId,
		TaskName:             query.TaskName,
		TaskNameLike:         query.TaskNameLike,
		Assignee:             query.Assignee,
		AssigneeLike:         query.AssigneeLike,
		ProcessInstanceId:    query.ProcessInstanceId,
		ProcessDefinitionId:  query.ProcessDefinitionId,
		ProcessDefinitionKey: query.ProcessDefinitionKey,
		Status:               query.Status,
		Finished:             query.Finished,
		Unfinished:           query.Unfinished,
		SortBy:               query.SortBy,
		SortOrder:            query.SortOrder,
		FirstResult:          query.FirstResult,
		MaxResults:           query.MaxResults,
	}

	// Parse time filters
	if query.StartedBefore != "" {
		if t, err := time.Parse(time.RFC3339, query.StartedBefore); err == nil {
			params.StartedBefore = &t
		}
	}
	if query.StartedAfter != "" {
		if t, err := time.Parse(time.RFC3339, query.StartedAfter); err == nil {
			params.StartedAfter = &t
		}
	}
	if query.FinishedBefore != "" {
		if t, err := time.Parse(time.RFC3339, query.FinishedBefore); err == nil {
			params.FinishedBefore = &t
		}
	}
	if query.FinishedAfter != "" {
		if t, err := time.Parse(time.RFC3339, query.FinishedAfter); err == nil {
			params.FinishedAfter = &t
		}
	}

	tasks, err := repo.FindHistoricTasks(params)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	// Convert to Camunda response format
	response := make([]HistoricTaskResponse, len(tasks))
	for i, task := range tasks {
		var duration *int64
		if task.StartTime != nil && task.EndTime != nil {
			d := task.EndTime.Sub(*task.StartTime).Milliseconds()
			duration = &d
		}

		response[i] = HistoricTaskResponse{
			Id:                   task.ID.String(),
			ProcessInstanceId:    task.ProcessInstanceID.String(),
			ProcessDefinitionId:  task.ProcessDefinitionID.String(),
			ProcessDefinitionKey: task.ProcessDefinitionKey,
			TaskDefinitionKey:    task.TaskDefinitionKey,
			TaskName:             task.TaskName,
			Assignee:             task.Assignee,
			Status:               task.Status,
			StartTime:            task.StartTime,
			EndTime:              task.EndTime,
			DurationInMillis:     duration,
			ClaimTime:            task.ClaimTime,
			DeleteReason:         task.DeleteReason,
		}
	}

	return c.JSON(response)
}

// GetHistoricTaskCount handles GET /history/task/count
func (hc *HistoricTaskController) GetHistoricTaskCount(c *fiber.Ctx) error {
	var query HistoricTaskQuery
	if err := c.QueryParser(&query); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	repo := repository.NewHistoricTaskRepository(hc.db)

	params := repository.HistoricTaskQueryParams{
		TaskId:               query.TaskId,
		TaskName:             query.TaskName,
		TaskNameLike:         query.TaskNameLike,
		Assignee:             query.Assignee,
		AssigneeLike:         query.AssigneeLike,
		ProcessInstanceId:    query.ProcessInstanceId,
		ProcessDefinitionId:  query.ProcessDefinitionId,
		ProcessDefinitionKey: query.ProcessDefinitionKey,
		Status:               query.Status,
		Finished:             query.Finished,
		Unfinished:           query.Unfinished,
	}

	count, err := repo.CountHistoricTasks(params)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"count": count,
	})
}
