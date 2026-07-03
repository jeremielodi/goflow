// internal/api/historic_task_controller.go
package api

import (
	"sort"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type HistoricTaskController struct {
	db          *sqlx.DB
	archiveRepo *repository.HistoricArchiveRepository
}

func NewHistoricTaskController(db *sqlx.DB) *HistoricTaskController {
	return &HistoricTaskController{db: db, archiveRepo: repository.NewHistoricArchiveRepository(db)}
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

func (q HistoricTaskQuery) toParams(firstResult, maxResults int) repository.HistoricTaskQueryParams {
	params := repository.HistoricTaskQueryParams{
		TaskId:               q.TaskId,
		TaskName:             q.TaskName,
		TaskNameLike:         q.TaskNameLike,
		Assignee:             q.Assignee,
		AssigneeLike:         q.AssigneeLike,
		ProcessInstanceId:    q.ProcessInstanceId,
		ProcessDefinitionId:  q.ProcessDefinitionId,
		ProcessDefinitionKey: q.ProcessDefinitionKey,
		Status:               q.Status,
		Finished:             q.Finished,
		Unfinished:           q.Unfinished,
		SortBy:               q.SortBy,
		SortOrder:            q.SortOrder,
		FirstResult:          firstResult,
		MaxResults:           maxResults,
	}
	if t, err := time.Parse(time.RFC3339, q.StartedBefore); err == nil {
		params.StartedBefore = &t
	}
	if t, err := time.Parse(time.RFC3339, q.StartedAfter); err == nil {
		params.StartedAfter = &t
	}
	if t, err := time.Parse(time.RFC3339, q.FinishedBefore); err == nil {
		params.FinishedBefore = &t
	}
	if t, err := time.Parse(time.RFC3339, q.FinishedAfter); err == nil {
		params.FinishedAfter = &t
	}
	return params
}

func toHistoricTaskResponse(task repository.HistoricTask) HistoricTaskResponse {
	var duration *int64
	if task.StartTime != nil && task.EndTime != nil {
		d := task.EndTime.Sub(*task.StartTime).Milliseconds()
		duration = &d
	}
	return HistoricTaskResponse{
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

// findMergedHistoricTasks merges tasks belonging to still-live instances
// (repository.HistoricTaskRepository, backed by the live `tasks` table)
// with tasks belonging to already-archived instances
// (HistoricArchiveRepository, backed by `historic_tasks` — see
// archive_service.go). Pagination is applied in Go after merging, since a
// SQL UNION with correct LIMIT/OFFSET across two tables is meaningfully
// more complex than this is worth for a first pass.
func (hc *HistoricTaskController) findMergedHistoricTasks(query HistoricTaskQuery) ([]repository.HistoricTask, error) {
	liveRepo := repository.NewHistoricTaskRepository(hc.db)

	liveTasks, err := liveRepo.FindHistoricTasks(query.toParams(0, 0)) // unpaginated
	if err != nil {
		return nil, err
	}
	historicTasks, err := hc.archiveRepo.FindTasksForHistoricQuery(query.toParams(0, 0))
	if err != nil {
		return nil, err
	}

	merged := append(liveTasks, historicTasks...)
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].CreatedAt.After(merged[j].CreatedAt)
	})
	return merged, nil
}

// GetHistoricTasks handles GET /engine-rest/history/task
func (hc *HistoricTaskController) GetHistoricTasks(c *fiber.Ctx) error {
	var query HistoricTaskQuery
	if err := c.QueryParser(&query); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	merged, err := hc.findMergedHistoricTasks(query)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	firstResult := query.FirstResult
	maxResults := query.MaxResults
	if maxResults <= 0 {
		maxResults = 100
	}
	if firstResult >= len(merged) {
		return c.JSON([]HistoricTaskResponse{})
	}
	end := firstResult + maxResults
	if end > len(merged) {
		end = len(merged)
	}

	response := make([]HistoricTaskResponse, 0, end-firstResult)
	for _, task := range merged[firstResult:end] {
		response = append(response, toHistoricTaskResponse(task))
	}
	return c.JSON(response)
}

// GetHistoricTask handles GET /engine-rest/history/task/:id
func (hc *HistoricTaskController) GetHistoricTask(c *fiber.Ctx) error {
	taskID := c.Params("id")
	merged, err := hc.findMergedHistoricTasks(HistoricTaskQuery{TaskId: taskID})
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}
	if len(merged) == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"type":    "NotFound",
			"message": "historic task not found",
		})
	}
	return c.JSON(toHistoricTaskResponse(merged[0]))
}

// GetHistoricTaskCount handles GET /engine-rest/history/task/count
func (hc *HistoricTaskController) GetHistoricTaskCount(c *fiber.Ctx) error {
	var query HistoricTaskQuery
	if err := c.QueryParser(&query); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"type":    "InvalidRequest",
			"message": err.Error(),
		})
	}

	merged, err := hc.findMergedHistoricTasks(query)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"type":    "InternalError",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{"count": len(merged)})
}
