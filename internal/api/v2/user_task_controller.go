package v2

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type UserTaskController struct {
	db         *sqlx.DB
	dispatcher *events.TaskEventDispatcher
	taskRepo   *repository.TaskRepository
	engineRepo *repository.EngineRepository
}

func NewUserTaskController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *UserTaskController {
	return &UserTaskController{
		db:         db,
		dispatcher: dispatcher,
		taskRepo:   repository.NewTaskRepository(db, dispatcher),
		engineRepo: repository.NewEngineRepository(db, dispatcher),
	}
}

// userTaskState maps GoFlow's task status to the state names used by the
// Camunda 8 Tasklist API.
func userTaskState(status string) string {
	switch status {
	case "created":
		return "CREATED"
	case "claimed":
		return "ASSIGNED"
	case "completed":
		return "COMPLETED"
	case "cancelled":
		return "CANCELED"
	default:
		return status
	}
}

func (uc *UserTaskController) toV2UserTask(t *models.Task) fiber.Map {
	var formKey string
	if graph, err := uc.engineRepo.GetProcessGraphByInstanceID(t.ProcessInstanceID); err == nil && graph != nil {
		if n, ok := graph.Nodes[t.TaskDefinitionKey]; ok {
			formKey = n.FormKey
		}
	}

	return fiber.Map{
		"userTaskKey":        t.ID,
		"elementId":          t.TaskDefinitionKey,
		"name":               t.TaskName,
		"processInstanceKey": t.ProcessInstanceID,
		"assignee":           t.Assignee,
		"candidateGroups":    t.CandidateGroup,
		"state":              userTaskState(t.Status),
		"creationDate":       t.CreatedAt,
		"formKey":            formKey,
		"priority":           t.Priority,
	}
}

// SearchUserTasksRequest matches the (simplified) Camunda 8 search body.
type SearchUserTasksRequest struct {
	Filter struct {
		ProcessInstanceKey string `json:"processInstanceKey"`
		Assignee           string `json:"assignee"`
		State              string `json:"state"`
		ElementId          string `json:"elementId"`
		CandidateGroup     string `json:"candidateGroup"`
		Priority           *int   `json:"priority"`
	} `json:"filter"`
}

// SearchUserTasks handles POST /v2/user-tasks/search.
func (uc *UserTaskController) SearchUserTasks(c *fiber.Ctx) error {
	var req SearchUserTasksRequest
	_ = c.BodyParser(&req)

	params := map[string]interface{}{}
	if req.Filter.ProcessInstanceKey != "" {
		params["process_instance_id"] = req.Filter.ProcessInstanceKey
	}
	if req.Filter.Assignee != "" {
		params["assignee"] = req.Filter.Assignee
	}
	if req.Filter.ElementId != "" {
		params["task_definition_key"] = req.Filter.ElementId
	}
	if req.Filter.CandidateGroup != "" {
		params["candidate_group"] = req.Filter.CandidateGroup
	}
	if req.Filter.Priority != nil {
		params["priority"] = *req.Filter.Priority
	}
	if req.Filter.State != "" {
		switch req.Filter.State {
		case "CREATED":
			params["status"] = "created"
		case "ASSIGNED":
			params["status"] = "claimed"
		case "COMPLETED":
			params["status"] = "completed"
		case "CANCELED":
			params["status"] = "cancelled"
		}
	}

	tasks, err := uc.taskRepo.FindAll(params)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	items := make([]fiber.Map, 0, len(tasks))
	for i := range tasks {
		items = append(items, uc.toV2UserTask(&tasks[i]))
	}

	// The live `tasks` table only ever holds tasks of still-running
	// instances now — once an instance completes/terminates it's archived
	// (see archive_service.go) and its tasks move to historic_tasks. Merge
	// those in when a specific instance was searched, so a task search
	// right after that instance finishes still finds it (candidateGroup
	// isn't stored on historic_tasks, so skip the merge for that filter —
	// a known, narrow limitation rather than returning wrong results).
	if req.Filter.ProcessInstanceKey != "" && req.Filter.CandidateGroup == "" {
		if instanceID, parseErr := uuid.Parse(req.Filter.ProcessInstanceKey); parseErr == nil {
			archiveRepo := repository.NewHistoricArchiveRepository(uc.db)
			historicTasks, histErr := archiveRepo.FindTasksByProcessInstance(instanceID)
			if histErr == nil {
				for _, ht := range historicTasks {
					if req.Filter.ElementId != "" && ht.TaskDefinitionKey != req.Filter.ElementId {
						continue
					}
					if req.Filter.Assignee != "" && (ht.Assignee == nil || *ht.Assignee != req.Filter.Assignee) {
						continue
					}
					if req.Filter.Priority != nil && (ht.Priority == nil || *ht.Priority != *req.Filter.Priority) {
						continue
					}
					if params["status"] != nil && ht.Status != params["status"] {
						continue
					}
					items = append(items, uc.toV2UserTask(&models.Task{
						ID:                ht.ID,
						ProcessInstanceID: ht.ProcessInstanceID,
						TaskDefinitionKey: ht.TaskDefinitionKey,
						TaskName:          derefOrEmpty(ht.TaskName),
						Assignee:          ht.Assignee,
						Status:            ht.Status,
						Priority:          derefIntOr(ht.Priority, 0),
						CreatedAt:         ht.CreatedAt,
						ClaimedAt:         ht.ClaimedAt,
						CompletedAt:       ht.CompletedAt,
					}))
				}
			}
		}
	}

	return c.JSON(fiber.Map{"items": items})
}

func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefIntOr(i *int, def int) int {
	if i == nil {
		return def
	}
	return *i
}
