// internal/repository/task_repository.go
package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

type TaskRepository struct {
	db         *sqlx.DB
	dispatcher *events.TaskEventDispatcher
}

func NewTaskRepository(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *TaskRepository {
	return &TaskRepository{db: db, dispatcher: dispatcher}
}

// ============================================================
// CREATE
// ============================================================

// CreateTask inserts a new user task record.
// Expects a models.Task (with all fields except auto‑generated timestamps).
func (r *TaskRepository) CreateTask(task models.Task) (sql.Result, error) {
	adapter := database.NewDabaseAdapter(r.db)
	now := time.Now()
	id := uuid.New()

	// Convert form_data (if any) to JSONB
	var formDataJSON []byte
	if task.FormData != nil {
		formDataJSON, _ = json.Marshal(task.FormData)
	}

	// Set task ID for event
	task.ID = id
	task.Status = "created"
	task.CreatedAt = now

	result, err := adapter.Insert("public.tasks", []database.QueryParameter{
		{Key: "id", Value: id},
		{Key: "process_instance_id", Value: task.ProcessInstanceID},
		{Key: "execution_id", Value: task.ExecutionID},
		{Key: "task_definition_key", Value: task.TaskDefinitionKey},
		{Key: "task_name", Value: task.TaskName},
		{Key: "assignee", Value: task.Assignee},
		{Key: "candidate_group", Value: task.CandidateGroup},
		{Key: "status", Value: task.Status},
		{Key: "form_data", Value: formDataJSON},
		{Key: "created_at", Value: now},
		{Key: "claimed_at", Value: task.ClaimedAt},
		{Key: "completed_at", Value: task.CompletedAt},
	})

	if err != nil {
		return result, err
	}

	// Dispatch TaskCreated event
	r.dispatchTaskEvent(id, &task, events.TaskCreated, nil)

	return result, nil
}

// ============================================================
// FIND / QUERY
// ============================================================

// FindTaskByID returns a single task by its UUID.
func (r *TaskRepository) FindTaskByID(id uuid.UUID) (models.Task, error) {
	var task models.Task
	err := r.db.Get(&task, `
		SELECT
			id,
			process_instance_id,
			execution_id,
			task_definition_key,
			task_name,
			assignee,
			candidate_group,
			status,
			priority,
			form_data,
			created_at,
			claimed_at,
			completed_at
		FROM public.tasks
		WHERE id = $1
	`, id)
	return task, err
}

// FindTasksByAssignee returns all open tasks assigned to a specific user.
func (r *TaskRepository) FindTasksByAssignee(assignee string, status ...string) ([]models.Task, error) {
	var tasks []models.Task
	query := `
		SELECT
			id,
			process_instance_id,
			execution_id,
			task_definition_key,
			task_name,
			assignee,
			candidate_group,
			status,
			priority,
			form_data,
			created_at,
			claimed_at,
			completed_at
		FROM public.tasks
		WHERE assignee = $1
	`
	args := []interface{}{assignee}
	if len(status) > 0 && status[0] != "" {
		query += " AND status = $2"
		args = append(args, status[0])
	}
	query += " ORDER BY created_at ASC"
	err := r.db.Select(&tasks, query, args...)
	return tasks, err
}

// FindAll returns tasks filtered by any column given in the params map.
// excludeStatuses, if given, are omitted from the result — but only when the
// caller didn't already filter explicitly by "status", so an explicit
// ?status=completed request still works.
func (r *TaskRepository) FindAll(params map[string]interface{}, excludeStatuses ...string) ([]models.Task, error) {
	allowedColumns := map[string]bool{
		"id": true, "process_instance_id": true, "execution_id": true,
		"task_definition_key": true, "task_name": true, "assignee": true,
		"candidate_group": true, "status": true, "form_data": true,
		"priority": true, "created_at": true, "claimed_at": true, "completed_at": true,
	}

	query := `
        SELECT
            id,
            process_instance_id,
            execution_id,
            task_definition_key,
            task_name,
            assignee,
            candidate_group,
            status,
            priority,
            form_data,
            created_at,
            claimed_at,
            completed_at
        FROM public.tasks
    `
	var conditions []string
	var args []interface{}
	i := 1
	for key, value := range params {
		if allowedColumns[key] {
			conditions = append(conditions, fmt.Sprintf("%s = $%d", key, i))
			args = append(args, value)
			i++
		}
	}
	if _, hasStatus := params["status"]; !hasStatus && len(excludeStatuses) > 0 {
		placeholders := make([]string, len(excludeStatuses))
		for j, s := range excludeStatuses {
			placeholders[j] = fmt.Sprintf("$%d", i)
			args = append(args, s)
			i++
		}
		conditions = append(conditions, fmt.Sprintf("status NOT IN (%s)", strings.Join(placeholders, ", ")))
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY created_at ASC"

	var tasks []models.Task
	err := r.db.Select(&tasks, query, args...)
	return tasks, err
}

// FindTasksByCandidateGroup returns all open tasks for a given group.
func (r *TaskRepository) FindTasksByCandidateGroup(group string, status ...string) ([]models.Task, error) {
	var tasks []models.Task
	query := `
		SELECT
			id,
			process_instance_id,
			execution_id,
			task_definition_key,
			task_name,
			assignee,
			candidate_group,
			status,
			priority,
			form_data,
			created_at,
			claimed_at,
			completed_at
		FROM public.tasks
		WHERE candidate_group = $1
	`
	args := []interface{}{group}
	if len(status) > 0 && status[0] != "" {
		query += " AND status = $2"
		args = append(args, status[0])
	}
	query += " ORDER BY created_at ASC"
	err := r.db.Select(&tasks, query, args...)
	return tasks, err
}

// FindTasksByProcessInstance returns all tasks belonging to a process instance.
func (r *TaskRepository) FindTasksByProcessInstance(instanceID uuid.UUID) ([]models.Task, error) {
	var tasks []models.Task
	err := r.db.Select(&tasks, `
		SELECT
			id,
			process_instance_id,
			execution_id,
			task_definition_key,
			task_name,
			assignee,
			candidate_group,
			status,
			priority,
			form_data,
			created_at,
			claimed_at,
			completed_at
		FROM public.tasks
		WHERE process_instance_id = $1
		ORDER BY created_at DESC
	`, instanceID)
	return tasks, err
}

// ============================================================
// UPDATE / STATE CHANGE
// ============================================================

// ClaimTask assigns a task to a user and updates status to 'claimed'.
func (r *TaskRepository) ClaimTask(taskID uuid.UUID, assignee string) (sql.Result, error) {
	now := time.Now()

	// Get task before update for event
	taskBefore, err := r.FindTaskByID(taskID)
	if err != nil {
		return nil, err
	}

	// Direct SQL rather than the DataBaseAdapter.Update helper (which only
	// supports a single WHERE condition) — process_instance_id is needed
	// alongside id for single-shard Citus routing.
	result, err := r.db.Exec(`
		UPDATE public.tasks
		SET assignee = $1, status = 'claimed', claimed_at = $2
		WHERE process_instance_id = $3 AND id = $4
	`, assignee, now, taskBefore.ProcessInstanceID, taskID)

	if err != nil {
		return result, err
	}

	// Dispatch TaskClaimed event
	r.dispatchTaskEvent(taskID, &taskBefore, events.TaskClaimed, map[string]interface{}{
		"assignee": assignee,
	})

	return result, nil
}

// UnClaimTask removes assignee from a task and resets status to 'created'.
func (r *TaskRepository) UnClaimTask(taskID uuid.UUID) (sql.Result, error) {
	// Get task before update for event
	taskBefore, err := r.FindTaskByID(taskID)
	if err != nil {
		return nil, err
	}

	// Direct SQL rather than the DataBaseAdapter.Update helper (which only
	// supports a single WHERE condition) — process_instance_id is needed
	// alongside id for single-shard Citus routing.
	result, err := r.db.Exec(`
		UPDATE public.tasks
		SET assignee = NULL, status = 'created', claimed_at = NULL
		WHERE process_instance_id = $1 AND id = $2
	`, taskBefore.ProcessInstanceID, taskID)

	if err != nil {
		return result, err
	}

	r.dispatchTaskEvent(taskID, &taskBefore, events.TaskUnclaimed, nil)

	return result, nil
}

// UpdateTaskFormData merges new form data into the existing JSONB.
func (r *TaskRepository) UpdateTaskFormData(processInstanceID, taskID uuid.UUID, formData map[string]interface{}) (sql.Result, error) {
	newJSON, _ := json.Marshal(formData)
	_, err := r.db.Exec(`
		UPDATE public.tasks
		SET form_data = COALESCE(form_data, '{}'::jsonb) || $1::jsonb
		WHERE process_instance_id = $2 AND id = $3
	`, newJSON, processInstanceID, taskID)
	if err != nil {
		return nil, err
	}
	return sql.Result(nil), nil
}

// ============================================================
// HELPERS
// ============================================================

// CancelOpenTasksByProcessInstanceTx cancels every open (created/claimed)
// task belonging to a process instance — used when the whole instance is
// terminated, so no task is left dangling in an "open" state for a process
// that no longer runs. Returns the cancelled task IDs so the caller can
// audit-log each one.
func (r *TaskRepository) CancelOpenTasksByProcessInstanceTx(tx *sqlx.Tx, instanceID uuid.UUID) ([]uuid.UUID, error) {
	var cancelledTaskIDs []uuid.UUID
	err := tx.Select(&cancelledTaskIDs, `
		UPDATE public.tasks SET status = 'cancelled', updated_at = NOW()
		WHERE process_instance_id = $1 AND status IN ('created', 'claimed')
		RETURNING id
	`, instanceID)
	return cancelledTaskIDs, err
}

// ============================================================
// EVENT DISPATCHING
// ============================================================

// dispatchTaskEvent dispatches a task event to all registered listeners
func (r *TaskRepository) dispatchTaskEvent(taskID uuid.UUID, task *models.Task, eventType events.TaskEventType, additionalData map[string]interface{}) {
	if r.dispatcher == nil {
		return
	}

	// Get process key from process definition
	processKey := ""
	if task.ProcessInstanceID != uuid.Nil {
		engineRepo := NewEngineRepository(r.db, r.dispatcher)
		procDef, err := engineRepo.GetProcessDefinitionByInstanceID(task.ProcessInstanceID)
		if err == nil && procDef != nil {
			processKey = procDef.ProcessKey
		}
	}

	// Get the new status based on event type
	newStatus := r.getStatusForEventType(eventType)

	event := &events.TaskEvent{
		ID:                uuid.New().String(),
		EventType:         eventType,
		TaskID:            taskID.String(),
		ProcessInstanceID: task.ProcessInstanceID.String(),
		ExecutionID:       task.ExecutionID.String(),
		TaskName:          task.TaskName,
		TaskDefinitionKey: task.TaskDefinitionKey,
		Assignee:          task.Assignee,
		CandidateGroup:    task.CandidateGroup,
		OldStatus:         &task.Status,
		NewStatus:         newStatus,
		Timestamp:         time.Now(),
		Variables:         additionalData,
		ProcessKey:        processKey,
	}

	// Add assignee to event if present in additional data
	if assignee, ok := additionalData["assignee"].(string); ok && assignee != "" {
		event.Assignee = &assignee
	}

	// Add reason if present
	if reason, ok := additionalData["reason"].(string); ok {
		event.Comment = reason
	}

	// Dispatch in goroutine to not block
	go r.dispatcher.Dispatch(context.Background(), event)
}

// getStatusForEventType returns the status string for an event type
func (r *TaskRepository) getStatusForEventType(eventType events.TaskEventType) string {
	switch eventType {
	case events.TaskCreated:
		return "created"
	case events.TaskClaimed:
		return "claimed"
	case events.TaskUnclaimed:
		return "created"
	case events.TaskCompleted:
		return "completed"
	case events.TaskFailed:
		return "failed"
	case events.TaskCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}
