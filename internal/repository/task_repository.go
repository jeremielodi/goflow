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
func (r *TaskRepository) FindAll(params map[string]interface{}) ([]models.Task, error) {
	allowedColumns := map[string]bool{
		"id": true, "process_instance_id": true, "execution_id": true,
		"task_definition_key": true, "task_name": true, "assignee": true,
		"candidate_group": true, "status": true, "form_data": true,
		"created_at": true, "claimed_at": true, "completed_at": true,
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
	adapter := database.NewDabaseAdapter(r.db)
	now := time.Now()

	// Get task before update for event
	taskBefore, err := r.FindTaskByID(taskID)
	if err != nil {
		return nil, err
	}

	result, err := adapter.Update("public.tasks", []database.QueryParameter{
		{Key: "assignee", Value: assignee},
		{Key: "status", Value: "claimed"},
		{Key: "claimed_at", Value: now},
	}, database.QueryParameter{
		Key:   "id",
		Value: taskID,
	})

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
	adapter := database.NewDabaseAdapter(r.db)

	// Get task before update for event
	taskBefore, err := r.FindTaskByID(taskID)
	if err != nil {
		return nil, err
	}

	result, err := adapter.Update("public.tasks", []database.QueryParameter{
		{Key: "assignee", Value: nil},
		{Key: "status", Value: "created"},
		{Key: "claimed_at", Value: nil},
	}, database.QueryParameter{
		Key:   "id",
		Value: taskID,
	})

	if err != nil {
		return result, err
	}

	r.dispatchTaskEvent(taskID, &taskBefore, events.TaskUnclaimed, nil)

	return result, nil
}

// UpdateTaskFormData merges new form data into the existing JSONB.
func (r *TaskRepository) UpdateTaskFormData(taskID uuid.UUID, formData map[string]interface{}) (sql.Result, error) {
	newJSON, _ := json.Marshal(formData)
	_, err := r.db.Exec(`
		UPDATE public.tasks
		SET form_data = COALESCE(form_data, '{}'::jsonb) || $1::jsonb
		WHERE id = $2
	`, newJSON, taskID)
	if err != nil {
		return nil, err
	}
	return sql.Result(nil), nil
}

// UpdateTaskStatus updates task status.
func (r *TaskRepository) UpdateTaskStatus(taskID uuid.UUID, status string, claimedAt, completedAt *time.Time) (sql.Result, error) {
	adapter := database.NewDabaseAdapter(r.db)

	// Get task before update for event
	taskBefore, err := r.FindTaskByID(taskID)
	if err != nil {
		return nil, err
	}

	params := []database.QueryParameter{
		{Key: "status", Value: status},
	}
	if claimedAt != nil {
		params = append(params, database.QueryParameter{Key: "claimed_at", Value: *claimedAt})
	}
	if completedAt != nil {
		params = append(params, database.QueryParameter{Key: "completed_at", Value: *completedAt})
	}

	result, err := adapter.Update("public.tasks", params, database.QueryParameter{Key: "id", Value: taskID})

	if err != nil {
		return result, err
	}

	// Determine event type based on new status
	var eventType events.TaskEventType
	switch status {
	case "completed":
		eventType = events.TaskCompleted
	case "cancelled":
		eventType = events.TaskCancelled
	case "failed":
		eventType = events.TaskFailed
	default:
		// Don't dispatch for other status changes
		return result, nil
	}

	r.dispatchTaskEvent(taskID, &taskBefore, eventType, map[string]interface{}{
		"new_status": status,
	})

	return result, nil
}

// CompleteTask marks a task as completed.
func (r *TaskRepository) CompleteTask(taskID uuid.UUID) error {
	// Get task before update for event
	taskBefore, err := r.FindTaskByID(taskID)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(`
		UPDATE public.tasks
		SET status = 'completed', completed_at = $1, updated_at = $1
		WHERE id = $2
	`, time.Now(), taskID)

	if err != nil {
		return err
	}

	// Dispatch TaskCompleted event
	r.dispatchTaskEvent(taskID, &taskBefore, events.TaskCompleted, nil)

	return nil
}

// FailTask marks a task as failed.
func (r *TaskRepository) FailTask(taskID uuid.UUID, reason string) error {
	// Get task before update for event
	taskBefore, err := r.FindTaskByID(taskID)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(`
		UPDATE public.tasks
		SET status = 'failed', updated_at = $1
		WHERE id = $2
	`, time.Now(), taskID)

	if err != nil {
		return err
	}

	// Dispatch TaskFailed event
	r.dispatchTaskEvent(taskID, &taskBefore, events.TaskFailed, map[string]interface{}{
		"reason": reason,
	})

	return nil
}

// ============================================================
// DELETE
// ============================================================

// DeleteTask permanently removes a task (e.g., for cancellation).
func (r *TaskRepository) DeleteTask(taskID uuid.UUID) (sql.Result, error) {
	// Get task before delete for event
	taskBefore, err := r.FindTaskByID(taskID)
	if err != nil {
		return nil, err
	}

	result, err := r.db.Exec(`DELETE FROM public.tasks WHERE id = $1`, taskID)

	if err != nil {
		return result, err
	}

	// Dispatch TaskCancelled event
	r.dispatchTaskEvent(taskID, &taskBefore, events.TaskCancelled, map[string]interface{}{
		"reason": "deleted",
	})

	return result, nil
}

// ============================================================
// HELPERS
// ============================================================

// GetProcessInstanceID returns the process instance ID for a task.
func (r *TaskRepository) GetProcessInstanceID(taskID uuid.UUID) (uuid.UUID, error) {
	var instanceID uuid.UUID
	err := r.db.Get(&instanceID, `
        SELECT process_instance_id FROM public.tasks WHERE id = $1
    `, taskID)
	return instanceID, err
}

// GetExecutionID returns the execution ID for a task.
func (r *TaskRepository) GetExecutionID(taskID uuid.UUID) (uuid.UUID, error) {
	var execID uuid.UUID
	err := r.db.Get(&execID, `
        SELECT execution_id FROM public.tasks WHERE id = $1
    `, taskID)
	return execID, err
}

// UpdateTaskStatusTx updates task status inside an existing transaction.
func (r *TaskRepository) UpdateTaskStatusTx(tx *sqlx.Tx, taskID uuid.UUID, status string, claimedAt, completedAt *time.Time) (sql.Result, error) {
	query := `UPDATE public.tasks SET status = $1`
	args := []interface{}{status}
	if claimedAt != nil {
		query += `, claimed_at = $2`
		args = append(args, *claimedAt)
	}
	if completedAt != nil {
		query += `, completed_at = $3`
		args = append(args, *completedAt)
	}
	query += ` WHERE id = $4`
	args = append(args, taskID)
	return tx.Exec(query, args...)
}

// CompleteTaskTx completes a task within a transaction.
func (r *TaskRepository) CompleteTaskTx(tx *sqlx.Tx, taskID uuid.UUID) error {
	_, err := tx.Exec(`
		UPDATE public.tasks
		SET status = 'completed', completed_at = $1, updated_at = $1
		WHERE id = $2
	`, time.Now(), taskID)
	return err
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
