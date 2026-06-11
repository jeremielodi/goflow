// internal/repository/task_repository.go
package repository

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

type TaskRepository struct {
	db *sqlx.DB
}

func NewTaskRepository(db *sqlx.DB) *TaskRepository {
	return &TaskRepository{db: db}
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

	return adapter.Insert("public.tasks", []database.QueryParameter{
		{Key: "id", Value: id},
		{Key: "process_instance_id", Value: task.ProcessInstanceID},
		{Key: "execution_id", Value: task.ExecutionID},
		{Key: "task_definition_key", Value: task.TaskDefinitionKey},
		{Key: "task_name", Value: task.TaskName},
		{Key: "assignee", Value: task.Assignee},
		{Key: "candidate_group", Value: task.CandidateGroup},
		{Key: "status", Value: task.Status}, // "created", "claimed", "completed"
		{Key: "form_data", Value: formDataJSON},
		{Key: "created_at", Value: now},
		{Key: "claimed_at", Value: task.ClaimedAt}, // can be nil
		{Key: "completed_at", Value: task.CompletedAt},
	})
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
// Example: params := map[string]interface{}{"assignee": "john", "status": "created"}
func (r *TaskRepository) FindAll(params map[string]interface{}) ([]models.Task, error) {
	// Allowed column names for filtering (prevent SQL injection)
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
	return adapter.Update("public.tasks", []database.QueryParameter{
		{Key: "assignee", Value: assignee},
		{Key: "status", Value: "claimed"},
		{Key: "claimed_at", Value: now},
	}, database.QueryParameter{
		Key:   "id",
		Value: taskID,
	})
}

// ClaimTask assigns a task to a user and updates status to 'claimed'.
func (r *TaskRepository) UnClaimTask(taskID uuid.UUID) (sql.Result, error) {
	adapter := database.NewDabaseAdapter(r.db)
	return adapter.Update("public.tasks", []database.QueryParameter{
		{Key: "assignee", Value: nil},
		{Key: "status", Value: "created"},
		{Key: "claimed_at", Value: nil},
	}, database.QueryParameter{
		Key:   "id",
		Value: taskID,
	})
}

// UpdateTaskFormData merges new form data into the existing JSONB.
func (r *TaskRepository) UpdateTaskFormData(taskID uuid.UUID, formData map[string]interface{}) (sql.Result, error) {
	// Use a direct Exec because the adapter may not support JSONB merging easily.
	newJSON, _ := json.Marshal(formData)
	// PostgreSQL JSONB concatenation: data || '{"new":"value"}'::jsonb
	_, err := r.db.Exec(`
		UPDATE public.tasks
		SET form_data = COALESCE(form_data, '{}'::jsonb) || $1::jsonb
		WHERE id = $2
	`, newJSON, taskID)
	if err != nil {
		return nil, err
	}
	return sql.Result(nil), nil // placeholder, but the method signature expects sql.Result
}

// ============================================================
// DELETE
// ============================================================

// DeleteTask permanently removes a task (e.g., for cancellation).
func (r *TaskRepository) DeleteTask(taskID uuid.UUID) (sql.Result, error) {
	return r.db.Exec(`DELETE FROM public.tasks WHERE id = $1`, taskID)
}

func (r *TaskRepository) UpdateTaskStatus(taskID uuid.UUID, status string, claimedAt, completedAt *time.Time) (sql.Result, error) {
	adapter := database.NewDabaseAdapter(r.db)
	params := []database.QueryParameter{
		{Key: "status", Value: status},
	}
	if claimedAt != nil {
		params = append(params, database.QueryParameter{Key: "claimed_at", Value: *claimedAt})
	}
	if completedAt != nil {
		params = append(params, database.QueryParameter{Key: "completed_at", Value: *completedAt})
	}
	return adapter.Update("public.tasks", params, database.QueryParameter{Key: "id", Value: taskID})
}

func (r *TaskRepository) GetProcessInstanceID(taskID uuid.UUID) (uuid.UUID, error) {
	var instanceID uuid.UUID
	err := r.db.Get(&instanceID, `
        SELECT process_instance_id FROM public.tasks WHERE id = $1
    `, taskID)
	return instanceID, err
}

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
func (r *TaskRepository) CompleteTask(taskID uuid.UUID) error {
	_, err := r.db.Exec(`
		UPDATE public.tasks
		SET status = 'completed', completed_at = $1
		WHERE id = $2
	`, time.Now(), taskID)
	return err
}
