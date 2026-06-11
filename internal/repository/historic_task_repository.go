// internal/repository/historic_task_repository.go
package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type HistoricTask struct {
	ID                   uuid.UUID  `db:"id"`
	ProcessInstanceID    uuid.UUID  `db:"process_instance_id"`
	ProcessDefinitionID  uuid.UUID  `db:"process_definition_id"`
	ProcessDefinitionKey string     `db:"process_definition_key"`
	TaskDefinitionKey    string     `db:"task_definition_key"`
	TaskName             string     `db:"task_name"`
	Assignee             *string    `db:"assignee"`
	Status               string     `db:"status"`
	StartTime            *time.Time `db:"start_time"`
	EndTime              *time.Time `db:"end_time"`
	ClaimTime            *time.Time `db:"claim_time"`
	DeleteReason         *string    `db:"delete_reason"`
	CreatedAt            time.Time  `db:"created_at"`
	UpdatedAt            *time.Time `db:"updated_at"`
}

type HistoricTaskQueryParams struct {
	TaskId               string
	TaskName             string
	TaskNameLike         string
	Assignee             *string
	AssigneeLike         string
	ProcessInstanceId    string
	ProcessDefinitionId  string
	ProcessDefinitionKey string
	Status               string
	Finished             bool
	Unfinished           bool
	StartedBefore        *time.Time
	StartedAfter         *time.Time
	FinishedBefore       *time.Time
	FinishedAfter        *time.Time
	SortBy               string
	SortOrder            string
	FirstResult          int
	MaxResults           int
}

type HistoricTaskRepository struct {
	db *sqlx.DB
}

func NewHistoricTaskRepository(db *sqlx.DB) *HistoricTaskRepository {
	return &HistoricTaskRepository{db: db}
}

// FindHistoricTasks retrieves historic tasks based on query parameters
func (r *HistoricTaskRepository) FindHistoricTasks(params HistoricTaskQueryParams) ([]HistoricTask, error) {
	query := `
		SELECT 
			t.id,
			t.process_instance_id,
			pd.id as process_definition_id,
			pd.process_key as process_definition_key,
			t.task_definition_key,
			t.task_name,
			t.assignee,
			t.status,
			t.created_at as start_time,
			t.completed_at as end_time,
			t.claimed_at as claim_time,
			'' as delete_reason,
			t.created_at,
			t.updated_at
		FROM public.tasks t
		LEFT JOIN public.process_instances pi ON pi.id = t.process_instance_id
		LEFT JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE 1=1
	`

	var conditions []string
	var args []interface{}
	argIndex := 1

	// Add filters
	if params.TaskId != "" {
		conditions = append(conditions, fmt.Sprintf("t.id = $%d", argIndex))
		args = append(args, params.TaskId)
		argIndex++
	}

	if params.TaskName != "" {
		conditions = append(conditions, fmt.Sprintf("t.task_name = $%d", argIndex))
		args = append(args, params.TaskName)
		argIndex++
	}

	if params.TaskNameLike != "" {
		conditions = append(conditions, fmt.Sprintf("t.task_name ILIKE $%d", argIndex))
		args = append(args, "%"+params.TaskNameLike+"%")
		argIndex++
	}

	if params.Assignee != nil {
		conditions = append(conditions, fmt.Sprintf("t.assignee = $%d", argIndex))
		args = append(args, params.Assignee)
		argIndex++
	}

	if params.AssigneeLike != "" {
		conditions = append(conditions, fmt.Sprintf("t.assignee ILIKE $%d", argIndex))
		args = append(args, "%"+params.AssigneeLike+"%")
		argIndex++
	}

	if params.ProcessInstanceId != "" {
		conditions = append(conditions, fmt.Sprintf("t.process_instance_id = $%d", argIndex))
		args = append(args, params.ProcessInstanceId)
		argIndex++
	}

	if params.ProcessDefinitionId != "" {
		conditions = append(conditions, fmt.Sprintf("pd.id = $%d", argIndex))
		args = append(args, params.ProcessDefinitionId)
		argIndex++
	}

	if params.ProcessDefinitionKey != "" {
		conditions = append(conditions, fmt.Sprintf("pd.process_key = $%d", argIndex))
		args = append(args, params.ProcessDefinitionKey)
		argIndex++
	}

	if params.Status != "" {
		conditions = append(conditions, fmt.Sprintf("t.status = $%d", argIndex))
		args = append(args, params.Status)
		argIndex++
	}

	// Finished/Unfinished filters
	if params.Finished && !params.Unfinished {
		conditions = append(conditions, "t.completed_at IS NOT NULL")
	}
	if params.Unfinished && !params.Finished {
		conditions = append(conditions, "t.completed_at IS NULL")
	}

	// Time filters
	if params.StartedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("t.created_at <= $%d", argIndex))
		args = append(args, *params.StartedBefore)
		argIndex++
	}

	if params.StartedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("t.created_at >= $%d", argIndex))
		args = append(args, *params.StartedAfter)
		argIndex++
	}

	if params.FinishedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("t.completed_at <= $%d", argIndex))
		args = append(args, *params.FinishedBefore)
		argIndex++
	}

	if params.FinishedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("t.completed_at >= $%d", argIndex))
		args = append(args, *params.FinishedAfter)
		argIndex++
	}

	// Build WHERE clause
	if len(conditions) > 0 {
		query += " AND " + strings.Join(conditions, " AND ")
	}

	// Add sorting
	if params.SortBy != "" {
		validSortColumns := map[string]bool{
			"id": true, "task_name": true, "assignee": true, "status": true,
			"created_at": true, "completed_at": true, "claimed_at": true,
		}
		if validSortColumns[params.SortBy] {
			sortOrder := "ASC"
			if params.SortOrder == "desc" {
				sortOrder = "DESC"
			}
			query += fmt.Sprintf(" ORDER BY %s %s", params.SortBy, sortOrder)
		}
	} else {
		query += " ORDER BY t.created_at DESC"
	}

	// Add pagination
	if params.MaxResults > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, params.MaxResults)
		argIndex++
	}
	if params.FirstResult > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIndex)
		args = append(args, params.FirstResult)
	}

	var tasks []HistoricTask
	err := r.db.Select(&tasks, query, args...)
	return tasks, err
}

// CountHistoricTasks counts historic tasks based on query parameters
func (r *HistoricTaskRepository) CountHistoricTasks(params HistoricTaskQueryParams) (int, error) {
	query := `
		SELECT COUNT(*)
		FROM public.tasks t
		LEFT JOIN public.process_instances pi ON pi.id = t.process_instance_id
		LEFT JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE 1=1
	`

	var conditions []string
	var args []interface{}
	argIndex := 1

	// Add filters (same as FindHistoricTasks)
	if params.TaskId != "" {
		conditions = append(conditions, fmt.Sprintf("t.id = $%d", argIndex))
		args = append(args, params.TaskId)
		argIndex++
	}
	if params.TaskName != "" {
		conditions = append(conditions, fmt.Sprintf("t.task_name = $%d", argIndex))
		args = append(args, params.TaskName)
		argIndex++
	}
	if params.TaskNameLike != "" {
		conditions = append(conditions, fmt.Sprintf("t.task_name ILIKE $%d", argIndex))
		args = append(args, "%"+params.TaskNameLike+"%")
		argIndex++
	}
	if params.Assignee != nil {
		conditions = append(conditions, fmt.Sprintf("t.assignee = $%d", argIndex))
		args = append(args, params.Assignee)
		argIndex++
	}
	if params.AssigneeLike != "" {
		conditions = append(conditions, fmt.Sprintf("t.assignee ILIKE $%d", argIndex))
		args = append(args, "%"+params.AssigneeLike+"%")
		argIndex++
	}
	if params.ProcessInstanceId != "" {
		conditions = append(conditions, fmt.Sprintf("t.process_instance_id = $%d", argIndex))
		args = append(args, params.ProcessInstanceId)
		argIndex++
	}
	if params.ProcessDefinitionId != "" {
		conditions = append(conditions, fmt.Sprintf("pd.id = $%d", argIndex))
		args = append(args, params.ProcessDefinitionId)
		argIndex++
	}
	if params.ProcessDefinitionKey != "" {
		conditions = append(conditions, fmt.Sprintf("pd.process_key = $%d", argIndex))
		args = append(args, params.ProcessDefinitionKey)
		argIndex++
	}
	if params.Status != "" {
		conditions = append(conditions, fmt.Sprintf("t.status = $%d", argIndex))
		args = append(args, params.Status)
		argIndex++
	}
	if params.Finished && !params.Unfinished {
		conditions = append(conditions, "t.completed_at IS NOT NULL")
	}
	if params.Unfinished && !params.Finished {
		conditions = append(conditions, "t.completed_at IS NULL")
	}
	if params.StartedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("t.created_at <= $%d", argIndex))
		args = append(args, *params.StartedBefore)
		argIndex++
	}
	if params.StartedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("t.created_at >= $%d", argIndex))
		args = append(args, *params.StartedAfter)
		argIndex++
	}
	if params.FinishedBefore != nil {
		conditions = append(conditions, fmt.Sprintf("t.completed_at <= $%d", argIndex))
		args = append(args, *params.FinishedBefore)
		argIndex++
	}
	if params.FinishedAfter != nil {
		conditions = append(conditions, fmt.Sprintf("t.completed_at >= $%d", argIndex))
		args = append(args, *params.FinishedAfter)
		argIndex++
	}

	if len(conditions) > 0 {
		query += " AND " + strings.Join(conditions, " AND ")
	}

	var count int
	err := r.db.Get(&count, query, args...)
	return count, err
}
