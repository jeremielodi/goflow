// repository/audit_repository.go
package repository

import (
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

type AuditRepository struct {
	db *sqlx.DB
}

func NewAuditRepository(db *sqlx.DB) *AuditRepository {
	return &AuditRepository{db: db}
}

// CreateAuditLog creates a single audit log entry
func (r *AuditRepository) CreateAuditLog(log *models.AuditLog) error {
	adapter := database.NewDabaseAdapter(r.db)

	params := []database.QueryParameter{
		{Key: "id", Value: log.ID},
		{Key: "action", Value: string(log.Action)},
		{Key: "created_at", Value: log.CreatedAt},
	}

	if log.ProcessInstanceID != nil {
		params = append(params, database.QueryParameter{Key: "process_instance_id", Value: *log.ProcessInstanceID})
	}
	if log.TaskID != nil {
		params = append(params, database.QueryParameter{Key: "task_id", Value: *log.TaskID})
	}
	if log.UserID != nil {
		params = append(params, database.QueryParameter{Key: "user_id", Value: *log.UserID})
	}
	if log.OldData != nil {
		params = append(params, database.QueryParameter{Key: "old_data", Value: log.OldData})
	}
	if log.NewData != nil {
		params = append(params, database.QueryParameter{Key: "new_data", Value: log.NewData})
	}

	_, err := adapter.Insert("public.audit_logs", params)
	return err
}

// CreateAuditLogTx creates a single audit log entry within a transaction
func (r *AuditRepository) CreateAuditLogTx(tx *sqlx.Tx, log *models.AuditLog) error {
	query := `
		INSERT INTO public.audit_logs 
		(id, process_instance_id, task_id, user_id, action, old_data, new_data, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err := tx.Exec(query,
		log.ID,
		log.ProcessInstanceID,
		log.TaskID,
		log.UserID,
		string(log.Action),
		log.OldData,
		log.NewData,
		log.CreatedAt,
	)
	return err
}

// GetProcessAuditLogs retrieves all audit logs for a process instance
func (r *AuditRepository) GetProcessAuditLogs(processInstanceID uuid.UUID) ([]models.AuditLog, error) {
	var logs []models.AuditLog

	query := `
		SELECT id, process_instance_id, task_id, user_id, action, old_data, new_data, created_at
		FROM public.audit_logs
		WHERE process_instance_id = $1
		ORDER BY created_at ASC
	`

	err := r.db.Select(&logs, query, processInstanceID)
	return logs, err
}

// GetTaskAuditLogs retrieves all audit logs for a task
func (r *AuditRepository) GetTaskAuditLogs(taskID uuid.UUID) ([]models.AuditLog, error) {
	var logs []models.AuditLog

	query := `
		SELECT id, process_instance_id, task_id, user_id, action, old_data, new_data, created_at
		FROM public.audit_logs
		WHERE task_id = $1
		ORDER BY created_at ASC
	`

	err := r.db.Select(&logs, query, taskID)
	return logs, err
}

// GetAuditLogsByAction retrieves audit logs by action type
func (r *AuditRepository) GetAuditLogsByAction(action models.AuditAction, limit int) ([]models.AuditLog, error) {
	var logs []models.AuditLog

	query := `
		SELECT id, process_instance_id, task_id, user_id, action, old_data, new_data, created_at
		FROM public.audit_logs
		WHERE action = $1
		ORDER BY created_at DESC
		LIMIT $2
	`

	err := r.db.Select(&logs, query, string(action), limit)
	return logs, err
}

// GetAuditLogsByDateRange retrieves audit logs within a date range
func (r *AuditRepository) GetAuditLogsByDateRange(start, end time.Time) ([]models.AuditLog, error) {
	var logs []models.AuditLog

	query := `
		SELECT id, process_instance_id, task_id, user_id, action, old_data, new_data, created_at
		FROM public.audit_logs
		WHERE created_at BETWEEN $1 AND $2
		ORDER BY created_at ASC
	`

	err := r.db.Select(&logs, query, start, end)
	return logs, err
}
