// repository/audit_repository.go
package repository

import (
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type AuditRepository struct {
	db *sqlx.DB
}

func NewAuditRepository(db *sqlx.DB) *AuditRepository {
	return &AuditRepository{db: db}
}

// CreateAuditLog creates a single audit log entry. It doesn't wait for
// the write to reach the disk (synchronous_commit off for this
// transaction only): on a crash the last few milliseconds of audit rows
// may be lost, never engine state, which is written durably by its own
// transactions. Waiting for the disk on every audit row was the largest
// cost of running an instance — COMMIT dominated the database time under
// load.
func (r *AuditRepository) CreateAuditLog(log *models.AuditLog) error {
	if log.ID == uuid.Nil {
		log.ID = uuid.New()
	}
	if log.CreatedAt.IsZero() {
		log.CreatedAt = time.Now()
	}
	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SET LOCAL synchronous_commit TO OFF`); err != nil {
		return err
	}
	if err := r.CreateAuditLogTx(tx, log); err != nil {
		return err
	}
	return tx.Commit()
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
