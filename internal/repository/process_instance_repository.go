package repository

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type ProcessInstanceRepository struct {
	db *sqlx.DB
}

func NewProcessInstanceRepository(db *sqlx.DB) *ProcessInstanceRepository {
	return &ProcessInstanceRepository{db: db}
}

// CreateProcessInstance creates a new process instance. tenantID is
// stamped from the process definition being instantiated (nil = no
// tenant / untenanted deployment) so list/search/get endpoints can filter
// by the caller's tenant without a join.
func (r *ProcessInstanceRepository) CreateProcessInstanceTx(tx *sqlx.Tx, instanceID, processDefID uuid.UUID, startedAt time.Time, tenantID *string) error {
	_, err := tx.Exec(`
		INSERT INTO public.process_instances
			(id, process_definition_id, status, started_at, tenant_id)
		VALUES ($1, $2, 'running', $3, $4)
	`, instanceID, processDefID, startedAt, tenantID)
	return err
}

// CreateVariables creates initial variables for a process instance
func (r *ProcessInstanceRepository) CreateVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, variables map[string]interface{}, updatedAt time.Time) error {
	varData, err := json.Marshal(variables)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO public.variables
			(id, process_instance_id, data, updated_at)
		VALUES ($1, $2, $3, $4)
	`, uuid.New(), instanceID, varData, updatedAt)
	return err
}

// CreateExecution creates an initial execution record
func (r *ProcessInstanceRepository) CreateExecutionTx(tx *sqlx.Tx, execID, instanceID uuid.UUID, startNodeID string, now time.Time) error {
	_, err := tx.Exec(`
		INSERT INTO public.executions
			(id, process_instance_id, current_element_id, status, is_active, created_at, updated_at)
		VALUES ($1, $2, $3, 'active', true, $4, $5)
	`, execID, instanceID, startNodeID, now, now)
	return err
}

// GetVariables returns the current variables for a process instance
func (r *ProcessInstanceRepository) GetVariables(instanceID uuid.UUID) (map[string]interface{}, error) {
	var data []byte
	err := r.db.Get(&data, `SELECT data FROM public.variables WHERE process_instance_id = $1`, instanceID)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	vars := make(map[string]interface{})
	if len(data) > 0 {
		if err := json.Unmarshal(data, &vars); err != nil {
			return nil, err
		}
	}
	return vars, nil
}

// UpsertVariables inserts or updates variables for a process instance
func (r *ProcessInstanceRepository) UpsertVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, vars map[string]interface{}) error {
	jsonData, err := json.Marshal(vars)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
		INSERT INTO public.variables (id, process_instance_id, data, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (process_instance_id) DO UPDATE
		SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at
	`, uuid.New(), instanceID, jsonData, time.Now())
	return err
}

// ReactivateExecution sets execution status to 'active'
func (r *ProcessInstanceRepository) ReactivateExecutionTx(tx *sqlx.Tx, execID uuid.UUID) error {
	_, err := tx.Exec(`UPDATE public.executions SET status = 'active', updated_at = $1 WHERE id = $2`, time.Now(), execID)
	return err
}

// GetProcessDefinitionID returns the definition_id of a process instance
func (r *ProcessInstanceRepository) GetProcessDefinitionID(instanceID uuid.UUID) (uuid.UUID, error) {
	var defID uuid.UUID
	err := r.db.Get(&defID, `SELECT process_definition_id FROM public.process_instances WHERE id = $1`, instanceID)
	return defID, err
}

// UpdateProcessInstanceStatus updates the status of a process instance
func (r *ProcessInstanceRepository) UpdateProcessInstanceStatusTx(tx *sqlx.Tx, instanceID uuid.UUID, status string, endTime *time.Time) error {
	query := `UPDATE public.process_instances SET status = $1`
	args := []interface{}{status}

	if endTime != nil {
		query += `, ended_at = $2`
		args = append(args, *endTime)
		query += ` WHERE id = $3`
		args = append(args, instanceID)
	} else {
		query += ` WHERE id = $2`
		args = append(args, instanceID)
	}

	_, err := tx.Exec(query, args...)
	return err
}

// internal/repository/process_instance_repository.go
// Add these methods

// UpsertVariables creates or updates variables for a process instance
func (r *ProcessInstanceRepository) UpsertVariables(instanceID uuid.UUID, variables map[string]interface{}) error {
	jsonData, err := json.Marshal(variables)
	if err != nil {
		return err
	}

	// Check if variables exist
	var exists bool
	err = r.db.Get(&exists, `
		SELECT EXISTS(SELECT 1 FROM public.variables WHERE process_instance_id = $1)
	`, instanceID)
	if err != nil {
		return err
	}

	if exists {
		_, err = r.db.Exec(`
			UPDATE public.variables 
			SET data = $1, updated_at = NOW()
			WHERE process_instance_id = $2
		`, jsonData, instanceID)
	} else {
		_, err = r.db.Exec(`
			INSERT INTO public.variables (id, process_instance_id, data, updated_at)
			VALUES ($1, $2, $3, NOW())
		`, uuid.New(), instanceID, jsonData)
	}
	return err
}

// FindAll retrieves process instances with optional filters. An empty
// tenantID means "no tenant filter" (superuser / untenanted deployment).
func (r *ProcessInstanceRepository) FindAll(status, processKey, tenantID string) ([]models.ProcessInstance, error) {
	var instances []models.ProcessInstance
	query := `
		SELECT pi.id, pi.process_definition_id, pi.status, pi.started_by, pi.started_at, pi.ended_at, pi.tenant_id,
		       pd.process_key, pd.process_name, pd.version
		FROM public.process_instances pi
		LEFT JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE 1=1
	`
	var args []interface{}
	argIndex := 1

	if status != "" {
		query += fmt.Sprintf(" AND pi.status = $%d", argIndex)
		args = append(args, status)
		argIndex++
	}
	if processKey != "" {
		query += fmt.Sprintf(" AND pd.process_key = $%d", argIndex)
		args = append(args, processKey)
		argIndex++
	}
	if tenantID != "" {
		query += fmt.Sprintf(" AND pi.tenant_id = $%d", argIndex)
		args = append(args, tenantID)
		argIndex++
	}

	query += " ORDER BY pi.started_at DESC"

	err := r.db.Select(&instances, query, args...)
	return instances, err
}

// Delete removes a process instance and all related data
func (r *ProcessInstanceRepository) Delete(instanceID uuid.UUID) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete related data (foreign keys will cascade)
	_, err = tx.Exec("DELETE FROM public.process_instances WHERE id = $1", instanceID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// internal/repository/process_instance_repository.go
// Add this method to the ProcessInstanceRepository struct

// FindByID retrieves a process instance by its ID
func (r *ProcessInstanceRepository) FindByID(id uuid.UUID) (*models.ProcessInstance, error) {
	var instance models.ProcessInstance

	query := `
		SELECT 
			pi.id,
			pi.process_definition_id,
			pi.status,
			pi.started_by,
			pi.transaction_scope_id,
			pi.started_at,
			pi.ended_at,
			pi.tenant_id,
			pd.process_key,
			pd.process_name,
			pd.version
		FROM public.process_instances pi
		LEFT JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE pi.id = $1
	`

	err := r.db.Get(&instance, query, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &instance, nil
}

// GetExecutionByID retrieves an execution by its ID
func (r *ProcessInstanceRepository) GetExecutionByID(execID uuid.UUID) (*Execution, error) {
	var exec Execution
	err := r.db.Get(&exec, `
		SELECT id, process_instance_id, current_element_id, status, is_active, created_at, updated_at
		FROM public.executions 
		WHERE id = $1
	`, execID)
	if err != nil {
		return nil, err
	}
	return &exec, nil
}

// internal/repository/process_instance_repository.go
// Add these missing methods to the ProcessInstanceRepository struct

// ============================================================
// PROCESS INSTANCE STATE MANAGEMENT
// ============================================================

// UpdateStatus updates the status of a process instance
func (r *ProcessInstanceRepository) UpdateStatus(instanceID uuid.UUID, status string) error {
	_, err := r.db.Exec(`
		UPDATE public.process_instances 
		SET status = $1, ended_at = NOW()
		WHERE id = $2
	`, status, instanceID)
	return err
}

// UpdateStatusTx updates the status of a process instance within a transaction
func (r *ProcessInstanceRepository) UpdateStatusTx(tx *sqlx.Tx, instanceID uuid.UUID, status string, endedAt *time.Time) error {
	query := `UPDATE public.process_instances SET status = $1`
	args := []interface{}{status}

	if endedAt != nil {
		query += `, ended_at = $2`
		args = append(args, *endedAt)
		query += ` WHERE id = $3`
		args = append(args, instanceID)
	} else {
		query += ` WHERE id = $2`
		args = append(args, instanceID)
	}

	_, err := tx.Exec(query, args...)
	return err
}

// ============================================================
// EXECUTION MANAGEMENT
// ============================================================

// GetActiveExecutions retrieves all active executions for a process instance
func (r *ProcessInstanceRepository) GetActiveExecutions(instanceID uuid.UUID) ([]Execution, error) {
	var executions []Execution
	err := r.db.Select(&executions, `
		SELECT id, process_instance_id, current_element_id, status, is_active,
		       parent_execution_id, path_id, created_at, updated_at
		FROM public.executions
		WHERE process_instance_id = $1 AND is_active = true
	`, instanceID)
	return executions, err
}

// FindExecutionsByInstance retrieves every execution (active or completed)
// for a process instance — completed executions are never deleted, only
// marked is_active=false, so this doubles as the flow-node instance history
// for the Operate-equivalent GET /v2/flownode-instances/search.
func (r *ProcessInstanceRepository) FindExecutionsByInstance(instanceID uuid.UUID) ([]Execution, error) {
	var executions []Execution
	err := r.db.Select(&executions, `
		SELECT id, process_instance_id, current_element_id, status, is_active,
		       parent_execution_id, path_id, created_at, updated_at
		FROM public.executions
		WHERE process_instance_id = $1
		ORDER BY created_at ASC
	`, instanceID)
	return executions, err
}

// UpdateExecutionStatus updates the status of an execution
func (r *ProcessInstanceRepository) UpdateExecutionStatus(execID uuid.UUID, status string) error {
	_, err := r.db.Exec(`
		UPDATE public.executions 
		SET status = $1, updated_at = NOW()
		WHERE id = $2
	`, status, execID)
	return err
}

// UpdateExecutionStatusTx updates the status of an execution within a transaction
func (r *ProcessInstanceRepository) UpdateExecutionStatusTx(tx *sqlx.Tx, execID uuid.UUID, status string) error {
	_, err := tx.Exec(`
		UPDATE public.executions 
		SET status = $1, updated_at = NOW()
		WHERE id = $2
	`, status, execID)
	return err
}

// UpdateExecutionNode updates the current node of an execution
func (r *ProcessInstanceRepository) UpdateExecutionNode(execID uuid.UUID, nodeID string) error {
	_, err := r.db.Exec(`
		UPDATE public.executions 
		SET current_element_id = $1, updated_at = NOW()
		WHERE id = $2
	`, nodeID, execID)
	return err
}

// ============================================================
// HISTORIC QUERIES
// ============================================================

// FindHistoricByID retrieves a completed/terminated process instance
func (r *ProcessInstanceRepository) FindHistoricByID(instanceID uuid.UUID) (*models.ProcessInstance, error) {
	var instance models.ProcessInstance

	query := `
		SELECT 
			pi.id,
			pi.process_definition_id,
			pi.status,
			pi.started_by,
			pi.started_at,
			pi.ended_at,
			pi.tenant_id,
			pd.process_key,
			pd.process_name,
			pd.version
		FROM public.process_instances pi
		LEFT JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE pi.id = $1 AND (pi.status = 'completed' OR pi.status = 'terminated')
	`

	err := r.db.Get(&instance, query, instanceID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &instance, nil
}

// FindAllHistoric retrieves all completed/terminated process instances with filters
func (r *ProcessInstanceRepository) FindAllHistoric(status, processKey string, limit, offset int) ([]models.ProcessInstance, error) {
	var instances []models.ProcessInstance

	query := `
		SELECT 
			pi.id,
			pi.process_definition_id,
			pi.status,
			pi.started_by,
			pi.started_at,
			pi.ended_at,
			pd.process_key,
			pd.process_name,
			pd.version
		FROM public.process_instances pi
		LEFT JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE (pi.status = 'completed' OR pi.status = 'terminated')
	`

	var args []interface{}
	argIndex := 1

	if status != "" {
		query += fmt.Sprintf(" AND pi.status = $%d", argIndex)
		args = append(args, status)
		argIndex++
	}
	if processKey != "" {
		query += fmt.Sprintf(" AND pd.process_key = $%d", argIndex)
		args = append(args, processKey)
		argIndex++
	}

	query += " ORDER BY pi.started_at DESC"

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, limit)
		argIndex++
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIndex)
		args = append(args, offset)
	}

	err := r.db.Select(&instances, query, args...)
	return instances, err
}

// CountHistoric counts completed/terminated process instances with filters
func (r *ProcessInstanceRepository) CountHistoric(status, processKey string) (int, error) {
	var count int

	query := `
		SELECT COUNT(*)
		FROM public.process_instances pi
		LEFT JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE (pi.status = 'completed' OR pi.status = 'terminated')
	`

	var args []interface{}
	argIndex := 1

	if status != "" {
		query += fmt.Sprintf(" AND pi.status = $%d", argIndex)
		args = append(args, status)
		argIndex++
	}
	if processKey != "" {
		query += fmt.Sprintf(" AND pd.process_key = $%d", argIndex)
		args = append(args, processKey)
		argIndex++
	}

	err := r.db.Get(&count, query, args...)
	return count, err
}

// ============================================================
// BULK OPERATIONS
// ============================================================

// DeleteAllHistoric deletes all completed/terminated process instances older than a date
func (r *ProcessInstanceRepository) DeleteAllHistoric(beforeDate time.Time) (int64, error) {
	result, err := r.db.Exec(`
		DELETE FROM public.process_instances 
		WHERE (status = 'completed' OR status = 'terminated') 
		AND ended_at < $1
	`, beforeDate)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// ============================================================
// VARIABLE MANAGEMENT
// ============================================================

// DeleteVariable removes a single variable from a process instance
func (r *ProcessInstanceRepository) DeleteVariable(instanceID uuid.UUID, varName string) error {
	// Get current variables
	variables, err := r.GetVariables(instanceID)
	if err != nil {
		return err
	}

	// Delete variable
	delete(variables, varName)

	// Save updated variables
	return r.UpsertVariables(instanceID, variables)
}

// DeleteVariableTx removes a single variable from a process instance within a transaction
func (r *ProcessInstanceRepository) DeleteVariableTx(tx *sqlx.Tx, instanceID uuid.UUID, varName string) error {
	// Get current variables
	variables, err := r.GetVariables(instanceID)
	if err != nil {
		return err
	}

	// Delete variable
	delete(variables, varName)

	// Save updated variables
	return r.UpsertVariablesTx(tx, instanceID, variables)
}

// ============================================================
// PROCESS INSTANCE VALIDATION
// ============================================================

// Exists checks if a process instance exists
func (r *ProcessInstanceRepository) Exists(instanceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.Get(&exists, `
		SELECT EXISTS(SELECT 1 FROM public.process_instances WHERE id = $1)
	`, instanceID)
	return exists, err
}

// IsActive checks if a process instance is still active (not completed/terminated)
func (r *ProcessInstanceRepository) IsActive(instanceID uuid.UUID) (bool, error) {
	var status string
	err := r.db.Get(&status, `
		SELECT status FROM public.process_instances WHERE id = $1
	`, instanceID)
	if err != nil {
		return false, err
	}
	return status == "running" || status == "suspended", nil
}

// ============================================================
// AUDIT LOGS
// ============================================================

// CreateAuditLog creates an audit log entry
func (r *ProcessInstanceRepository) CreateAuditLog(log models.AuditLog) error {
	_, err := r.db.Exec(`
		INSERT INTO public.audit_logs 
		(id, process_instance_id, task_id, user_id, action, old_data, new_data, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, uuid.New(), log.ProcessInstanceID, log.TaskID, log.UserID, log.Action, log.OldData, log.NewData, time.Now())
	return err
}

// GetAuditLogs retrieves audit logs for a process instance
func (r *ProcessInstanceRepository) GetAuditLogs(instanceID uuid.UUID) ([]models.AuditLog, error) {
	var logs []models.AuditLog
	err := r.db.Select(&logs, `
		SELECT id, process_instance_id, task_id, user_id, action, old_data, new_data, created_at
		FROM public.audit_logs
		WHERE process_instance_id = $1
		ORDER BY created_at ASC
	`, instanceID)
	return logs, err
}

// ============================================================
// TASK MANAGEMENT
// ============================================================

// GetTasksByProcessInstance retrieves all tasks for a process instance
func (r *ProcessInstanceRepository) GetTasksByProcessInstance(instanceID uuid.UUID) ([]models.Task, error) {
	var tasks []models.Task
	err := r.db.Select(&tasks, `
		SELECT id, process_instance_id, execution_id, task_definition_key, task_name,
		       assignee, candidate_group, status, form_data, created_at, claimed_at, completed_at
		FROM public.tasks
		WHERE process_instance_id = $1
		ORDER BY created_at ASC
	`, instanceID)
	return tasks, err
}

// ============================================================
// STATISTICS
// ============================================================

// GetStatistics returns statistics for process instances
func (r *ProcessInstanceRepository) GetStatistics() (map[string]int, error) {
	stats := make(map[string]int)

	var total int
	err := r.db.Get(&total, "SELECT COUNT(*) FROM public.process_instances")
	if err != nil {
		return nil, err
	}
	stats["total"] = total

	var running int
	err = r.db.Get(&running, "SELECT COUNT(*) FROM public.process_instances WHERE status = 'running'")
	if err != nil {
		return nil, err
	}
	stats["running"] = running

	var completed int
	err = r.db.Get(&completed, "SELECT COUNT(*) FROM public.process_instances WHERE status = 'completed'")
	if err != nil {
		return nil, err
	}
	stats["completed"] = completed

	var terminated int
	err = r.db.Get(&terminated, "SELECT COUNT(*) FROM public.process_instances WHERE status = 'terminated'")
	if err != nil {
		return nil, err
	}
	stats["terminated"] = terminated

	var suspended int
	err = r.db.Get(&suspended, "SELECT COUNT(*) FROM public.process_instances WHERE status = 'suspended'")
	if err != nil {
		return nil, err
	}
	stats["suspended"] = suspended

	return stats, nil
}

// Execution represents an execution record
type Execution struct {
	ID                uuid.UUID  `db:"id"`
	ProcessInstanceID uuid.UUID  `db:"process_instance_id"`
	CurrentElementID  string     `db:"current_element_id"`
	Status            string     `db:"status"`
	ParentExecutionID *uuid.UUID `db:"parent_execution_id"`
	PathID            *string    `db:"path_id"`
	IsActive          bool       `db:"is_active"`
	CreatedAt         time.Time  `db:"created_at"`
	UpdatedAt         time.Time  `db:"updated_at"`
}
