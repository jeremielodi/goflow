package repository

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type ProcessInstanceRepository struct {
	db *sqlx.DB
}

func NewProcessInstanceRepository(db *sqlx.DB) *ProcessInstanceRepository {
	return &ProcessInstanceRepository{db: db}
}

// CreateProcessInstance creates a new process instance
func (r *ProcessInstanceRepository) CreateProcessInstanceTx(tx *sqlx.Tx, instanceID, processDefID uuid.UUID, startedAt time.Time) error {
	_, err := tx.Exec(`
		INSERT INTO public.process_instances
			(id, process_definition_id, status, started_at)
		VALUES ($1, $2, 'running', $3)
	`, instanceID, processDefID, startedAt)
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

// Execution represents an execution record
type Execution struct {
	ID                uuid.UUID `db:"id"`
	ProcessInstanceID uuid.UUID `db:"process_instance_id"`
	CurrentElementID  string    `db:"current_element_id"`
	Status            string    `db:"status"`
	IsActive          bool      `db:"is_active"`
	CreatedAt         time.Time `db:"created_at"`
	UpdatedAt         time.Time `db:"updated_at"`
}
