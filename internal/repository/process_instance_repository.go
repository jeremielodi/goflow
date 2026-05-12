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

// GetVariablesTx returns the current variables for a process instance (inside a transaction).
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

// UpsertVariablesTx inserts or updates variables for a process instance (inside a transaction).
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

// ReactivateExecutionTx sets execution status to 'active' (inside a transaction).
func (r *ProcessInstanceRepository) ReactivateExecutionTx(tx *sqlx.Tx, execID uuid.UUID) error {
	_, err := tx.Exec(`UPDATE public.executions SET status = 'active', updated_at = $1 WHERE id = $2`, time.Now(), execID)
	return err
}

// GetProcessDefinitionID returns the definition_id of a process instance.
func (r *ProcessInstanceRepository) GetProcessDefinitionID(instanceID uuid.UUID) (uuid.UUID, error) {
	var defID uuid.UUID
	err := r.db.Get(&defID, `SELECT process_definition_id FROM public.process_instances WHERE id = $1`, instanceID)
	return defID, err
}
