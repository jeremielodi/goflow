// internal/repository/historic_archive_repository.go
package repository

import (
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// HistoricArchiveRepository owns the historic_* tables that a finished
// process instance is copied into before its live rows are deleted — see
// internal/service/archive_service.go for the orchestration.
type HistoricArchiveRepository struct {
	db *sqlx.DB
}

func NewHistoricArchiveRepository(db *sqlx.DB) *HistoricArchiveRepository {
	return &HistoricArchiveRepository{db: db}
}

// HistoricProcessInstance mirrors historic_process_instances. Deliberately
// a repository-local type (not shared with internal/service) to avoid an
// import cycle: internal/service already imports internal/repository.
type HistoricProcessInstance struct {
	ID                       uuid.UUID  `db:"id" json:"id"`
	ProcessDefinitionID      uuid.UUID  `db:"process_definition_id" json:"processDefinitionId"`
	ProcessDefinitionKey     string     `db:"process_definition_key" json:"processDefinitionKey"`
	ProcessDefinitionVersion *int       `db:"process_definition_version" json:"processDefinitionVersion,omitempty"`
	TenantID                 *string    `db:"tenant_id" json:"tenantId,omitempty"`
	StartedBy                *string    `db:"started_by" json:"startedBy,omitempty"`
	StartedAt                time.Time  `db:"started_at" json:"startedAt"`
	EndedAt                  time.Time  `db:"ended_at" json:"endedAt"`
	DurationMillis           *int64     `db:"duration_millis" json:"durationInMillis,omitempty"`
	State                    string     `db:"state" json:"state"`
	DeleteReason             *string    `db:"delete_reason" json:"deleteReason,omitempty"`
	ArchivedAt               time.Time  `db:"archived_at" json:"archivedAt"`
}

// HistoricActivityInstanceRow mirrors historic_activity_instances — shaped
// like service.TokenHistoryStep so the archive step can convert 1:1.
type HistoricActivityInstanceRow struct {
	ProcessInstanceID uuid.UUID  `db:"process_instance_id"`
	ExecutionID       *uuid.UUID `db:"execution_id"`
	Action            string     `db:"action"`
	ElementID         *string    `db:"element_id"`
	ElementName       *string    `db:"element_name"`
	TaskID            *uuid.UUID `db:"task_id"`
	Detail            *string    `db:"detail"`
	OccurredAt        time.Time  `db:"occurred_at"`
}

// HistoricTaskRow mirrors historic_tasks.
type HistoricTaskRow struct {
	ID                uuid.UUID  `db:"id"`
	ProcessInstanceID uuid.UUID  `db:"process_instance_id"`
	TaskDefinitionKey string     `db:"task_definition_key"`
	TaskName          *string    `db:"task_name"`
	Assignee          *string    `db:"assignee"`
	Status            string     `db:"status"`
	Priority          *int       `db:"priority"`
	CreatedAt         time.Time  `db:"created_at"`
	ClaimedAt         *time.Time `db:"claimed_at"`
	CompletedAt       *time.Time `db:"completed_at"`
}

// ---- Writes (archival) ------------------------------------------------

func (r *HistoricArchiveRepository) InsertProcessInstanceTx(tx *sqlx.Tx, h HistoricProcessInstance) error {
	_, err := tx.Exec(`
		INSERT INTO public.historic_process_instances
			(id, process_definition_id, process_definition_key, process_definition_version,
			 tenant_id, started_by, started_at, ended_at, duration_millis, state, delete_reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, h.ID, h.ProcessDefinitionID, h.ProcessDefinitionKey, h.ProcessDefinitionVersion,
		h.TenantID, h.StartedBy, h.StartedAt, h.EndedAt, h.DurationMillis, h.State, h.DeleteReason)
	return err
}

func (r *HistoricArchiveRepository) InsertActivityInstancesTx(tx *sqlx.Tx, rows []HistoricActivityInstanceRow) error {
	for _, row := range rows {
		_, err := tx.Exec(`
			INSERT INTO public.historic_activity_instances
				(process_instance_id, execution_id, action, element_id, element_name, task_id, detail, occurred_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, row.ProcessInstanceID, row.ExecutionID, row.Action, row.ElementID, row.ElementName, row.TaskID, row.Detail, row.OccurredAt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *HistoricArchiveRepository) InsertTasksTx(tx *sqlx.Tx, instanceID uuid.UUID, rows []HistoricTaskRow) error {
	for _, row := range rows {
		_, err := tx.Exec(`
			INSERT INTO public.historic_tasks
				(id, process_instance_id, task_definition_key, task_name, assignee, status, priority, created_at, claimed_at, completed_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		`, row.ID, instanceID, row.TaskDefinitionKey, row.TaskName, row.Assignee, row.Status, row.Priority, row.CreatedAt, row.ClaimedAt, row.CompletedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *HistoricArchiveRepository) InsertVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, data map[string]interface{}, updatedAt time.Time) error {
	varData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
		INSERT INTO public.historic_variables (id, process_instance_id, data, updated_at)
		VALUES ($1, $2, $3, $4)
	`, uuid.New(), instanceID, varData, updatedAt)
	return err
}

// CleanupOrphanExecutionStateTx removes any leftover rows in tables that
// reference process_instances(id) with no ON DELETE clause — a leftover
// row in any of these would make DeleteLiveInstanceTx fail with an FK
// violation. Safe: these are internal execution-state tables (gateway
// join tracking, event subscriptions), not history data, so nothing of
// value is lost.
func (r *HistoricArchiveRepository) CleanupOrphanExecutionStateTx(tx *sqlx.Tx, instanceID uuid.UUID) error {
	for _, table := range []string{"gateway_state", "event_subprocesses", "event_subscriptions", "boundary_events"} {
		if _, err := tx.Exec(`DELETE FROM public.`+table+` WHERE process_instance_id = $1`, instanceID); err != nil {
			return err
		}
	}
	return nil
}

func (r *HistoricArchiveRepository) DeleteLiveInstanceTx(tx *sqlx.Tx, instanceID uuid.UUID) error {
	_, err := tx.Exec(`DELETE FROM public.process_instances WHERE id = $1`, instanceID)
	return err
}

// ---- Reads (historic fallback / merge) ---------------------------------

func (r *HistoricArchiveRepository) FindProcessInstanceByID(id uuid.UUID) (*HistoricProcessInstance, error) {
	var h HistoricProcessInstance
	err := r.db.Get(&h, `SELECT * FROM public.historic_process_instances WHERE id = $1`, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &h, nil
}

// HistoricProcessInstanceFilter mirrors the filters ListHistoricProcessInstances
// applies to the live table, so the historic query can be filtered the same
// way when merging live + historic results.
type HistoricProcessInstanceFilter struct {
	ProcessKey    string
	State         string // "" | "completed" | "terminated"
	StartedAfter  *time.Time
	StartedBefore *time.Time
	FinishedAfter *time.Time
}

func (r *HistoricArchiveRepository) FindAllProcessInstances(f HistoricProcessInstanceFilter) ([]HistoricProcessInstance, error) {
	query := `SELECT * FROM public.historic_process_instances WHERE 1=1`
	var args []interface{}
	idx := 1
	if f.ProcessKey != "" {
		query += ` AND process_definition_key = $` + strconv.Itoa(idx)
		args = append(args, f.ProcessKey)
		idx++
	}
	if f.State != "" {
		query += ` AND state = $` + strconv.Itoa(idx)
		args = append(args, f.State)
		idx++
	}
	if f.StartedAfter != nil {
		query += ` AND started_at > $` + strconv.Itoa(idx)
		args = append(args, *f.StartedAfter)
		idx++
	}
	if f.StartedBefore != nil {
		query += ` AND started_at < $` + strconv.Itoa(idx)
		args = append(args, *f.StartedBefore)
		idx++
	}
	if f.FinishedAfter != nil {
		query += ` AND ended_at > $` + strconv.Itoa(idx)
		args = append(args, *f.FinishedAfter)
		idx++
	}
	query += ` ORDER BY started_at DESC`

	var rows []HistoricProcessInstance
	err := r.db.Select(&rows, query, args...)
	return rows, err
}

func (r *HistoricArchiveRepository) FindActivityInstancesByProcessInstance(id uuid.UUID) ([]HistoricActivityInstanceRow, error) {
	var rows []HistoricActivityInstanceRow
	err := r.db.Select(&rows, `
		SELECT process_instance_id, execution_id, action, element_id, element_name, task_id, detail, occurred_at
		FROM public.historic_activity_instances
		WHERE process_instance_id = $1
		ORDER BY occurred_at ASC
	`, id)
	return rows, err
}

func (r *HistoricArchiveRepository) FindTasksByProcessInstance(id uuid.UUID) ([]HistoricTaskRow, error) {
	var rows []HistoricTaskRow
	err := r.db.Select(&rows, `
		SELECT id, process_instance_id, task_definition_key, task_name, assignee, status, priority, created_at, claimed_at, completed_at
		FROM public.historic_tasks
		WHERE process_instance_id = $1
		ORDER BY created_at DESC
	`, id)
	return rows, err
}

// FindTasksForHistoricQuery supports the same HistoricTaskQueryParams as
// HistoricTaskRepository.FindHistoricTasks (which only ever sees tasks
// belonging to still-live instances), so the two can be merged for a
// complete view across both live and archived instances. Ignores
// FirstResult/MaxResults — the caller merges with the live query's results
// and paginates the combined set in Go.
func (r *HistoricArchiveRepository) FindTasksForHistoricQuery(params HistoricTaskQueryParams) ([]HistoricTask, error) {
	query := `
		SELECT
			t.id,
			t.process_instance_id,
			hpi.process_definition_id as process_definition_id,
			hpi.process_definition_key as process_definition_key,
			t.task_definition_key,
			t.task_name,
			t.assignee,
			t.status,
			t.created_at as start_time,
			t.completed_at as end_time,
			t.claimed_at as claim_time,
			'' as delete_reason,
			t.created_at,
			t.created_at as updated_at
		FROM public.historic_tasks t
		JOIN public.historic_process_instances hpi ON hpi.id = t.process_instance_id
		WHERE 1=1
	`
	var conditions []string
	var args []interface{}
	idx := 1

	if params.TaskId != "" {
		conditions = append(conditions, `t.id = $`+strconv.Itoa(idx))
		args = append(args, params.TaskId)
		idx++
	}
	if params.TaskName != "" {
		conditions = append(conditions, `t.task_name = $`+strconv.Itoa(idx))
		args = append(args, params.TaskName)
		idx++
	}
	if params.TaskNameLike != "" {
		conditions = append(conditions, `t.task_name ILIKE $`+strconv.Itoa(idx))
		args = append(args, "%"+params.TaskNameLike+"%")
		idx++
	}
	if params.Assignee != nil {
		conditions = append(conditions, `t.assignee = $`+strconv.Itoa(idx))
		args = append(args, params.Assignee)
		idx++
	}
	if params.AssigneeLike != "" {
		conditions = append(conditions, `t.assignee ILIKE $`+strconv.Itoa(idx))
		args = append(args, "%"+params.AssigneeLike+"%")
		idx++
	}
	if params.ProcessInstanceId != "" {
		conditions = append(conditions, `t.process_instance_id = $`+strconv.Itoa(idx))
		args = append(args, params.ProcessInstanceId)
		idx++
	}
	if params.ProcessDefinitionId != "" {
		conditions = append(conditions, `hpi.process_definition_id = $`+strconv.Itoa(idx))
		args = append(args, params.ProcessDefinitionId)
		idx++
	}
	if params.ProcessDefinitionKey != "" {
		conditions = append(conditions, `hpi.process_definition_key = $`+strconv.Itoa(idx))
		args = append(args, params.ProcessDefinitionKey)
		idx++
	}
	if params.Status != "" {
		conditions = append(conditions, `t.status = $`+strconv.Itoa(idx))
		args = append(args, params.Status)
		idx++
	}
	if params.Finished && !params.Unfinished {
		conditions = append(conditions, `t.completed_at IS NOT NULL`)
	}
	if params.Unfinished && !params.Finished {
		conditions = append(conditions, `t.completed_at IS NULL`)
	}
	if params.StartedBefore != nil {
		conditions = append(conditions, `t.created_at <= $`+strconv.Itoa(idx))
		args = append(args, *params.StartedBefore)
		idx++
	}
	if params.StartedAfter != nil {
		conditions = append(conditions, `t.created_at >= $`+strconv.Itoa(idx))
		args = append(args, *params.StartedAfter)
		idx++
	}
	if params.FinishedBefore != nil {
		conditions = append(conditions, `t.completed_at <= $`+strconv.Itoa(idx))
		args = append(args, *params.FinishedBefore)
		idx++
	}
	if params.FinishedAfter != nil {
		conditions = append(conditions, `t.completed_at >= $`+strconv.Itoa(idx))
		args = append(args, *params.FinishedAfter)
		idx++
	}

	if len(conditions) > 0 {
		query += " AND " + strings.Join(conditions, " AND ")
	}
	query += ` ORDER BY t.created_at DESC`

	var rows []HistoricTask
	err := r.db.Select(&rows, query, args...)
	return rows, err
}

func (r *HistoricArchiveRepository) FindVariablesByProcessInstance(id uuid.UUID) (map[string]interface{}, error) {
	var data []byte
	err := r.db.Get(&data, `SELECT data FROM public.historic_variables WHERE process_instance_id = $1`, id)
	if err != nil {
		if err == sql.ErrNoRows {
			return map[string]interface{}{}, nil
		}
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

