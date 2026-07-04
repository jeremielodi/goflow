package repository

import (
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// EventLogProjectionRepository backs Phase 0's read-model projector — see
// architecture/event-sourced-kafka-design.md. Every method here upserts a
// single row derived from one consumed event; there is no batching or
// shared transaction across events, so a mid-stream failure never loses
// more than the one event being processed. This is deliberately not the
// HistoricArchiveRepository's Tx-per-batch pattern (that exists for
// multi-table archival within one commit boundary, which doesn't apply
// here — each event maps to exactly one row in exactly one table).
type EventLogProjectionRepository struct {
	db *sqlx.DB
}

func NewEventLogProjectionRepository(db *sqlx.DB) *EventLogProjectionRepository {
	return &EventLogProjectionRepository{db: db}
}

// UpsertInstance creates or refreshes a projected instance row — called on
// ProcessStarted. Fields left blank by the publishing call site (see
// process_start.go) stay NULL rather than being overwritten with empty
// strings.
func (r *EventLogProjectionRepository) UpsertInstance(id uuid.UUID, processDefinitionID *uuid.UUID, processKey string, version int32, status string, startedAt time.Time) error {
	_, err := r.db.Exec(`
		INSERT INTO public.eventlog_projected_instances
			(id, process_definition_id, process_key, version, status, started_at, updated_at)
		VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, 0), $5, $6, NOW())
		ON CONFLICT (id) DO UPDATE SET
			process_definition_id = COALESCE(EXCLUDED.process_definition_id, eventlog_projected_instances.process_definition_id),
			process_key = COALESCE(EXCLUDED.process_key, eventlog_projected_instances.process_key),
			version = COALESCE(EXCLUDED.version, eventlog_projected_instances.version),
			status = EXCLUDED.status,
			updated_at = NOW()
	`, id, processDefinitionID, processKey, version, status, startedAt)
	return err
}

// UpdateInstanceStatus marks a projected instance completed/terminated —
// called on ProcessCompleted/ProcessTerminated. If the instance row doesn't
// exist yet (e.g. the projector started after the instance began), this
// inserts a minimal row rather than silently doing nothing, so the
// projection still reflects reality once folded events catch up.
func (r *EventLogProjectionRepository) UpdateInstanceStatus(id uuid.UUID, status string, endedAt time.Time) error {
	_, err := r.db.Exec(`
		INSERT INTO public.eventlog_projected_instances (id, status, ended_at, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			ended_at = EXCLUDED.ended_at,
			updated_at = NOW()
	`, id, status, endedAt)
	return err
}

// UpsertJob creates or refreshes a projected job row — called on JobCreated.
func (r *EventLogProjectionRepository) UpsertJob(id, processInstanceID uuid.UUID, executionID *uuid.UUID, jobType, status string) error {
	_, err := r.db.Exec(`
		INSERT INTO public.eventlog_projected_jobs
			(id, process_instance_id, execution_id, job_type, status, updated_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			updated_at = NOW()
	`, id, processInstanceID, executionID, jobType, status)
	return err
}

// UpdateJobStatus marks a projected job completed/failed — called on
// JobCompleted/JobFailed. Same insert-if-missing rationale as
// UpdateInstanceStatus.
func (r *EventLogProjectionRepository) UpdateJobStatus(id, processInstanceID uuid.UUID, status, errorMessage string, retriesRemaining *int32) error {
	_, err := r.db.Exec(`
		INSERT INTO public.eventlog_projected_jobs (id, process_instance_id, status, error_message, retries_remaining, updated_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, NOW())
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			error_message = COALESCE(EXCLUDED.error_message, eventlog_projected_jobs.error_message),
			retries_remaining = COALESCE(EXCLUDED.retries_remaining, eventlog_projected_jobs.retries_remaining),
			updated_at = NOW()
	`, id, processInstanceID, status, errorMessage, retriesRemaining)
	return err
}

// ProjectedInstance and ProjectedJob are the JSON-friendly shapes returned
// by the debug endpoint (GET /engine-rest/eventlog/projected).
type ProjectedInstance struct {
	ID                  uuid.UUID  `db:"id" json:"id"`
	ProcessDefinitionID *uuid.UUID `db:"process_definition_id" json:"processDefinitionId,omitempty"`
	ProcessKey          *string    `db:"process_key" json:"processKey,omitempty"`
	Version             *int32     `db:"version" json:"version,omitempty"`
	Status              string     `db:"status" json:"status"`
	StartedAt           *string    `db:"started_at" json:"startedAt,omitempty"`
	EndedAt             *string    `db:"ended_at" json:"endedAt,omitempty"`
}

type ProjectedJob struct {
	ID                uuid.UUID  `db:"id" json:"id"`
	ProcessInstanceID uuid.UUID  `db:"process_instance_id" json:"processInstanceId"`
	ExecutionID       *uuid.UUID `db:"execution_id" json:"executionId,omitempty"`
	JobType           *string    `db:"job_type" json:"jobType,omitempty"`
	Status            string     `db:"status" json:"status"`
	ErrorMessage      *string    `db:"error_message" json:"errorMessage,omitempty"`
	RetriesRemaining  *int32     `db:"retries_remaining" json:"retriesRemaining,omitempty"`
}

func (r *EventLogProjectionRepository) ListInstances() ([]ProjectedInstance, error) {
	var rows []ProjectedInstance
	err := r.db.Select(&rows, `
		SELECT id, process_definition_id, process_key, version, status,
		       started_at::text, ended_at::text
		FROM public.eventlog_projected_instances
		ORDER BY updated_at DESC
		LIMIT 200
	`)
	return rows, err
}

func (r *EventLogProjectionRepository) ListJobs() ([]ProjectedJob, error) {
	var rows []ProjectedJob
	err := r.db.Select(&rows, `
		SELECT id, process_instance_id, execution_id, job_type, status,
		       error_message, retries_remaining
		FROM public.eventlog_projected_jobs
		ORDER BY updated_at DESC
		LIMIT 200
	`)
	return rows, err
}
