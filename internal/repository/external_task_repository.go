package repository

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

type ExternalTaskRepository struct {
	db *sqlx.DB
}

func NewExternalTaskRepository(db *sqlx.DB) *ExternalTaskRepository {
	return &ExternalTaskRepository{db: db}
}

// FetchAndLockJobs fetches and locks pending jobs for given topics
func (r *ExternalTaskRepository) FetchAndLockJobsTx(tx *sqlx.Tx, topics []TopicConfig, workerID string, maxTasks int) ([]TopicJobs, error) {
	// A lost lock only means the job is fetched again (see NoSyncTx).
	if err := NoSyncTx(tx); err != nil {
		return nil, err
	}
	result := []TopicJobs{}

	for _, topic := range topics {
		// First, select and lock pending jobs — jobs alone, no join. Citus
		// phase 2 (see architecture/citus-distributed-storage-design.md)
		// made executions a distributed table; a FOR UPDATE query can't
		// join a distributed table unless it's routable to a single shard,
		// so the previous single query (jobs JOIN executions ... FOR
		// UPDATE) stopped working the moment executions was distributed —
		// found only by actually running the full regression against a
		// real Citus-backed stack. jobs itself stays local and
		// undistributed either way (see the design doc for why), so
		// locking it alone here is unaffected by any of this.
		var jobs []LockedJob

		lockQuery := `
			SELECT
				j.id,
				j.payload,
				j.retries,
				j.execution_id,
				j.process_instance_id,
				j.status,
				j.created_at
			FROM public.jobs j
			WHERE j.job_type = $1
			  AND j.status = 'pending'
			  AND (j.locked_by IS NULL OR j.locked_until < NOW())
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		`

		lockedUntil := time.Now().Add(time.Duration(topic.LockDuration) * time.Millisecond)
		var err error
		if distributedClaims.Load() {
			jobs, err = claimJobsTx(tx, topic.TopicName, workerID, lockedUntil, maxTasks)
		} else {
			err = tx.Select(&jobs, lockQuery, topic.TopicName, maxTasks)
		}
		if err != nil {
			return nil, err
		}

		if len(jobs) > 0 {
			// Fetch current_element_id for the now-locked jobs' executions
			// as a separate, unlocked query — a plain multi-shard SELECT
			// against a distributed table is fine, only FOR UPDATE isn't.
			elemPlaceholders := make([]string, len(jobs))
			elemArgs := make([]interface{}, len(jobs))
			for i, job := range jobs {
				elemPlaceholders[i] = fmt.Sprintf("$%d", i+1)
				elemArgs[i] = job.ExecutionID
			}
			var elements []struct {
				ID               uuid.UUID `db:"id"`
				CurrentElementID string    `db:"current_element_id"`
			}
			elemQuery := fmt.Sprintf(`
				SELECT id, current_element_id FROM public.executions WHERE id IN (%s)
			`, strings.Join(elemPlaceholders, ", "))
			if err := tx.Select(&elements, elemQuery, elemArgs...); err != nil {
				return nil, err
			}
			elemByID := make(map[uuid.UUID]string, len(elements))
			for _, e := range elements {
				elemByID[e.ID] = e.CurrentElementID
			}
			for i := range jobs {
				jobs[i].CurrentElementID = elemByID[jobs[i].ExecutionID]
			}
			if distributedClaims.Load() {
				// Already locked, one by one, by claimJobsTx.
				result = append(result, TopicJobs{TopicName: topic.TopicName, Jobs: jobs})
				continue
			}

			// Build IN clause with proper placeholders
			placeholders := make([]string, len(jobs))
			args := make([]interface{}, 0, len(jobs)+2)
			args = append(args, workerID, lockedUntil)

			for i, job := range jobs {
				placeholders[i] = fmt.Sprintf("$%d", i+3)
				args = append(args, job.ID)
			}

			updateQuery := fmt.Sprintf(`
				UPDATE public.jobs
				SET status = 'locked', locked_by = $1, locked_until = $2
				WHERE id IN (%s)
			`, strings.Join(placeholders, ", "))

			_, err = tx.Exec(updateQuery, args...)
			if err != nil {
				return nil, err
			}

			result = append(result, TopicJobs{
				TopicName: topic.TopicName,
				Jobs:      jobs,
			})
		}
	}

	return result, nil
}

// GetJobWithExecution retrieves a job with its execution context
func (r *ExternalTaskRepository) GetJobWithExecutionTx(tx *sqlx.Tx, jobID uuid.UUID) (*JobWithExecution, error) {
	var result JobWithExecution

	// The job row stays locked until tx ends: two completions (or a
	// completion and a failure) of one job can't both see it 'locked'. The
	// instance comes first so the locking query targets one instance —
	// the only kind of FOR UPDATE Citus accepts on a distributed table.
	var instanceID uuid.UUID
	if err := tx.Get(&instanceID, `SELECT process_instance_id FROM public.jobs WHERE id = $1`, jobID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	query := `
		SELECT
			j.execution_id,
			j.process_instance_id,
			e.current_element_id,
			j.status,
			j.retries
		FROM public.jobs j
		JOIN public.executions e ON e.process_instance_id = j.process_instance_id AND e.id = j.execution_id
		WHERE j.process_instance_id = $1 AND j.id = $2
		FOR UPDATE OF j
	`

	err := tx.QueryRowx(query, instanceID, jobID).StructScan(&result)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &result, nil
}

// The job writes below name the job's process instance: on Citus that
// sends each to the one shard holding the job.

// CompleteJobTx marks a job as completed.
func (r *ExternalTaskRepository) CompleteJobTx(tx *sqlx.Tx, processInstanceID, jobID uuid.UUID) error {
	_, err := tx.Exec(`
		UPDATE public.jobs SET status = 'completed', completed_at = NOW()
		WHERE process_instance_id = $1 AND id = $2`, processInstanceID, jobID)
	return err
}

// FailJobPermanentlyTx marks a job as failed with no more retries.
func (r *ExternalTaskRepository) FailJobPermanentlyTx(tx *sqlx.Tx, processInstanceID, jobID uuid.UUID, errorMessage string, errorCode string) error {
	_, err := tx.Exec(`
		UPDATE public.jobs
		SET status = 'failed', error_message = $1, error_code = $2, updated_at = NOW()
		WHERE process_instance_id = $3 AND id = $4
	`, errorMessage, errorCode, processInstanceID, jobID)
	return err
}

// ResetJobForRetryTx puts a job back to pending with its remaining retries.
func (r *ExternalTaskRepository) ResetJobForRetryTx(tx *sqlx.Tx, processInstanceID, jobID uuid.UUID, retries int, errorMessage string) error {
	_, err := tx.Exec(`
		UPDATE public.jobs
		SET retries = $1, status = 'pending', locked_by = NULL, locked_until = NULL, error_message = $2
		WHERE process_instance_id = $3 AND id = $4
	`, retries, errorMessage, processInstanceID, jobID)
	return err
}

// GetProcessDefinitionGraph retrieves the process graph for a process instance

// GetProcessDefinitionGraphTx retrieves the process graph for a process instance
func (r *ExternalTaskRepository) GetProcessDefinitionGraphTx(tx *sqlx.Tx, instanceID uuid.UUID) ([]byte, error) {
	var graphJSON []byte
	var err error

	query := `
        SELECT pd.parsed_graph
        FROM public.process_definitions pd
        JOIN public.process_instances pi ON pi.process_definition_id = pd.id
        WHERE pi.id = $1
    `

	if tx != nil {
		err = tx.Get(&graphJSON, query, instanceID)
	} else {
		err = r.db.Get(&graphJSON, query, instanceID)
	}

	if err != nil {
		return nil, err
	}

	return graphJSON, nil
}

// CompleteExecution marks an execution as completed. Uses direct SQL rather
// than the DataBaseAdapter.Update helper (which only supports a single
// WHERE condition) — process_instance_id is needed alongside id for
// single-shard Citus routing.
func (r *ExternalTaskRepository) CompleteExecutionTx(tx *sqlx.Tx, processInstanceID, execID uuid.UUID) error {
	_, err := tx.Exec(`
		UPDATE public.executions
		SET status = 'completed', is_active = false, updated_at = NOW()
		WHERE process_instance_id = $1 AND id = $2
	`, processInstanceID, execID)
	return err
}

// UpdateExecutionCurrentNode updates the current node of an execution. Same
// direct-SQL reasoning as CompleteExecutionTx above.
func (r *ExternalTaskRepository) UpdateExecutionCurrentNodeTx(tx *sqlx.Tx, processInstanceID, execID uuid.UUID, nextNodeID string) error {
	_, err := tx.Exec(`
		UPDATE public.executions
		SET current_element_id = $1, status = 'active', updated_at = NOW()
		WHERE process_instance_id = $2 AND id = $3
	`, nextNodeID, processInstanceID, execID)
	return err
}

// GetServiceTaskNode checks if a node exists and returns it
func (r *ExternalTaskRepository) GetServiceTaskNode(graph *engine.ProcessGraph, nodeID string) (*engine.Node, error) {
	node, ok := graph.Nodes[nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}

	if node.Type != engine.ServiceTaskType {
		return nil, ErrNotServiceTask
	}

	return node, nil
}

// CreateJob creates a new external task job
func (r *ExternalTaskRepository) CreateJob(job *models.Job) (sql.Result, error) {
	adapter := database.NewDabaseAdapter(r.db)

	return adapter.Insert("public.jobs", []database.QueryParameter{
		{Key: "id", Value: job.ID},
		{Key: "job_type", Value: job.JobType},
		{Key: "status", Value: job.Status},
		{Key: "payload", Value: job.Payload},
		{Key: "retries", Value: job.Retries},
		{Key: "execution_id", Value: job.ExecutionID},
		{Key: "process_instance_id", Value: job.ProcessInstanceID},
		{Key: "created_at", Value: time.Now()},
	})
}

func (r *ExternalTaskRepository) GetNodeByID(graph *engine.ProcessGraph, nodeID string) (*engine.Node, error) {
	node, ok := graph.Nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("node %s not found in graph", nodeID)
	}
	return node, nil
}

// GetPendingJobsByType retrieves pending jobs by type
func (r *ExternalTaskRepository) GetPendingJobsByType(jobType string, limit int) ([]models.Job, error) {
	var jobs []models.Job

	query := `
		SELECT id, job_type, status, payload, retries, execution_id, process_instance_id, created_at
		FROM public.jobs
		WHERE job_type = $1 AND status = 'pending'
		LIMIT $2
	`

	err := r.db.Select(&jobs, query, jobType, limit)
	return jobs, err
}

// TopicConfig represents a topic configuration for fetching jobs
type TopicConfig struct {
	TopicName    string
	LockDuration int // milliseconds
}

// TopicJobs represents jobs fetched for a specific topic
type TopicJobs struct {
	TopicName string
	Jobs      []LockedJob
}

// JobWithExecution represents a job joined with its execution context
type JobWithExecution struct {
	ExecutionID       uuid.UUID `db:"execution_id"`
	ProcessInstanceID uuid.UUID `db:"process_instance_id"`
	CurrentElementID  string    `db:"current_element_id"`
	Status            string    `db:"status"`
	Retries           int       `db:"retries"`
}

// LockedJob represents a job with its execution context
type LockedJob struct {
	ID                uuid.UUID       `db:"id"`
	Payload           json.RawMessage `db:"payload"`
	Retries           int             `db:"retries"`
	ExecutionID       uuid.UUID       `db:"execution_id"`
	ProcessInstanceID uuid.UUID       `db:"process_instance_id"`
	CurrentElementID  string          `db:"current_element_id"`
	Status            string          `db:"status"`
	CreatedAt         time.Time       `db:"created_at"`
}

// Custom errors
var (
	ErrNotServiceTask = errors.New("node is not a service task")
)

// ErrJobNotLockedBy: the job isn't locked by that worker (or not locked).
var ErrJobNotLockedBy = errors.New("external task is not locked by this worker")

// ExtendLock moves a locked job's lock expiry, if workerID holds it.
func (r *ExternalTaskRepository) ExtendLock(jobID uuid.UUID, workerID string, until time.Time) error {
	return r.lockUpdate(jobID, workerID, `UPDATE public.jobs SET locked_until = $1
		WHERE process_instance_id = $2 AND id = $3 AND status = 'locked' AND locked_by = $4`, until)
}

// Unlock gives a locked job back, to be fetched again.
func (r *ExternalTaskRepository) Unlock(jobID uuid.UUID) error {
	return r.lockUpdate(jobID, "", `UPDATE public.jobs SET status = 'pending', locked_by = NULL, locked_until = NULL
		WHERE process_instance_id = $1 AND id = $2 AND status = 'locked'`)
}

// LockOwner returns the worker holding a job ("" when it isn't locked),
// and ErrJobNotFound semantics as sql.ErrNoRows.
func (r *ExternalTaskRepository) LockOwner(jobID uuid.UUID) (string, error) {
	var owner sql.NullString
	err := r.db.Get(&owner, `SELECT CASE WHEN status = 'locked' THEN locked_by END FROM public.jobs WHERE id = $1`, jobID)
	return owner.String, err
}

func (r *ExternalTaskRepository) lockUpdate(jobID uuid.UUID, workerID, query string, until ...time.Time) error {
	var instanceID uuid.UUID
	if err := r.db.Get(&instanceID, `SELECT process_instance_id FROM public.jobs WHERE id = $1`, jobID); err != nil {
		return err
	}
	var res sql.Result
	var err error
	if len(until) > 0 {
		res, err = r.db.Exec(query, until[0], instanceID, jobID, workerID)
	} else {
		res, err = r.db.Exec(query, instanceID, jobID)
	}
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrJobNotLockedBy
	}
	return nil
}
