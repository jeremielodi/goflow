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
	result := []TopicJobs{}

	for _, topic := range topics {
		// First, select pending jobs
		var jobs []LockedJob

		selectQuery := `
			SELECT 
				j.id, 
				j.payload, 
				j.retries, 
				j.execution_id,
				j.process_instance_id,
				e.current_element_id,
				j.status
			FROM public.jobs j
			JOIN public.executions e ON e.id = j.execution_id
			WHERE j.job_type = $1 
			  AND j.status = 'pending'
			  AND (j.locked_by IS NULL OR j.locked_until < NOW())
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		`

		err := tx.Select(&jobs, selectQuery, topic.TopicName, maxTasks)
		if err != nil {
			return nil, err
		}

		if len(jobs) > 0 {
			// Lock the jobs using adapter pattern
			lockedUntil := time.Now().Add(time.Duration(topic.LockDuration) * time.Millisecond)

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

	query := `
		SELECT 
			j.execution_id, 
			j.process_instance_id, 
			e.current_element_id, 
			j.status,
			j.retries
		FROM public.jobs j
		JOIN public.executions e ON e.id = j.execution_id
		WHERE j.id = $1
	`

	err := tx.QueryRowx(query, jobID).StructScan(&result)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return &result, nil
}

// CompleteJob marks a job as completed
func (r *ExternalTaskRepository) CompleteJobTx(tx *sqlx.Tx, jobID uuid.UUID) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.jobs",
		[]database.QueryParameter{
			{Key: "status", Value: "completed"},
			{Key: "completed_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: jobID},
	)
	return err
}

// FailJobPermanently marks a job as failed with no more retries
func (r *ExternalTaskRepository) FailJobPermanentlyTx(tx *sqlx.Tx, jobID uuid.UUID, errorMessage string, errorCode string) error {
	_, err := tx.Exec(`
		UPDATE public.jobs
		SET status = 'failed', error_message = $1, error_code = $2, updated_at = NOW()
		WHERE id = $3
	`, errorMessage, errorCode, jobID)
	return err
}

// ResetJobForRetry resets a job to pending state with decremented retries
func (r *ExternalTaskRepository) ResetJobForRetryTx(tx *sqlx.Tx, jobID uuid.UUID, retries int, errorMessage string) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.jobs",
		[]database.QueryParameter{
			{Key: "retries", Value: retries},
			{Key: "status", Value: "pending"},
			{Key: "locked_by", Value: nil},
			{Key: "locked_until", Value: nil},
			{Key: "error_message", Value: errorMessage},
		},
		database.QueryParameter{Key: "id", Value: jobID},
	)
	return err
}

// UpdateJobStatus updates the status of a job
func (r *ExternalTaskRepository) UpdateJobStatusTx(tx *sqlx.Tx, jobID uuid.UUID, status string) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.jobs",
		[]database.QueryParameter{
			{Key: "status", Value: status},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: jobID},
	)
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

// CompleteProcessInstance marks a process instance as completed
func (r *ExternalTaskRepository) CompleteProcessInstanceTx(tx *sqlx.Tx, instanceID uuid.UUID) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.process_instances",
		[]database.QueryParameter{
			{Key: "status", Value: "completed"},
			{Key: "ended_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: instanceID},
	)
	return err
}

// CompleteExecution marks an execution as completed
func (r *ExternalTaskRepository) CompleteExecutionTx(tx *sqlx.Tx, execID uuid.UUID) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.executions",
		[]database.QueryParameter{
			{Key: "status", Value: "completed"},
			{Key: "is_active", Value: false},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: execID},
	)
	return err
}

// UpdateExecutionCurrentNode updates the current node of an execution
func (r *ExternalTaskRepository) UpdateExecutionCurrentNodeTx(tx *sqlx.Tx, execID uuid.UUID, nextNodeID string) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.executions",
		[]database.QueryParameter{
			{Key: "current_element_id", Value: nextNodeID},
			{Key: "status", Value: "active"},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: execID},
	)
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
}

// Custom errors
var (
	ErrNotServiceTask = errors.New("node is not a service task")
)
