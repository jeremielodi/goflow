package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

// Custom errors
var (
	ErrInvalidOutgoingFlows = errors.New("node has invalid number of outgoing flows")
	ErrNodeNotFound         = errors.New("node not found in graph")
)

type EngineRepository struct {
	db *sqlx.DB
}

func NewEngineRepository(db *sqlx.DB) *EngineRepository {
	return &EngineRepository{db: db}
}

// GetActiveExecutionWithTx retrieves an active execution within a transaction
func (r *EngineRepository) GetActiveExecutionWithTx(tx *sqlx.Tx, execID uuid.UUID) (*Execution, error) {
	var exec Execution
	query := `
		SELECT id, process_instance_id, current_element_id, status, is_active, parent_execution_id, path_id, created_at, updated_at
		FROM public.executions
		WHERE id = $1 AND is_active = true
	`
	err := tx.Get(&exec, query, execID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &exec, nil
}

// GetActiveExecution retrieves an active execution by ID
func (r *EngineRepository) GetActiveExecution(ctx context.Context, execID uuid.UUID) (*Execution, error) {
	var exec Execution
	query := `
		SELECT id, process_instance_id, current_element_id, status, is_active, parent_execution_id, path_id, created_at, updated_at
		FROM public.executions
		WHERE id = $1 AND is_active = true
	`
	err := r.db.GetContext(ctx, &exec, query, execID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &exec, nil
}

// CreateTimerJob creates a new timer job
func (r *EngineRepository) CreateTimerJob(timer *models.TimerJob) error {
	_, err := r.db.Exec(`
		INSERT INTO public.timer_jobs 
		(id, process_instance_id, execution_id, event_type, due_at, payload, is_triggered, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, false, NOW())
	`, timer.ID, timer.ProcessInstanceID, timer.ExecutionID, timer.EventType, timer.DueAt, timer.Payload)
	return err
}

// GetExecutionByID retrieves an execution by ID regardless of active status
func (r *EngineRepository) GetExecutionByID(execID uuid.UUID) (*Execution, error) {
	var exec Execution
	query := `
		SELECT id, process_instance_id, current_element_id, status, is_active, parent_execution_id, path_id, created_at, updated_at
		FROM public.executions
		WHERE id = $1
	`
	err := r.db.Get(&exec, query, execID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &exec, nil
}

func (r *EngineRepository) CountGatewayWaitingState(processInstanceId *uuid.UUID, nodeId string) int {
	var count int
	query := `
		SELECT COUNT(*) 
		FROM public.gateway_state 
		WHERE process_instance_id = $1 
		  AND gateway_id = $2 
		  AND status = 'waiting'
	`
	r.db.Get(&count, query, processInstanceId, nodeId)
	return count
}

// GetProcessVariables retrieves variables for a process instance
func (r *EngineRepository) GetProcessVariables(instanceID uuid.UUID) (map[string]interface{}, error) {
	var data []byte
	err := r.db.Get(&data, `
		SELECT data FROM public.variables
		WHERE process_instance_id = $1
	`, instanceID)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	variables := make(map[string]interface{})
	if len(data) > 0 {
		if err := json.Unmarshal(data, &variables); err != nil {
			return nil, err
		}
	}
	return variables, nil
}

func (r *EngineRepository) CreateGatewayStateTx(tx *sqlx.Tx, state *models.GatewayState) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Insert("public.gateway_state", []database.QueryParameter{
		{Key: "id", Value: state.ID},
		{Key: "process_instance_id", Value: state.ProcessInstanceID},
		{Key: "gateway_id", Value: state.GatewayID},
		{Key: "expected_incoming", Value: state.ExpectedIncoming},
		{Key: "received_incoming", Value: state.ReceivedIncoming},
		{Key: "joined_flows", Value: state.JoinedFlows},
		{Key: "status", Value: state.Status},
		{Key: "created_at", Value: time.Now()},
	})
	return err
}

// CreateTimerJobTx creates a new timer job within a transaction
func (r *EngineRepository) CreateTimerJobTx(tx *sqlx.Tx, timer *models.TimerJob) error {
	_, err := tx.Exec(`
		INSERT INTO public.timer_jobs 
		(id, process_instance_id, execution_id, event_type, due_at, payload, is_triggered, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, timer.ID, timer.ProcessInstanceID, timer.ExecutionID, timer.EventType, timer.DueAt, timer.Payload, false, time.Now())
	return err
}

// CreateTimerTx creates a timer job
func (r *EngineRepository) CreateTimerTx(tx *sqlx.Tx, timerID, processInstanceID uuid.UUID, executionID *uuid.UUID, nextNodeID string, dueAt time.Time, eventType string) error {
	// Store the next node ID in payload (where to go after timer triggers)
	payload := map[string]interface{}{
		"nextNodeId": nextNodeID,
		"eventType":  eventType,
	}
	payloadBytes, _ := json.Marshal(payload)

	_, err := tx.Exec(`
		INSERT INTO public.timer_jobs 
		(id, process_instance_id, execution_id, event_type, due_at, payload, is_triggered, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, false, NOW())
	`, timerID, processInstanceID, executionID, eventType, dueAt, payloadBytes)
	return err
}

// GetDueTimers retrieves all timers that are due to be triggered
func (r *EngineRepository) GetDueTimers(ctx context.Context, now time.Time) ([]models.TimerJob, error) {
	var timers []models.TimerJob

	query := `
        SELECT id, process_instance_id, execution_id, event_type, due_at, payload, is_triggered, created_at, triggered_at
        FROM public.timer_jobs
        WHERE is_triggered = false AND due_at <= $1
        ORDER BY due_at ASC
        LIMIT 100
    `

	err := r.db.SelectContext(ctx, &timers, query, now)

	for _, t := range timers {
		log.Printf("   - Timer %s: due_at=%s, triggered=%v", t.ID, t.DueAt, t.IsTriggered)
	}

	return timers, err
}

// GetDueTimersByInstance retrieves due timers for a specific process instance
func (r *EngineRepository) GetDueTimersByInstance(instanceID uuid.UUID, now time.Time) ([]models.TimerJob, error) {
	var timers []models.TimerJob

	query := `
		SELECT id, process_instance_id, execution_id, event_type, due_at, payload, is_triggered, created_at, triggered_at
		FROM public.timer_jobs
		WHERE process_instance_id = $1 AND is_triggered = false AND due_at <= $2
		ORDER BY due_at ASC
	`

	err := r.db.Select(&timers, query, instanceID, now)
	return timers, err
}

// MarkTimerFired marks a timer as triggered
func (r *EngineRepository) MarkTimerFired(ctx context.Context, timerID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE public.timer_jobs
		SET is_triggered = true, triggered_at = NOW()
		WHERE id = $1
	`, timerID)
	return err
}

// MarkTimerFiredTx marks a timer as triggered within a transaction
func (r *EngineRepository) MarkTimerFiredTx(tx *sqlx.Tx, timerID uuid.UUID) error {
	_, err := tx.Exec(`
		UPDATE public.timer_jobs
		SET is_triggered = true, triggered_at = NOW()
		WHERE id = $1
	`, timerID)
	return err
}

// DeleteTimer removes a timer job
func (r *EngineRepository) DeleteTimerTx(tx *sqlx.Tx, timerID uuid.UUID) error {
	_, err := tx.Exec(`DELETE FROM public.timer_jobs WHERE id = $1`, timerID)
	return err
}

// DeleteTimersByExecution removes all timers for a specific execution
func (r *EngineRepository) DeleteTimersByExecutionTx(tx *sqlx.Tx, executionID uuid.UUID) error {
	_, err := tx.Exec(`DELETE FROM public.timer_jobs WHERE execution_id = $1`, executionID)
	return err
}

// CancelUserTaskByExecution cancels any active user task for an execution
func (r *EngineRepository) CancelUserTaskByExecutionTx(tx *sqlx.Tx, executionID uuid.UUID) error {
	_, err := tx.Exec(`
		UPDATE public.tasks
		SET status = 'canceled', updated_at = NOW()
		WHERE execution_id = $1 AND status = 'created'
	`, executionID)
	return err
}

// CompleteUserTaskByExecution completes any active user task for an execution
func (r *EngineRepository) CompleteUserTaskByExecutionTx(tx *sqlx.Tx, executionID uuid.UUID) error {
	_, err := tx.Exec(`
		UPDATE public.tasks
		SET status = 'completed', updated_at = NOW()
		WHERE execution_id = $1 AND status = 'created'
	`, executionID)
	return err
}

// UpdateGatewayJoin updates the join count for a gateway
func (r *EngineRepository) UpdateGatewayJoinTx(tx *sqlx.Tx, gatewayID uuid.UUID, flowID string) error {
	var currentState models.GatewayState

	// Get current state with lock
	err := tx.Get(&currentState, `
		SELECT id, expected_incoming, received_incoming, joined_flows, status
		FROM public.gateway_state
		WHERE id = $1 AND status = 'waiting'
		FOR UPDATE
	`, gatewayID)
	if err != nil {
		return err
	}

	newReceived := currentState.ReceivedIncoming + 1

	// Update joined flows
	var joinedFlows []string
	json.Unmarshal(currentState.JoinedFlows, &joinedFlows)
	joinedFlows = append(joinedFlows, flowID)
	joinedFlowsJSON, _ := json.Marshal(joinedFlows)

	// Check if all flows have joined
	if newReceived == currentState.ExpectedIncoming {
		// Gateway is complete
		_, err = tx.Exec(`
			UPDATE public.gateway_state
			SET received_incoming = $1, 
				joined_flows = $2, 
				status = 'completed',
				completed_at = NOW()
			WHERE id = $3
		`, newReceived, joinedFlowsJSON, gatewayID)
	} else {
		// Still waiting for more flows
		_, err = tx.Exec(`
			UPDATE public.gateway_state
			SET received_incoming = $1, joined_flows = $2
			WHERE id = $3
		`, newReceived, joinedFlowsJSON, gatewayID)
	}

	return err
}

// GetChildExecutions gets all child executions for a parent
func (r *EngineRepository) GetChildExecutions(parentExecID uuid.UUID) ([]Execution, error) {
	var executions []Execution
	err := r.db.Select(&executions, `
		SELECT id, process_instance_id, current_element_id, status, is_active, path_id
		FROM public.executions
		WHERE parent_execution_id = $1 AND is_active = true
	`, parentExecID)
	return executions, err
}

// CompleteChildExecution marks a child execution as completed
func (r *EngineRepository) CompleteChildExecutionTx(tx *sqlx.Tx, execID uuid.UUID) error {
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

// UpdateExecutionNode updates the current node of an execution
func (r *EngineRepository) UpdateExecutionNodeTx(tx *sqlx.Tx, execID uuid.UUID, nodeID string) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.executions",
		[]database.QueryParameter{
			{Key: "current_element_id", Value: nodeID},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: execID},
	)
	return err
}

// UpdateExecutionStatus updates the status of an execution
func (r *EngineRepository) UpdateExecutionStatusTx(tx *sqlx.Tx, execID uuid.UUID, status string) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.executions",
		[]database.QueryParameter{
			{Key: "status", Value: status},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: execID},
	)
	return err
}

// UpdateExecutionStatusAndNode updates both status and current node
func (r *EngineRepository) UpdateExecutionStatusAndNodeTx(tx *sqlx.Tx, execID uuid.UUID, status, nodeID string) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Update("public.executions",
		[]database.QueryParameter{
			{Key: "status", Value: status},
			{Key: "current_element_id", Value: nodeID},
			{Key: "updated_at", Value: time.Now()},
		},
		database.QueryParameter{Key: "id", Value: execID},
	)
	return err
}

// CompleteExecution marks an execution as completed
func (r *EngineRepository) CompleteExecutionTx(tx *sqlx.Tx, execID uuid.UUID) error {
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

// CreateJob creates a new service task job
func (r *EngineRepository) CreateJobTx(tx *sqlx.Tx, jobID, processInstanceID, executionID uuid.UUID, jobType string, payload []byte) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Insert("public.jobs", []database.QueryParameter{
		{Key: "id", Value: jobID},
		{Key: "process_instance_id", Value: processInstanceID},
		{Key: "execution_id", Value: executionID},
		{Key: "job_type", Value: jobType},
		{Key: "status", Value: "pending"},
		{Key: "payload", Value: payload},
		{Key: "retries", Value: 3},
		{Key: "created_at", Value: time.Now()},
		{Key: "updated_at", Value: time.Now()},
	})
	return err
}

type UserTask struct {
	ID                uuid.UUID
	ProcessInstanceID uuid.UUID
	ExecutionID       uuid.UUID
	TaskDefinitionKey string
	TaskName          *string
	Assignee          *string
	CandidateGroup    *string
}

// CreateUserTask creates a new user task
func (r *EngineRepository) CreateUserTaskTx(tx *sqlx.Tx, task *UserTask) error {
	adapter := database.NewDabaseAdapter(r.db)

	params := []database.QueryParameter{
		{Key: "id", Value: task.ID},
		{Key: "process_instance_id", Value: task.ProcessInstanceID},
		{Key: "execution_id", Value: task.ExecutionID},
		{Key: "task_definition_key", Value: task.TaskDefinitionKey},
		{Key: "status", Value: "created"},
		{Key: "created_at", Value: time.Now()},
		{Key: "updated_at", Value: time.Now()},
	}

	if task.TaskName != nil {
		params = append(params, database.QueryParameter{Key: "task_name", Value: *task.TaskName})
	}
	if task.Assignee != nil {
		params = append(params, database.QueryParameter{Key: "assignee", Value: *task.Assignee})
	}
	if task.CandidateGroup != nil {
		params = append(params, database.QueryParameter{Key: "candidate_group", Value: *task.CandidateGroup})
	}

	_, err := adapter.Insert("public.tasks", params)
	return err
}

// UpdateProcessInstanceStatus updates the status of a process instance
func (r *EngineRepository) UpdateProcessInstanceStatusTx(tx *sqlx.Tx, instanceID uuid.UUID, status string, endedAt *time.Time) error {
	adapter := database.NewDabaseAdapter(r.db)

	params := []database.QueryParameter{
		{Key: "status", Value: status},
	}

	if endedAt != nil {
		params = append(params, database.QueryParameter{Key: "ended_at", Value: *endedAt})
	}

	_, err := adapter.Update("public.process_instances",
		params,
		database.QueryParameter{Key: "id", Value: instanceID},
	)
	return err
}

// CompleteProcessInstance marks a process instance as completed
func (r *EngineRepository) CompleteProcessInstanceTx(tx *sqlx.Tx, instanceID uuid.UUID) error {
	return r.UpdateProcessInstanceStatusTx(tx, instanceID, "completed", &[]time.Time{time.Now()}[0])
}

// UpdateProcessVariables updates variables for a process instance
func (r *EngineRepository) UpdateProcessVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, variables map[string]interface{}) error {
	jsonData, err := json.Marshal(variables)
	if err != nil {
		return err
	}

	adapter := database.NewDabaseAdapter(r.db)

	// Check if variables exist
	var exists bool
	err = tx.QueryRow(`
		SELECT EXISTS(SELECT 1 FROM public.variables WHERE process_instance_id = $1)
	`, instanceID).Scan(&exists)
	if err != nil {
		return err
	}

	if exists {
		_, err = adapter.Update("public.variables",
			[]database.QueryParameter{
				{Key: "data", Value: jsonData},
				{Key: "updated_at", Value: time.Now()},
			},
			database.QueryParameter{Key: "process_instance_id", Value: instanceID},
		)
	} else {
		_, err = adapter.Insert("public.variables", []database.QueryParameter{
			{Key: "id", Value: uuid.New()},
			{Key: "process_instance_id", Value: instanceID},
			{Key: "data", Value: jsonData},
			{Key: "updated_at", Value: time.Now()},
		})
	}
	return err
}

// MergeProcessVariables merges new variables with existing ones
func (r *EngineRepository) MergeProcessVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, newVars map[string]interface{}) error {
	// Get existing variables
	existingVars, err := r.GetProcessVariables(instanceID)
	if err != nil {
		return err
	}

	// Merge
	for k, v := range newVars {
		existingVars[k] = v
	}

	// Save merged variables
	return r.UpdateProcessVariablesTx(tx, instanceID, existingVars)
}

// MarkTimerTriggered marks a timer as triggered
func (r *EngineRepository) MarkTimerTriggeredTx(tx *sqlx.Tx, timerID uuid.UUID) error {
	_, err := tx.Exec(`
		UPDATE public.timer_jobs
		SET is_triggered = true, triggered_at = NOW()
		WHERE id = $1
	`, timerID)
	return err
}

// GetProcessDefinitionByInstanceID retrieves the process definition for a process instance
func (r *EngineRepository) GetProcessDefinitionByInstanceID(instanceID uuid.UUID) (*models.ProcessDefinition, error) {
	var def models.ProcessDefinition
	query := `
		SELECT pd.id, pd.process_key, pd.version, pd.parsed_graph, pd.created_at
		FROM public.process_definitions pd
		JOIN public.process_instances pi ON pi.process_definition_id = pd.id
		WHERE pi.id = $1
	`
	err := r.db.Get(&def, query, instanceID)
	if err != nil {
		return nil, err
	}
	return &def, nil
}

// GetProcessGraphByInstanceID retrieves just the process graph for a process instance
func (r *EngineRepository) GetProcessGraphByInstanceID(instanceID uuid.UUID) (*engine.ProcessGraph, error) {
	var graphJSON []byte
	query := `
		SELECT pd.parsed_graph
		FROM public.process_definitions pd
		JOIN public.process_instances pi ON pi.process_definition_id = pd.id
		WHERE pi.id = $1
	`
	err := r.db.Get(&graphJSON, query, instanceID)
	if err != nil {
		return nil, err
	}

	var graph engine.ProcessGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return nil, err
	}
	return &graph, nil
}

// GetNodeByID retrieves a node from the graph by ID
func (r *EngineRepository) GetNodeByID(graph *engine.ProcessGraph, nodeID string) (*engine.Node, error) {
	node, ok := graph.Nodes[nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return node, nil
}

// GetProcessInstanceIDByExecutionID retrieves the process instance ID for an execution
func (r *EngineRepository) GetProcessInstanceIDByExecutionID(execID uuid.UUID) (uuid.UUID, error) {
	var instanceID uuid.UUID
	err := r.db.Get(&instanceID, `
		SELECT process_instance_id FROM public.executions WHERE id = $1
	`, execID)
	return instanceID, err
}

// CreateExecution creates a new execution
func (r *EngineRepository) CreateExecutionTx(tx *sqlx.Tx, exec *Execution) error {
	adapter := database.NewDabaseAdapter(r.db)

	_, err := adapter.Insert("public.executions", []database.QueryParameter{
		{Key: "id", Value: exec.ID},
		{Key: "process_instance_id", Value: exec.ProcessInstanceID},
		{Key: "current_element_id", Value: exec.CurrentElementID},
		{Key: "status", Value: exec.Status},
		{Key: "is_active", Value: exec.IsActive},
		{Key: "created_at", Value: time.Now()},
		{Key: "updated_at", Value: time.Now()},
	})
	return err
}

// GetOutgoingFlows returns the outgoing flows from a node
func (r *EngineRepository) GetOutgoingFlows(node *engine.Node) []engine.Flow {
	return node.Outgoing
}

// HasMultipleOutgoingFlows checks if a node has multiple outgoing flows
func (r *EngineRepository) HasMultipleOutgoingFlows(node *engine.Node) bool {
	return len(node.Outgoing) > 1
}

// HasSingleOutgoingFlow checks if a node has a single outgoing flow
func (r *EngineRepository) HasSingleOutgoingFlow(node *engine.Node) bool {
	return len(node.Outgoing) == 1
}

// GetSingleOutgoingFlow returns the single outgoing flow from a node
func (r *EngineRepository) GetSingleOutgoingFlow(node *engine.Node) (*engine.Flow, error) {
	if len(node.Outgoing) != 1 {
		return nil, ErrInvalidOutgoingFlows
	}
	return &node.Outgoing[0], nil
}

// GetServiceTaskNode finds a service task node by ID
func (r *EngineRepository) GetServiceTaskNode(graph *engine.ProcessGraph, nodeID string) (*engine.Node, error) {
	node, ok := graph.Nodes[nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}
	if node.Type != engine.ServiceTaskType {
		return nil, errors.New("node is not a service task")
	}
	return node, nil
}

// CompleteJob marks a job as completed
func (r *EngineRepository) CompleteJobTx(tx *sqlx.Tx, jobID uuid.UUID) error {
	_, err := tx.Exec(`
		UPDATE public.jobs
		SET status = 'completed', updated_at = NOW()
		WHERE id = $1
	`, jobID)
	return err
}

// GetJobByID retrieves a job by ID
func (r *EngineRepository) GetJobByID(jobID uuid.UUID) (*models.Job, error) {
	var job models.Job
	err := r.db.Get(&job, `
		SELECT id, process_instance_id, execution_id, job_type, status, payload, retries, created_at, updated_at
		FROM public.jobs
		WHERE id = $1
	`, jobID)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// GetAvailableJobsByTopic retrieves pending jobs for a topic
func (r *EngineRepository) GetAvailableJobsByTopic(topic string, limit int) ([]models.Job, error) {
	var jobs []models.Job
	err := r.db.Select(&jobs, `
		SELECT id, process_instance_id, execution_id, job_type, status, payload, retries, created_at, updated_at
		FROM public.jobs
		WHERE job_type = $1 AND status = 'pending'
		ORDER BY created_at ASC
		LIMIT $2
	`, topic, limit)
	return jobs, err
}

// LockJob locks a job for a worker
func (r *EngineRepository) LockJob(jobID uuid.UUID, workerID string, lockDuration int64) (bool, error) {
	result, err := r.db.Exec(`
		UPDATE public.jobs
		SET status = 'locked', locked_by = $1, locked_until = NOW() + ($2 * interval '1 millisecond'), updated_at = NOW()
		WHERE id = $3 AND status = 'pending'
	`, workerID, lockDuration, jobID)
	if err != nil {
		return false, err
	}
	rows, _ := result.RowsAffected()
	return rows > 0, nil
}
