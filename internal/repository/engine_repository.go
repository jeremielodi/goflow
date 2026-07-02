package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

// Custom errors
var (
	ErrInvalidOutgoingFlows = errors.New("node has invalid number of outgoing flows")
	ErrNodeNotFound         = errors.New("node not found in graph")
)

// graphCache holds deserialized ProcessGraph values keyed by process_instance_id string.
var graphCache sync.Map

type EngineRepository struct {
	db         *sqlx.DB
	dispatcher *events.TaskEventDispatcher
}

func NewEngineRepository(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *EngineRepository {
	return &EngineRepository{db: db, dispatcher: dispatcher}
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
	_, err := tx.Exec(`
		INSERT INTO public.gateway_state
		(id, process_instance_id, gateway_id, expected_incoming, received_incoming, joined_flows, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
	`, state.ID, state.ProcessInstanceID, state.GatewayID,
		state.ExpectedIncoming, state.ReceivedIncoming, state.JoinedFlows, state.Status)
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
func (r *EngineRepository) CreateTimerTx(tx *sqlx.Tx, timer *models.TimerJob) error {
	_, err := tx.Exec(`
        INSERT INTO public.timer_jobs 
        (id, process_instance_id, execution_id, event_type, due_at, payload, 
         is_triggered, created_at, cycle_count, total_cycles, repeat_interval)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    `, timer.ID, timer.ProcessInstanceID, timer.ExecutionID,
		timer.EventType, timer.DueAt, timer.Payload, timer.IsTriggered,
		timer.CreatedAt, timer.CycleCount, timer.TotalCycles, timer.RepeatInterval)
	return err
}

// GetDueTimers retrieves all timers that are due to be triggered (legacy, no locking).
func (r *EngineRepository) GetDueTimers(ctx context.Context, now time.Time) ([]models.TimerJob, error) {
	var timers []models.TimerJob
	query := `
        SELECT id, process_instance_id, execution_id, event_type, due_at, payload,
               is_triggered, created_at, triggered_at, cycle_count, total_cycles, repeat_interval
        FROM public.timer_jobs
        WHERE is_triggered = false AND due_at <= NOW()
        ORDER BY due_at ASC
        LIMIT 100
    `
	err := r.db.SelectContext(ctx, &timers, query)
	if err != nil {
		return nil, err
	}
	for _, t := range timers {
		log.Printf("   - Timer %s: due_at=%s, triggered=%v", t.ID, t.DueAt, t.IsTriggered)
	}
	return timers, err
}

// ClaimDueTimers atomically marks due timers as triggered and returns them.
// Uses FOR UPDATE SKIP LOCKED so concurrent scheduler instances never double-fire.
func (r *EngineRepository) ClaimDueTimers(ctx context.Context, limit int) ([]models.TimerJob, error) {
	var timers []models.TimerJob
	query := `
        WITH to_claim AS (
            SELECT id FROM public.timer_jobs
            WHERE is_triggered = false AND due_at <= NOW()
            ORDER BY due_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE public.timer_jobs t
        SET is_triggered = true, triggered_at = NOW()
        FROM to_claim
        WHERE t.id = to_claim.id
        RETURNING t.id, t.process_instance_id, t.execution_id, t.event_type, t.due_at,
                  t.payload, t.is_triggered, t.created_at, t.triggered_at,
                  t.cycle_count, t.total_cycles, t.repeat_interval
    `
	err := r.db.SelectContext(ctx, &timers, query, limit)
	if err != nil {
		return nil, err
	}
	for _, t := range timers {
		log.Printf("   ✅ Claimed timer %s due_at=%s", t.ID, t.DueAt)
	}
	return timers, nil
}

func (r *EngineRepository) CreateTimerWithDurationSeconds(tx *sqlx.Tx, timerID, processInstanceID uuid.UUID, executionID *uuid.UUID, durationSeconds int, eventType string, payload []byte, cycleCount, totalCycles int, repeatInterval string) error {
	_, err := tx.Exec(`
        INSERT INTO public.timer_jobs 
        (id, process_instance_id, execution_id, event_type, due_at, payload, 
         is_triggered, created_at, cycle_count, total_cycles, repeat_interval)
        VALUES ($1, $2, $3, $4, NOW() + ($5 || ' seconds')::interval, $6, false, NOW(), $7, $8, $9)
    `, timerID, processInstanceID, executionID, eventType, durationSeconds, payload, cycleCount, totalCycles, repeatInterval)
	return err
}

// GetMultiInstanceChildrenCount returns the count of completed child executions
func (r *EngineRepository) GetMultiInstanceChildrenCount(parentExecutionID uuid.UUID, status string) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM public.multi_instance_children WHERE parent_execution_id = $1`
	if status != "" {
		query += ` AND status = $2`
		err := r.db.Get(&count, query, parentExecutionID, status)
		return count, err
	}
	err := r.db.Get(&count, query, parentExecutionID)
	return count, err
}

// CompleteMultiInstanceChild marks a child execution as completed
func (r *EngineRepository) CompleteMultiInstanceChildTx(tx *sqlx.Tx, childExecutionID uuid.UUID) error {
	_, err := tx.Exec(`
        UPDATE public.multi_instance_children 
        SET status = 'completed', completed_at = NOW() 
        WHERE child_execution_id = $1
    `, childExecutionID)
	return err
}

func (r *EngineRepository) UpdateMultiInstanceProgressTx(tx *sqlx.Tx, miExecID uuid.UUID, completedCount int) error {
	_, err := tx.Exec(`
        UPDATE public.multi_instance_executions 
        SET completed_count = $1, 
            nr_of_completed_instances = $1, 
            updated_at = NOW() 
        WHERE id = $2
    `, completedCount, miExecID)
	return err
}

// GetMultiInstanceExecution retrieves a multi-instance execution by execution ID
func (r *EngineRepository) GetMultiInstanceExecution(executionID uuid.UUID) (*models.MultiInstanceExecution, error) {
	var miExec models.MultiInstanceExecution
	query := `
        SELECT id, process_instance_id, execution_id, activity_id, is_sequential, total_count, 
               completed_count, nr_of_active_instances, nr_of_completed_instances, loop_counter, 
               completion_condition, element_variable, input_collection, output_collection, status, 
               created_at, updated_at 
        FROM public.multi_instance_executions 
        WHERE execution_id = $1 AND status = 'active'
    `
	err := r.db.Get(&miExec, query, executionID)
	if err != nil {
		return nil, err
	}
	return &miExec, nil
}

func (r *EngineRepository) GetMultiInstanceExecutionByParentId(parentExecID uuid.UUID) (*models.MultiInstanceExecution, error) {
	var mi models.MultiInstanceExecution
	err := r.db.Get(&mi, `SELECT * FROM public.multi_instance_executions WHERE execution_id = $1 AND status = 'active'`, parentExecID)
	return &mi, err
}

// CompleteMultiInstance marks the multi-instance execution as completed
func (r *EngineRepository) CompleteMultiInstanceTx(tx *sqlx.Tx, miExecID uuid.UUID) error {
	_, err := tx.Exec(`
        UPDATE public.multi_instance_executions 
        SET status = 'completed', updated_at = NOW() 
        WHERE id = $1
    `, miExecID)
	return err
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

// internal/repository/engine_repository.go

// CreateTimerWithCycleTx creates a timer job with cycle support
func (r *EngineRepository) CreateTimerWithCycleTx(tx *sqlx.Tx, timerID, processInstanceID uuid.UUID, executionID *uuid.UUID, dueAt time.Time, eventType string, payload []byte, cycleCount, totalCycles int, repeatInterval string) error {
	_, err := tx.Exec(`
        INSERT INTO public.timer_jobs 
        (id, process_instance_id, execution_id, event_type, due_at, payload, is_triggered, created_at, cycle_count, total_cycles, repeat_interval)
        VALUES ($1, $2, $3, $4, $5, $6, false, NOW(), $7, $8, $9)
    `, timerID, processInstanceID, executionID, eventType, dueAt, payload, cycleCount, totalCycles, repeatInterval)
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
	_, err := tx.Exec(`
		UPDATE public.executions SET status = 'completed', is_active = false, updated_at = NOW() WHERE id = $1
	`, execID)
	return err
}

// UpdateExecutionNode updates the current node of an execution
func (r *EngineRepository) UpdateExecutionNodeTx(tx *sqlx.Tx, execID uuid.UUID, nodeID string) error {
	_, err := tx.Exec(`
		UPDATE public.executions SET current_element_id = $1, updated_at = NOW() WHERE id = $2
	`, nodeID, execID)
	return err
}

// UpdateExecutionStatus updates the status of an execution
func (r *EngineRepository) UpdateExecutionStatusTx(tx *sqlx.Tx, execID uuid.UUID, status string) error {
	_, err := tx.Exec(`
		UPDATE public.executions SET status = $1, updated_at = NOW() WHERE id = $2
	`, status, execID)
	return err
}

// UpdateExecutionStatusAndNode updates both status and current node
func (r *EngineRepository) UpdateExecutionStatusAndNodeTx(tx *sqlx.Tx, execID uuid.UUID, status, nodeID string) error {
	_, err := tx.Exec(`
		UPDATE public.executions SET status = $1, current_element_id = $2, updated_at = NOW() WHERE id = $3
	`, status, nodeID, execID)
	return err
}

// CompleteExecution marks an execution as completed
func (r *EngineRepository) CompleteExecutionTx(tx *sqlx.Tx, execID uuid.UUID) error {
	_, err := tx.Exec(`
		UPDATE public.executions SET status = 'completed', is_active = false, updated_at = NOW() WHERE id = $1
	`, execID)
	return err
}

// CreateJob creates a new service task job
func (r *EngineRepository) CreateJobTx(tx *sqlx.Tx, jobID, processInstanceID, executionID uuid.UUID, jobType string, payload []byte) error {
	_, err := tx.Exec(`
		INSERT INTO public.jobs
		(id, process_instance_id, execution_id, job_type, status, payload, retries, created_at, updated_at)
		VALUES ($1, $2, $3, $4, 'pending', $5, 3, NOW(), NOW())
	`, jobID, processInstanceID, executionID, jobType, payload)
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
	Priority          int
}

// CreateUserTask creates a new user task
func (r *EngineRepository) CreateUserTaskTx(tx *sqlx.Tx, task *UserTask) error {
	priority := task.Priority
	if priority == 0 {
		priority = 50
	}
	_, err := tx.Exec(`
		INSERT INTO public.tasks
		(id, process_instance_id, execution_id, task_definition_key, task_name, assignee, candidate_group, priority, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'created', NOW(), NOW())
	`, task.ID, task.ProcessInstanceID, task.ExecutionID, task.TaskDefinitionKey,
		task.TaskName, task.Assignee, task.CandidateGroup, priority)
	return err
}

// repository/engine_repository.go

// CreateProcessInstanceTx creates a new process instance within a transaction
func (r *EngineRepository) CreateProcessInstanceTx(tx *sqlx.Tx, instanceID, processDefinitionID uuid.UUID, startedAt time.Time) error {
	query := `
        INSERT INTO public.process_instances 
        (id, process_definition_id, status, started_at)
        VALUES ($1, $2, $3, $4)
    `
	// Use 'running' from the enum
	_, err := tx.Exec(query, instanceID, processDefinitionID, "running", startedAt)
	return err
}

// UpdateProcessInstanceStatusTx updates the status of a process instance
func (r *EngineRepository) UpdateProcessInstanceStatusTx(tx *sqlx.Tx, instanceID uuid.UUID, status string, endedAt *time.Time) error {
	if endedAt != nil {
		_, err := tx.Exec(`UPDATE public.process_instances SET status = $1, ended_at = $2 WHERE id = $3`, status, *endedAt, instanceID)
		return err
	}
	_, err := tx.Exec(`UPDATE public.process_instances SET status = $1 WHERE id = $2`, status, instanceID)
	return err
}

// CompleteProcessInstance marks a process instance as completed
func (r *EngineRepository) CompleteProcessInstanceTx(tx *sqlx.Tx, instanceID uuid.UUID) error {
	return r.UpdateProcessInstanceStatusTx(tx, instanceID, "completed", &[]time.Time{time.Now()}[0])
}

// // CreateExecutionTx creates a new execution within a transaction
// func (r *EngineRepository) CreateExecutionTx(tx *sqlx.Tx, execID, processInstanceID uuid.UUID, currentElementID string, createdAt time.Time) error {
// 	query := `
//         INSERT INTO public.executions
//         (id, process_instance_id, current_element_id, status, is_active, created_at, updated_at)
//         VALUES ($1, $2, $3, $4, $5, $6, $7)
//     `
// 	// Use 'active' from execution_status enum
// 	_, err := tx.Exec(query, execID, processInstanceID, currentElementID, "active", true, createdAt, createdAt)
// 	return err
// }

// UpdateProcessVariables updates variables for a process instance
func (r *EngineRepository) UpdateProcessVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, variables map[string]interface{}) error {
	jsonData, err := json.Marshal(variables)
	if err != nil {
		return err
	}

	var exists bool
	if err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM public.variables WHERE process_instance_id = $1)`, instanceID).Scan(&exists); err != nil {
		return err
	}

	if exists {
		_, err = tx.Exec(`UPDATE public.variables SET data = $1, updated_at = NOW() WHERE process_instance_id = $2`, jsonData, instanceID)
	} else {
		_, err = tx.Exec(`INSERT INTO public.variables (id, process_instance_id, data, updated_at) VALUES ($1, $2, $3, NOW())`, uuid.New(), instanceID, jsonData)
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

// GetProcessGraphByInstanceID retrieves the process graph for a process instance.
// Results are cached in-process to avoid repeated JSON deserialization.
func (r *EngineRepository) GetProcessGraphByInstanceID(instanceID uuid.UUID) (*engine.ProcessGraph, error) {
	key := instanceID.String()
	if cached, ok := graphCache.Load(key); ok {
		return cached.(*engine.ProcessGraph), nil
	}

	var graphJSON []byte
	query := `
		SELECT pd.parsed_graph
		FROM public.process_definitions pd
		JOIN public.process_instances pi ON pi.process_definition_id = pd.id
		WHERE pi.id = $1
	`
	if err := r.db.Get(&graphJSON, query, instanceID); err != nil {
		return nil, err
	}

	var graph engine.ProcessGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return nil, err
	}

	graphCache.Store(key, &graph)
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

func (r *EngineRepository) CreateExecutionTx2(tx *sqlx.Tx, execID, processInstanceID uuid.UUID, currentElementID string, createdAt time.Time) error {
	query := `
        INSERT INTO public.executions 
        (id, process_instance_id, current_element_id, status, is_active, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    `
	_, err := tx.Exec(query, execID, processInstanceID, currentElementID, "active", true, createdAt, createdAt)
	return err
}

// CreateExecutionTx creates a new execution within a transaction
func (r *EngineRepository) CreateExecutionTx(tx *sqlx.Tx, exec *Execution) error {
	query := `
		INSERT INTO public.executions
		(id, process_instance_id, parent_execution_id, current_element_id, status, is_active, path_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
	`
	_, err := tx.Exec(query,
		exec.ID,
		exec.ProcessInstanceID,
		exec.ParentExecutionID,
		exec.CurrentElementID,
		exec.Status,
		exec.IsActive,
		exec.PathID,
	)
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

// Add these missing methods to repository/engine_repository.go

// ============================================================
// PROCESS DEFINITION METHODS
// ============================================================

// repository/engine_repository.go

func (r *EngineRepository) FindLatestProcessDefinitionByKey(processKey string) (*models.ProcessDefinition, error) {
	var def models.ProcessDefinition
	query := `
        SELECT id, deployment_id, process_key, process_name, version, is_active, bpmn_xml, parsed_graph, created_at
        FROM public.process_definitions
        WHERE process_key = $1 AND is_active = true
        ORDER BY version DESC
        LIMIT 1
    `
	err := r.db.Get(&def, query, processKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("process definition not found for key: %s", processKey)
		}
		return nil, err
	}
	return &def, nil
}

// ============================================================
// VARIABLE METHODS
// ============================================================

// CreateVariablesTx creates variables for a process instance within a transaction
func (r *EngineRepository) CreateVariablesTx(tx *sqlx.Tx, instanceID uuid.UUID, variables map[string]interface{}, updatedAt time.Time) error {
	if len(variables) == 0 {
		return nil
	}

	jsonData, err := json.Marshal(variables)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO public.variables 
		(id, process_instance_id, data, updated_at)
		VALUES ($1, $2, $3, $4)
	`, uuid.New(), instanceID, jsonData, updatedAt)
	return err
}

// SetProcessVariableTx sets a single process variable within a transaction
func (r *EngineRepository) SetProcessVariableTx(tx *sqlx.Tx, instanceID uuid.UUID, key string, value interface{}) error {
	// Get existing variables
	var data []byte
	err := tx.Get(&data, `SELECT data FROM public.variables WHERE process_instance_id = $1`, instanceID)

	vars := make(map[string]interface{})
	if err == nil && len(data) > 0 {
		if err := json.Unmarshal(data, &vars); err != nil {
			return err
		}
	} else if err != nil && err != sql.ErrNoRows {
		return err
	}

	vars[key] = value

	newData, err := json.Marshal(vars)
	if err != nil {
		return err
	}

	if len(data) > 0 {
		_, err = tx.Exec(`UPDATE public.variables SET data = $1, updated_at = NOW() WHERE process_instance_id = $2`, newData, instanceID)
	} else {
		_, err = tx.Exec(`INSERT INTO public.variables (id, process_instance_id, data, updated_at) VALUES ($1, $2, $3, NOW())`,
			uuid.New(), instanceID, newData)
	}
	return err
}

// ============================================================
// TASK METHODS
// ============================================================

// GetTaskByID retrieves a task by ID
func (r *EngineRepository) GetTaskByID(taskID uuid.UUID) (*UserTask, error) {
	var task UserTask
	query := `
		SELECT id, process_instance_id, execution_id, task_definition_key, task_name, 
		       assignee, candidate_group, status, created_at, completed_at
		FROM public.tasks
		WHERE id = $1
	`
	err := r.db.Get(&task, query, taskID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}

// CompleteTask marks a task as completed
func (r *EngineRepository) CompleteTask(taskID uuid.UUID, variables map[string]interface{}) error {
	tx, err := r.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get task to find process instance
	var task UserTask
	err = tx.Get(&task, `SELECT id, process_instance_id, execution_id, task_definition_key FROM public.tasks WHERE id = $1`, taskID)
	if err != nil {
		return err
	}

	// Update variables if any
	if len(variables) > 0 {
		err = r.MergeProcessVariablesTx(tx, task.ProcessInstanceID, variables)
		if err != nil {
			return err
		}
	}

	// Mark task as completed
	_, err = tx.Exec(`UPDATE public.tasks SET status = 'completed', completed_at = NOW(), updated_at = NOW() WHERE id = $1`, taskID)
	if err != nil {
		return err
	}

	// Get execution and update it
	_, err = tx.Exec(`UPDATE public.executions SET status = 'active', updated_at = NOW() WHERE id = $1`, task.ExecutionID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// ============================================================
// PROCESS INSTANCE CREATION (for worker pool)
// ============================================================

// CreateProcessInstanceWithVariables creates a process instance and its variables in one transaction
func (r *EngineRepository) CreateProcessInstanceWithVariables(processKey string, variables map[string]interface{}) (uuid.UUID, uuid.UUID, error) {
	// Get process definition
	procDef, err := r.FindLatestProcessDefinitionByKey(processKey)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	// Build graph
	graph, err := engine.BuildGraphForProcess([]byte(procDef.BpmnXML), processKey)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	// Find start node
	var startNodeID string
	for _, node := range graph.Nodes {
		if node.Type == engine.StartEventType {
			startNodeID = node.ID
			break
		}
	}
	if startNodeID == "" {
		return uuid.Nil, uuid.Nil, errors.New("no start event found")
	}

	// Start transaction
	tx, err := r.db.Beginx()
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	defer tx.Rollback()

	// Create process instance
	instanceID := uuid.New()
	now := time.Now()
	err = r.CreateProcessInstanceTx(tx, instanceID, procDef.ID, now)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	// Create variables
	if len(variables) > 0 {
		err = r.CreateVariablesTx(tx, instanceID, variables, now)
		if err != nil {
			return uuid.Nil, uuid.Nil, err
		}
	}

	// Create execution
	execID := uuid.New()
	err = r.CreateExecutionTx2(tx, execID, instanceID, startNodeID, now)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	if err := tx.Commit(); err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	return instanceID, execID, nil
}
