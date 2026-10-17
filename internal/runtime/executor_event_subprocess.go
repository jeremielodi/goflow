package runtime

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

// Event sub-processes. While a scope (the process, or an embedded
// sub-process) is active, each timer/message/signal event sub-process
// directly inside it is "armed": a trigger execution parks on the event
// sub-process node, owning the timer or subscription. When the event comes,
// the existing timer/message/signal machinery resumes that execution on the
// event sub-process node, and executeEventSubProcess starts the body under
// it — interrupting the rest of the scope first, or re-arming for the next
// occurrence when non-interrupting. Error event sub-processes aren't armed:
// the error-throwing paths create their trigger execution on demand.
//
// Armed triggers are not running work: live-work checks ignore executions
// parked on event sub-process nodes (engine.ProcessGraph.TriggerNodes),
// and closing or cancelling a scope removes them with their timers and
// subscriptions.

// armEventSubProcessesTx arms every timer/message/signal event sub-process
// directly inside scope ("" = the process itself). parentExecID is the
// scope's execution (nil at process level).
func (r *Runtime) armEventSubProcessesTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, graph *engine.ProcessGraph, scope string, instanceID uuid.UUID, parentExecID *uuid.UUID) error {
	for _, n := range graph.Nodes {
		if n.Type != engine.EventSubProcessType || n.ParentScope != scope || n.TriggerType == engine.TriggerError {
			continue
		}
		if err := r.armTriggerTx(tx, engineRepo, n, instanceID, parentExecID); err != nil {
			return fmt.Errorf("arming event sub-process %s: %w", n.ID, err)
		}
	}
	return nil
}

// armTriggerTx parks a new trigger execution on the event sub-process node
// and creates the timer or subscription that will resume it.
func (r *Runtime) armTriggerTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, node *engine.Node, instanceID uuid.UUID, parentExecID *uuid.UUID) error {
	trigger, err := r.createTriggerExecutionTx(tx, engineRepo, node, instanceID, parentExecID, "waiting")
	if err != nil {
		return err
	}
	switch node.TriggerType {
	case engine.TriggerTimer:
		_, err = r.createTimerTx(tx, engineRepo, trigger, node.TimerDefinition, node.ID, "eventSubProcess")
	case engine.TriggerMessage:
		_, err = tx.Exec(`
			INSERT INTO event_subscriptions
			  (process_instance_id, execution_id, event_type, event_name, target_element_id)
			VALUES ($1, $2, 'message', $3, $4)
		`, instanceID, trigger.ID, node.MessageRef, node.ID)
	case engine.TriggerSignal:
		_, err = tx.Exec(`
			INSERT INTO event_subscriptions
			  (process_instance_id, execution_id, event_type, event_name, target_element_id)
			VALUES ($1, $2, 'signal', $3, $4)
		`, instanceID, trigger.ID, node.SignalRef, node.ID)
	default:
		return fmt.Errorf("event sub-process %s has trigger type %q, which can't be armed", node.ID, node.TriggerType)
	}
	return err
}

// createTriggerExecutionTx creates an execution parked on an event
// sub-process node, under the scope's execution.
func (r *Runtime) createTriggerExecutionTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, node *engine.Node, instanceID uuid.UUID, parentExecID *uuid.UUID, status string) (*repository.Execution, error) {
	id := uuid.New()
	pathID := id.String() + "_" + node.ID
	exec := &repository.Execution{
		ID:                id,
		ProcessInstanceID: instanceID,
		ParentExecutionID: parentExecID,
		CurrentElementID:  node.ID,
		Status:            status,
		IsActive:          true,
		PathID:            &pathID,
	}
	if err := engineRepo.CreateExecutionTx(tx, exec); err != nil {
		return nil, fmt.Errorf("failed to create trigger execution: %w", err)
	}
	return exec, nil
}

// executeEventSubProcess runs an event sub-process whose event just came:
// exec is its trigger execution, resumed on the event sub-process node.
func (r *Runtime) executeEventSubProcess(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) error {
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var tasks, jobs []uuid.UUID
	if node.CancelActivity {
		// Interrupting: everything else in the enclosing scope stops.
		if node.ParentScope == "" {
			tasks, jobs, err = engineRepo.TerminateOtherWorkTx(tx, exec.ProcessInstanceID, exec.ID)
		} else if exec.ParentExecutionID != nil {
			tasks, jobs, err = engineRepo.CancelScopeTx(tx, exec.ProcessInstanceID, *exec.ParentExecutionID, exec.ID)
		}
		if err != nil {
			return fmt.Errorf("event sub-process %s: interrupting scope: %w", node.ID, err)
		}
	} else if node.TriggerType == engine.TriggerMessage || node.TriggerType == engine.TriggerSignal {
		// Non-interrupting: stay armed for the next occurrence. (A timer
		// fires once; a cycle reschedules itself on this execution.)
		if err := r.armTriggerTx(tx, engineRepo, node, exec.ProcessInstanceID, exec.ParentExecutionID); err != nil {
			return fmt.Errorf("event sub-process %s: re-arming: %w", node.ID, err)
		}
	}

	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ProcessInstanceID, exec.ID, "waiting", node.ID); err != nil {
		return fmt.Errorf("failed to park trigger execution: %w", err)
	}
	childID := uuid.New()
	pathID := exec.ID.String() + "_" + node.ID
	if err := engineRepo.CreateExecutionTx(tx, &repository.Execution{
		ID:                childID,
		ProcessInstanceID: exec.ProcessInstanceID,
		ParentExecutionID: &exec.ID,
		CurrentElementID:  node.StartNodeID,
		Status:            "active",
		IsActive:          true,
		PathID:            &pathID,
	}); err != nil {
		return fmt.Errorf("failed to create event sub-process execution: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit event sub-process start: %w", err)
	}

	r.logCancelled(exec.ProcessInstanceID, tasks, jobs, "interrupted by event sub-process "+node.ID)
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(childID, exec.ProcessInstanceID, node.ID, node.StartNodeID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(exec.ProcessInstanceID.String(), childID.String(), node.ID, node.StartNodeID)
	}
	return r.ExecuteExecution(ctx, childID)
}

// finishEventSubProcess runs once nothing is left inside a triggered event
// sub-process (scopeID is its trigger execution): it closes it, then ends
// that token in the enclosing scope, which completes the scope — the
// process, or the enclosing sub-process — if nothing else is running
// there. After an interrupting trigger nothing else is.
func (r *Runtime) finishEventSubProcess(ctx context.Context, engineRepo *repository.EngineRepository, tx *sqlx.Tx, instanceID, scopeID uuid.UUID, subNode *engine.Node, graph *engine.ProcessGraph, variables map[string]interface{}) error {
	if err := engineRepo.CloseScopeTx(tx, instanceID, scopeID); err != nil {
		return fmt.Errorf("failed to close event sub-process executions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit event sub-process completion: %w", err)
	}

	trigger := &repository.Execution{ID: scopeID, ProcessInstanceID: instanceID, CurrentElementID: subNode.ID}
	if subNode.ParentScope == "" {
		return r.executeEndEvent(ctx, engineRepo, trigger, graph.TriggerNodes())
	}
	return r.executeSubProcessEnd(ctx, engineRepo, trigger, &engine.Node{ID: subNode.ID, ParentScope: subNode.ParentScope}, graph, variables)
}

// armProcessEventSubProcesses arms the process-level event sub-processes,
// once, when the process's start event runs.
func (r *Runtime) armProcessEventSubProcesses(ctx context.Context, engineRepo *repository.EngineRepository, graph *engine.ProcessGraph, exec *repository.Execution) error {
	needed := false
	for _, n := range graph.Nodes {
		if n.Type == engine.EventSubProcessType && n.ParentScope == "" && n.TriggerType != engine.TriggerError {
			needed = true
			break
		}
	}
	if !needed {
		return nil
	}
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	if err := r.armEventSubProcessesTx(tx, engineRepo, graph, "", exec.ProcessInstanceID, nil); err != nil {
		return err
	}
	return tx.Commit()
}
