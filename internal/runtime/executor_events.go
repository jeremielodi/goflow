package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// advance moves the execution along node's outgoing flow without waiting,
// for pass-through elements (manual tasks, none/link throw events, link
// catch events, signal throw events once broadcast). Returns the next node.
func (r *Runtime) advance(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, variables map[string]interface{}) (string, error) {
	next, err := common.ResolveNext(node, variables)
	if err != nil {
		return "", err
	}
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	// A step that doesn't wait: the next wait state's durable commit
	// carries it (see repository.NoSyncTx).
	if err := repository.NoSyncTx(tx); err != nil {
		return "", err
	}
	if err := engineRepo.UpdateExecutionNodeTx(tx, exec.ProcessInstanceID, exec.ID, next); err != nil {
		return "", fmt.Errorf("failed to update execution node: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(exec.ID, exec.ProcessInstanceID, node.ID, next, variables))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(exec.ProcessInstanceID.String(), exec.ID.String(), node.ID, next)
	}
	return next, nil
}

// broadcastSignal throws node's signal to every process waiting for it.
// A failure to resume some waiting process is logged, not fatal to the
// thrower: broadcast is fire-and-forget in BPMN.
func (r *Runtime) broadcastSignal(node *engine.Node) {
	if node.SignalRef == "" {
		log.Printf("signal event %s has no signal name — nothing broadcast", node.ID)
		return
	}
	resumed, errs, err := BroadcastSignal(r.DB, r.dispatcher, node.SignalRef, nil)
	if err != nil {
		log.Printf("signal event %s: broadcasting %q failed: %v", node.ID, node.SignalRef, err)
		return
	}
	for _, e := range errs {
		log.Printf("signal event %s: broadcasting %q: %s", node.ID, node.SignalRef, e)
	}
	log.Printf("📡 signal %q thrown by %s resumed %d execution(s)", node.SignalRef, node.ID, resumed)
}

// executeEnd ends the token at an end event: the whole process outside any
// sub-process, otherwise only its sub-process (see executeSubProcessEnd).
func (r *Runtime) executeEnd(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph, variables map[string]interface{}) error {
	if node.ParentScope != "" {
		return r.executeSubProcessEnd(ctx, engineRepo, exec, node, graph, variables)
	}
	return r.executeEndEvent(ctx, engineRepo, exec, graph.TriggerNodes())
}

// executeTerminateEnd stops every other token of its scope, then ends the
// scope: the whole process instance outside any sub-process, otherwise the
// enclosing sub-process, which then continues on its outgoing flow.
func (r *Runtime) executeTerminateEnd(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph, variables map[string]interface{}) error {
	if node.ParentScope == "" {
		tx, err := r.DB.BeginTxx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback()
		tasks, jobs, err := engineRepo.TerminateOtherWorkTx(tx, exec.ProcessInstanceID, exec.ID)
		if err != nil {
			return fmt.Errorf("terminate end event %s: %w", node.ID, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit terminate end event: %w", err)
		}
		r.logCancelled(exec.ProcessInstanceID, tasks, jobs, "terminate end event "+node.ID)
		return r.executeEndEvent(ctx, engineRepo, exec, graph.TriggerNodes())
	}

	subNode, ok := graph.Nodes[node.ParentScope]
	if !ok {
		return fmt.Errorf("terminate end event %s: sub-process %s not found", node.ID, node.ParentScope)
	}
	next, err := common.ResolveNext(subNode, variables)
	if err != nil {
		return fmt.Errorf("leaving sub-process %s: %w", subNode.ID, err)
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	scopeID, err := engineRepo.FindScopeExecutionTx(tx, exec.ProcessInstanceID, exec.ID, subNode.ID)
	if err != nil {
		return fmt.Errorf("terminate end event %s: %w", node.ID, err)
	}
	tasks, jobs, err := engineRepo.CancelScopeTx(tx, exec.ProcessInstanceID, scopeID, uuid.Nil)
	if err != nil {
		return fmt.Errorf("terminate end event %s: %w", node.ID, err)
	}
	if err := engineRepo.DisarmBoundaryTriggersTx(tx, exec.ProcessInstanceID, scopeID); err != nil {
		return fmt.Errorf("failed to disarm sub-process boundary events: %w", err)
	}
	if err := leaveScopeVariablesTx(tx, exec.ProcessInstanceID, scopeID, subNode); err != nil {
		return fmt.Errorf("leaving sub-process %s: %w", subNode.ID, err)
	}
	// The sub-process completed: it can now be compensated.
	if subNode.CompensationHandler != "" {
		if err := engineRepo.RecordCompensableTx(tx, exec.ProcessInstanceID, scopeID, subNode.ID, subNode.CompensationHandler); err != nil {
			return fmt.Errorf("failed to record compensation: %w", err)
		}
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ProcessInstanceID, scopeID, "active", next); err != nil {
		return fmt.Errorf("failed to resume execution after sub-process: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit terminate end event: %w", err)
	}

	r.logCancelled(exec.ProcessInstanceID, tasks, jobs, "terminate end event "+node.ID)
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(scopeID, exec.ProcessInstanceID, subNode.ID, next, variables))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(exec.ProcessInstanceID.String(), scopeID.String(), subNode.ID, next)
	}
	return r.ExecuteExecution(ctx, scopeID)
}

// executeErrorEnd throws node's BPMN error. The nearest catcher — an error
// boundary event or error event sub-process, here or (through call
// activities) in a calling process — handles it. With no catcher, the
// token stops and an incident is raised, like Camunda 8's unhandled error
// event.
func (r *Runtime) executeErrorEnd(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph) error {
	if catcher, scope := graph.FindErrorCatcher(node.ID, node.ErrorCode); catcher != nil {
		return r.catchError(ctx, engineRepo, graph, exec, node.ID, catcher, scope, "BPMN error "+node.ErrorCode+" thrown by "+node.ID)
	}
	if handled, err := r.PropagateErrorToCaller(ctx, exec.ProcessInstanceID, node.ErrorCode); handled || err != nil {
		return err
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()
	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ProcessInstanceID, exec.ID, "waiting"); err != nil {
		return fmt.Errorf("failed to park execution: %w", err)
	}
	if _, err := repository.NewIncidentRepository(r.DB).CreateIncidentTx(tx, &models.IncidentCreate{
		ProcessInstanceID: exec.ProcessInstanceID,
		IncidentType:      "unhandledErrorEvent",
		ActivityID:        node.ID,
		ErrorMessage:      fmt.Sprintf("BPMN error %q thrown by end event %s was not caught by any error boundary event", node.ErrorCode, node.ID),
		ErrorCode:         node.ErrorCode,
	}); err != nil {
		return fmt.Errorf("failed to create incident: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit unhandled error: %w", err)
	}
	log.Printf("⚠️ unhandled BPMN error %q at %s (instance %s) — incident raised", node.ErrorCode, node.ID, exec.ProcessInstanceID)
	return nil
}

// catchError hands a BPMN error thrown by thrower (at element throwerID)
// to catcher, found by engine.ProcessGraph.FindErrorCatcher in scope:
//
//   - an error event sub-process: its trigger execution interrupts the
//     scope and runs the body;
//   - an error boundary event on the throwing activity itself (scope ""
//     and the boundary attached to throwerID — a call activity whose
//     called process threw): the activity's execution leaves through it;
//   - an error boundary event on an enclosing sub-process: everything in
//     it is interrupted and its execution leaves through the boundary.
func (r *Runtime) catchError(ctx context.Context, engineRepo *repository.EngineRepository, graph *engine.ProcessGraph, thrower *repository.Execution, throwerID string, catcher *engine.Node, scope string, reason string) error {
	instanceID := thrower.ProcessInstanceID
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if catcher.Type == engine.EventSubProcessType {
		if err := engineRepo.DisarmBoundaryTriggersTx(tx, instanceID, thrower.ID); err != nil {
			return err
		}
		if err := engineRepo.CompleteExecutionTx(tx, instanceID, thrower.ID); err != nil {
			return fmt.Errorf("failed to complete execution: %w", err)
		}
		triggerID, err := engineRepo.CreateErrorTriggerTx(tx, instanceID, thrower.ID, catcher.ID, scope)
		if err != nil {
			return fmt.Errorf("%s: %w", reason, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit caught error: %w", err)
		}
		r.logExecutionCompleted(thrower, throwerID)
		return r.ExecuteExecution(ctx, triggerID)
	}

	catchingID, fromNode := thrower.ID, throwerID
	var tasks, jobs []uuid.UUID
	if catcher.AttachedToRef != throwerID {
		if catchingID, err = engineRepo.FindScopeExecutionTx(tx, instanceID, thrower.ID, scope); err != nil {
			return fmt.Errorf("%s: %w", reason, err)
		}
		fromNode = scope
		if tasks, jobs, err = engineRepo.CancelScopeTx(tx, instanceID, catchingID, uuid.Nil); err != nil {
			return fmt.Errorf("%s: %w", reason, err)
		}
	}
	if err := engineRepo.DisarmBoundaryTriggersTx(tx, instanceID, catchingID); err != nil {
		return fmt.Errorf("failed to disarm boundary events: %w", err)
	}
	if err := repository.ClearLocalVariablesTx(tx, instanceID, catchingID); err != nil {
		return err
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, instanceID, catchingID, "active", catcher.ID); err != nil {
		return fmt.Errorf("failed to move execution to error boundary: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit caught error: %w", err)
	}

	r.logCancelled(instanceID, tasks, jobs, reason)
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(catchingID, instanceID, fromNode, catcher.ID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(instanceID.String(), catchingID.String(), fromNode, catcher.ID)
	}
	return r.ExecuteExecution(ctx, catchingID)
}

// PropagateErrorToCaller hands BPMN error errorCode, which nothing in
// instanceID catches, to the call activity that started it — and on up
// the chain of calling processes — as in Camunda 8: the called instance
// is terminated and the error is caught as if the call activity had
// thrown it. False when no caller catches it.
func (r *Runtime) PropagateErrorToCaller(ctx context.Context, instanceID uuid.UUID, errorCode string) (bool, error) {
	caller := service.CallerOf(r.DB, instanceID)
	if caller == nil {
		return false, nil
	}
	catcher, scope := caller.Graph.FindErrorCatcher(caller.Node.ID, errorCode)
	if catcher == nil {
		// Not here: maybe further up. If so, the caller's own instance gets
		// terminated by its caller, taking this one with it.
		if !service.CallerCatchesError(r.DB, caller.InstanceID, errorCode) {
			return false, nil
		}
		return r.PropagateErrorToCaller(ctx, caller.InstanceID, errorCode)
	}

	reason := fmt.Sprintf("BPMN error %s propagated to call activity %s", errorCode, caller.Node.ID)
	if err := service.TerminateProcessInstance(r.DB, r.dispatcher, instanceID, reason); err != nil && !errors.Is(err, service.ErrProcessInstanceNotFound) {
		return true, fmt.Errorf("terminating the called instance: %w", err)
	}
	engineRepo := repository.NewEngineRepository(r.DB, r.dispatcher)
	callExec, err := engineRepo.GetExecutionByID(caller.ExecID)
	if err != nil || callExec == nil {
		return true, fmt.Errorf("call activity execution %s: %v", caller.ExecID, err)
	}
	parentRT := NewRuntime(caller.Graph, r.DB, r.dispatcher)
	return true, parentRT.catchError(ctx, engineRepo, caller.Graph, callExec, caller.Node.ID, catcher, scope, reason)
}

// logCancelled records what an interruption cancelled. It also
// terminates the processes started by call activities the interruption
// left (see service.TerminateOrphanedCalledInstances).
func (r *Runtime) logCancelled(instanceID uuid.UUID, tasks, jobs []uuid.UUID, reason string) {
	service.TerminateOrphanedCalledInstances(r.DB, r.dispatcher, &instanceID)
	for _, id := range tasks {
		service.LogAuditErr("task cancelled", r.auditService.LogTaskCancelled(id, instanceID, reason))
	}
	for _, id := range jobs {
		service.LogAuditErr("job cancelled", r.auditService.LogJobFailed(id, instanceID, "cancelled: "+reason, 0))
	}
}
