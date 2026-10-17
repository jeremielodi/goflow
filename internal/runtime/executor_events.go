package runtime

import (
	"context"
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

// executeErrorEnd throws node's BPMN error. The nearest enclosing
// sub-process with a matching error boundary event catches it: everything
// inside that sub-process is interrupted and the token leaves through the
// boundary. With no catcher, the token stops and an incident is raised,
// like Camunda 8's unhandled error event.
func (r *Runtime) executeErrorEnd(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph) error {
	boundary, scope := graph.FindErrorCatcher(node.ID, node.ErrorCode)

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if boundary != nil && boundary.Type == engine.EventSubProcessType {
		// Caught by an error event sub-process: its trigger execution
		// interrupts the scope and runs the body.
		if err := engineRepo.CompleteExecutionTx(tx, exec.ProcessInstanceID, exec.ID); err != nil {
			return fmt.Errorf("failed to complete execution: %w", err)
		}
		triggerID, err := engineRepo.CreateErrorTriggerTx(tx, exec.ProcessInstanceID, exec.ID, boundary.ID, scope)
		if err != nil {
			return fmt.Errorf("error end event %s: %w", node.ID, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit error end event: %w", err)
		}
		r.logExecutionCompleted(exec, node.ID)
		return r.ExecuteExecution(ctx, triggerID)
	}

	if boundary == nil {
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

	scopeID, err := engineRepo.FindScopeExecutionTx(tx, exec.ProcessInstanceID, exec.ID, scope)
	if err != nil {
		return fmt.Errorf("error end event %s: %w", node.ID, err)
	}
	tasks, jobs, err := engineRepo.CancelScopeTx(tx, exec.ProcessInstanceID, scopeID, uuid.Nil)
	if err != nil {
		return fmt.Errorf("error end event %s: %w", node.ID, err)
	}
	if err := engineRepo.DisarmBoundaryTriggersTx(tx, exec.ProcessInstanceID, scopeID); err != nil {
		return fmt.Errorf("failed to disarm sub-process boundary events: %w", err)
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ProcessInstanceID, scopeID, "active", boundary.ID); err != nil {
		return fmt.Errorf("failed to move execution to error boundary: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit error end event: %w", err)
	}

	r.logCancelled(exec.ProcessInstanceID, tasks, jobs, "BPMN error "+node.ErrorCode+" thrown by "+node.ID)
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(scopeID, exec.ProcessInstanceID, scope, boundary.ID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(exec.ProcessInstanceID.String(), scopeID.String(), scope, boundary.ID)
	}
	return r.ExecuteExecution(ctx, scopeID)
}

func (r *Runtime) logCancelled(instanceID uuid.UUID, tasks, jobs []uuid.UUID, reason string) {
	for _, id := range tasks {
		service.LogAuditErr("task cancelled", r.auditService.LogTaskCancelled(id, instanceID, reason))
	}
	for _, id := range jobs {
		service.LogAuditErr("job cancelled", r.auditService.LogJobFailed(id, instanceID, "cancelled: "+reason, 0))
	}
}
