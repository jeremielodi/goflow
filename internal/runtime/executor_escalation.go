package runtime

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// throwEscalation throws node's escalation from exec and runs whatever
// catches it (see engine.ProcessGraph.FindEscalationCatcher) — here or,
// through call activities, in a calling process. It reports whether the
// throwing token carries on: yes when nothing caught the escalation
// (unlike an error, an uncaught escalation is harmless) or when a
// non-interrupting catcher did; no when an interrupting catcher stopped
// the scope the token was in.
func (r *Runtime) throwEscalation(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph) (bool, error) {
	catcher, scope := graph.FindEscalationCatcher(node.ID, node.EscalationCode)
	if catcher == nil {
		carryOn, err := r.propagateEscalationToCaller(ctx, exec.ProcessInstanceID, node.EscalationCode)
		if err == nil && carryOn {
			log.Printf("escalation %q thrown by %s: continuing", node.EscalationCode, node.ID)
		}
		return carryOn, err
	}
	return r.catchEscalation(ctx, engineRepo, graph, exec, node.ID, catcher, scope, node.EscalationCode)
}

// propagateEscalationToCaller hands an escalation nothing in instanceID
// catches to the call activity that started it, and on up, as in
// Camunda 8. It reports whether the thrower carries on: yes if nobody
// catches it or a non-interrupting catcher does; no if an interrupting one
// does (which also terminates the called instance, see logCancelled).
func (r *Runtime) propagateEscalationToCaller(ctx context.Context, instanceID uuid.UUID, code string) (bool, error) {
	caller := service.CallerOf(r.DB, instanceID)
	if caller == nil {
		return true, nil
	}
	catcher, scope := caller.Graph.FindEscalationCatcher(caller.Node.ID, code)
	if catcher == nil {
		return r.propagateEscalationToCaller(ctx, caller.InstanceID, code)
	}
	engineRepo := repository.NewEngineRepository(r.DB, r.dispatcher)
	callExec, err := engineRepo.GetExecutionByID(caller.ExecID)
	if err != nil || callExec == nil {
		return false, fmt.Errorf("call activity execution %s: %v", caller.ExecID, err)
	}
	parentRT := NewRuntime(caller.Graph, r.DB, r.dispatcher)
	return parentRT.catchEscalation(ctx, engineRepo, caller.Graph, callExec, caller.Node.ID, catcher, scope, code)
}

// catchEscalation runs catcher for an escalation thrown by thrower (at
// element throwerID): an escalation event sub-process, a boundary event
// on the throwing activity itself (a call activity whose called process
// escalated) or on an enclosing sub-process. It reports whether the
// thrower carries on.
func (r *Runtime) catchEscalation(ctx context.Context, engineRepo *repository.EngineRepository, graph *engine.ProcessGraph, thrower *repository.Execution, throwerID string, catcher *engine.Node, scope string, code string) (bool, error) {
	instanceID := thrower.ProcessInstanceID
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if catcher.Type == engine.EventSubProcessType {
		// The trigger execution interrupts the scope (or not) and runs the
		// event sub-process body.
		triggerID, err := engineRepo.CreateErrorTriggerTx(tx, instanceID, thrower.ID, catcher.ID, scope)
		if err != nil {
			return false, fmt.Errorf("escalation %q: %w", code, err)
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("failed to commit escalation: %w", err)
		}
		if err := r.ExecuteExecution(ctx, triggerID); err != nil {
			return false, err
		}
		if catcher.CancelActivity {
			r.logCancelled(instanceID, nil, nil, "escalation "+code)
		}
		return !catcher.CancelActivity, nil
	}

	// The execution of the activity the boundary event is attached to.
	scopeID, fromNode := thrower.ID, throwerID
	if catcher.AttachedToRef != throwerID {
		if scopeID, err = engineRepo.FindScopeExecutionTx(tx, instanceID, thrower.ID, scope); err != nil {
			return false, fmt.Errorf("escalation %q: %w", code, err)
		}
		fromNode = scope
	}

	if !catcher.CancelActivity {
		scopeExec, err := engineRepo.LockActivityExecutionTx(tx, instanceID, scopeID)
		if err != nil {
			return false, fmt.Errorf("escalation %q: %w", code, err)
		}
		tokenID := uuid.New()
		pathID := tokenID.String()
		if err := engineRepo.CreateExecutionTx(tx, &repository.Execution{
			ID:                tokenID,
			ProcessInstanceID: instanceID,
			ParentExecutionID: scopeExec.ParentExecutionID,
			CurrentElementID:  catcher.ID,
			Status:            "active",
			IsActive:          true,
			PathID:            &pathID,
		}); err != nil {
			return false, fmt.Errorf("failed to create escalation token: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("failed to commit escalation: %w", err)
		}
		if err := r.ExecuteExecution(ctx, tokenID); err != nil {
			return false, err
		}
		return true, nil
	}

	tasks, jobs, err := engineRepo.CancelActivityWorkTx(tx, instanceID, scopeID)
	if err != nil {
		return false, fmt.Errorf("escalation %q: %w", code, err)
	}
	scopeTasks, scopeJobs, err := engineRepo.CancelScopeTx(tx, instanceID, scopeID, uuid.Nil)
	if err != nil {
		return false, fmt.Errorf("escalation %q: %w", code, err)
	}
	if err := engineRepo.DisarmBoundaryTriggersTx(tx, instanceID, scopeID); err != nil {
		return false, fmt.Errorf("failed to disarm boundary events: %w", err)
	}
	if err := repository.ClearLocalVariablesTx(tx, instanceID, scopeID); err != nil {
		return false, err
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, instanceID, scopeID, "active", catcher.ID); err != nil {
		return false, fmt.Errorf("failed to move execution to escalation boundary: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit escalation: %w", err)
	}

	r.logCancelled(instanceID, append(tasks, scopeTasks...), append(jobs, scopeJobs...), "escalation "+code+" thrown by "+throwerID)
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(scopeID, instanceID, fromNode, catcher.ID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(instanceID.String(), scopeID.String(), fromNode, catcher.ID)
	}
	if err := r.ExecuteExecution(ctx, scopeID); err != nil {
		return false, err
	}
	return false, nil
}
