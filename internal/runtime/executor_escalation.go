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
// catches it (see engine.ProcessGraph.FindEscalationCatcher). It reports
// whether the throwing token carries on: yes when nothing caught the
// escalation (unlike an error, an uncaught escalation is harmless) or when
// a non-interrupting catcher did; no when an interrupting catcher stopped
// the scope the token was in.
func (r *Runtime) throwEscalation(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph) (bool, error) {
	catcher, scope := graph.FindEscalationCatcher(node.ID, node.EscalationCode)
	if catcher == nil {
		log.Printf("escalation %q thrown by %s was not caught — continuing", node.EscalationCode, node.ID)
		return true, nil
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if catcher.Type == engine.EventSubProcessType {
		// The trigger execution interrupts the scope (or not) and runs the
		// event sub-process body.
		triggerID, err := engineRepo.CreateErrorTriggerTx(tx, exec.ProcessInstanceID, exec.ID, catcher.ID, scope)
		if err != nil {
			return false, fmt.Errorf("escalation %q: %w", node.EscalationCode, err)
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("failed to commit escalation: %w", err)
		}
		if err := r.ExecuteExecution(ctx, triggerID); err != nil {
			return false, err
		}
		return !catcher.CancelActivity, nil
	}

	// A boundary event on the enclosing sub-process scope.
	scopeID, err := engineRepo.FindScopeExecutionTx(tx, exec.ProcessInstanceID, exec.ID, scope)
	if err != nil {
		return false, fmt.Errorf("escalation %q: %w", node.EscalationCode, err)
	}

	if !catcher.CancelActivity {
		scopeExec, err := engineRepo.LockActivityExecutionTx(tx, exec.ProcessInstanceID, scopeID)
		if err != nil {
			return false, fmt.Errorf("escalation %q: %w", node.EscalationCode, err)
		}
		tokenID := uuid.New()
		pathID := tokenID.String()
		if err := engineRepo.CreateExecutionTx(tx, &repository.Execution{
			ID:                tokenID,
			ProcessInstanceID: exec.ProcessInstanceID,
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

	tasks, jobs, err := engineRepo.CancelScopeTx(tx, exec.ProcessInstanceID, scopeID, uuid.Nil)
	if err != nil {
		return false, fmt.Errorf("escalation %q: %w", node.EscalationCode, err)
	}
	if err := engineRepo.DisarmBoundaryTriggersTx(tx, exec.ProcessInstanceID, scopeID); err != nil {
		return false, fmt.Errorf("failed to disarm sub-process boundary events: %w", err)
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ProcessInstanceID, scopeID, "active", catcher.ID); err != nil {
		return false, fmt.Errorf("failed to move execution to escalation boundary: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit escalation: %w", err)
	}

	r.logCancelled(exec.ProcessInstanceID, tasks, jobs, "escalation "+node.EscalationCode+" thrown by "+node.ID)
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(scopeID, exec.ProcessInstanceID, scope, catcher.ID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(exec.ProcessInstanceID.String(), scopeID.String(), scope, catcher.ID)
	}
	if err := r.ExecuteExecution(ctx, scopeID); err != nil {
		return false, err
	}
	return false, nil
}
