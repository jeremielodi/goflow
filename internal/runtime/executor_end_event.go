package runtime

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// executeEndEvent ends a token at an end event outside any sub-process. The
// process instance completes only once no other token is still running
// (BPMN's implicit end): a parallel branch that ends on its own end event
// must not cut off a sibling branch that is still working.
func (r *Runtime) executeEndEvent(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, ignoreNodes map[string]bool) error {
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Tokens reaching end events at the same moment take turns, so exactly
	// one of them sees that nothing else is running.
	if err := engineRepo.LockProcessInstanceTx(tx, exec.ProcessInstanceID); err != nil {
		return fmt.Errorf("failed to lock process instance: %w", err)
	}
	if err := engineRepo.CompleteExecutionTx(tx, exec.ProcessInstanceID, exec.ID); err != nil {
		return fmt.Errorf("failed to complete execution: %w", err)
	}
	live, err := engineRepo.HasLiveWorkInInstanceTx(tx, exec.ProcessInstanceID, ignoreNodes)
	if err != nil {
		return fmt.Errorf("failed to check remaining tokens: %w", err)
	}
	if live {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		r.logExecutionCompleted(exec, exec.CurrentElementID)
		return nil
	}
	if err := engineRepo.CompleteProcessInstanceTx(tx, exec.ProcessInstanceID); err != nil {
		return fmt.Errorf("failed to complete process instance: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	service.LogAuditErr("execution completed", r.auditService.LogExecutionCompleted(exec.ID, exec.ProcessInstanceID, exec.CurrentElementID))
	finalVars, _ := engineRepo.GetProcessVariables(exec.ProcessInstanceID)
	service.LogAuditErr("process completed", r.auditService.LogProcessCompleted(exec.ProcessInstanceID, finalVars))

	if p := eventlog.Global(); p != nil {
		p.ExecutionCompleted(exec.ProcessInstanceID.String(), exec.ID.String(), exec.CurrentElementID)
		p.ProcessCompleted(exec.ProcessInstanceID.String())
	}

	// Check if this is a call-activity child: if so, resume the parent
	// execution. Must happen BEFORE archiving below — once archived, this
	// instance's process_instances row (and thus its parent_instance_id/
	// parent_execution_id columns) no longer exists.
	var parentInstanceID, parentExecutionID *uuid.UUID
	row := r.DB.QueryRow(`
		SELECT parent_instance_id, parent_execution_id
		FROM process_instances WHERE id = $1
	`, exec.ProcessInstanceID)
	row.Scan(&parentInstanceID, &parentExecutionID)
	// A called process's variables, read before archiving removes them.
	var childVars map[string]interface{}
	if parentExecutionID != nil {
		childVars, _ = engineRepo.GetProcessVariables(exec.ProcessInstanceID)
	}

	// Archive this instance into historic_* tables and remove its live
	// rows — see internal/service/archive_service.go. Best-effort: a
	// failed archive leaves the instance live (already safely marked
	// completed above), never risking data loss, so it's logged but not
	// treated as fatal to process completion.
	if err := service.ArchiveAndDeleteProcessInstance(r.DB, exec.ProcessInstanceID, nil); err != nil {
		fmt.Printf("Warning: failed to archive process instance %s: %v\n", exec.ProcessInstanceID, err)
	}

	if parentExecutionID == nil {
		return nil
	}

	// Load the parent execution to find the call activity node
	parentExec, err := engineRepo.GetExecutionByID(*parentExecutionID)
	if err != nil || parentExec == nil {
		return nil // parent gone — ignore
	}

	// Load the parent's process graph
	parentGraph, err := engineRepo.GetProcessGraphByInstanceID(*parentInstanceID)
	if err != nil {
		return nil
	}

	// Find the next node after the call activity
	callNode, ok := parentGraph.Nodes[parentExec.CurrentElementID]
	if !ok || callNode.Type != engine.CallActivityType {
		return nil
	}
	// Without an outgoing flow the call activity's token ends in its scope.
	nextNodeID, err := common.ResolveNext(callNode, nil)
	if err != nil {
		return fmt.Errorf("callActivity end: %w", err)
	}

	// Advance parent execution to the next node and reactivate it, disarming
	// the call activity's boundary events as it leaves.
	parentTx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("callActivity end: begin tx: %w", err)
	}
	defer parentTx.Rollback()
	if err := engineRepo.DisarmBoundaryTriggersTx(parentTx, *parentInstanceID, *parentExecutionID); err != nil {
		return fmt.Errorf("callActivity end: disarm boundary events: %w", err)
	}
	// The child's variables come back as a job's would (scopes, output
	// mappings): all of them with propagateAllChildVariables, or only what
	// the output mappings take from them.
	handBack := map[string]interface{}{}
	if callNode.PropagateAllChildVariables || len(callNode.OutputMappings) > 0 {
		for k, v := range childVars {
			handBack[k] = v
		}
	}
	// A multi-instance call activity's loop variables belong to the loop:
	// the child only got copies (a stale outputCollection among them, which
	// would overwrite what other iterations collected).
	if mi := callNode.MultiInstance; mi != nil {
		delete(handBack, mi.Output())
		delete(handBack, mi.Element())
		delete(handBack, "loopCounter")
	}
	if err := service.ApplyActivityOutputTx(parentTx, *parentInstanceID, *parentExecutionID, callNode, handBack); err != nil {
		return fmt.Errorf("callActivity end: variables: %w", err)
	}
	if callNode.CompensationHandler != "" {
		if err := engineRepo.RecordCompensableTx(parentTx, *parentInstanceID, *parentExecutionID, callNode.ID, callNode.CompensationHandler); err != nil {
			return fmt.Errorf("callActivity end: record compensation: %w", err)
		}
	}
	if _, err = parentTx.Exec(`
		UPDATE executions
		SET current_element_id = $1, status = 'active', is_active = true, updated_at = NOW()
		WHERE process_instance_id = $2 AND id = $3
	`, nextNodeID, *parentInstanceID, *parentExecutionID); err != nil {
		return fmt.Errorf("callActivity end: reactivate parent: %w", err)
	}
	if err := parentTx.Commit(); err != nil {
		return fmt.Errorf("callActivity end: commit: %w", err)
	}
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(*parentExecutionID, *parentInstanceID, parentExec.CurrentElementID, nextNodeID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(parentInstanceID.String(), parentExecutionID.String(), parentExec.CurrentElementID, nextNodeID)
	}

	// Resume parent
	return ResumeExecution(r.DB, *parentExecutionID, r.dispatcher)
}
