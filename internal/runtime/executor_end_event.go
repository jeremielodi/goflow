package runtime

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

func (r *Runtime) executeEndEvent(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution) error {
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.CompleteProcessInstanceTx(tx, exec.ProcessInstanceID); err != nil {
		return fmt.Errorf("failed to complete process instance: %w", err)
	}
	if err := engineRepo.CompleteExecutionTx(tx, exec.ID); err != nil {
		return fmt.Errorf("failed to complete execution: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	service.LogAuditErr("execution completed", r.auditService.LogExecutionCompleted(exec.ID, exec.ProcessInstanceID, exec.CurrentElementID))
	finalVars, _ := engineRepo.GetProcessVariables(exec.ProcessInstanceID)
	service.LogAuditErr("process completed", r.auditService.LogProcessCompleted(exec.ProcessInstanceID, finalVars))

	// Check if this is a call-activity child: if so, resume the parent execution.
	var parentInstanceID, parentExecutionID *uuid.UUID
	row := r.DB.QueryRow(`
		SELECT parent_instance_id, parent_execution_id
		FROM process_instances WHERE id = $1
	`, exec.ProcessInstanceID)
	row.Scan(&parentInstanceID, &parentExecutionID)

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
	if len(callNode.Outgoing) == 0 {
		return nil
	}
	nextNodeID := callNode.Outgoing[0].TargetRef

	// Advance parent execution to the next node and reactivate it
	_, err = r.DB.Exec(`
		UPDATE executions
		SET current_element_id = $1, status = 'active', is_active = true, updated_at = NOW()
		WHERE id = $2
	`, nextNodeID, *parentExecutionID)
	if err != nil {
		return fmt.Errorf("callActivity end: reactivate parent: %w", err)
	}
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(*parentExecutionID, *parentInstanceID, parentExec.CurrentElementID, nextNodeID, nil))

	// Resume parent
	return ResumeExecution(r.DB, *parentExecutionID, r.dispatcher)
}
