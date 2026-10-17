package runtime

import (
	"context"
	"fmt"

	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// executeBoundaryTimerNode is called when a timer fires and execution reaches the BoundaryTimerEvent node.
// It simply advances to the node's first outgoing flow. Returns the next node ID.
func (r *Runtime) executeBoundaryTimerNode(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) (string, error) {
	if len(node.Outgoing) == 0 {
		return "", fmt.Errorf("boundary timer event %s has no outgoing flow", node.ID)
	}
	nextNodeID := node.Outgoing[0].TargetRef

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.UpdateExecutionNodeTx(tx, exec.ProcessInstanceID, exec.ID, nextNodeID); err != nil {
		return "", fmt.Errorf("failed to update execution node: %w", err)
	}
	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ProcessInstanceID, exec.ID, "active"); err != nil {
		return "", fmt.Errorf("failed to update execution status: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(exec.ID, exec.ProcessInstanceID, node.ID, nextNodeID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(exec.ProcessInstanceID.String(), exec.ID.String(), node.ID, nextNodeID)
	}

	return nextNodeID, nil
}
// executeIntermediateTimer handles an intermediate catch event (timer). Creates a timer job
// and parks the execution in "waiting" state until the timer fires.
func (r *Runtime) executeIntermediateTimer(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) error {
	if node.TimerDefinition == "" {
		return nil
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var nextNodeID string
	if len(node.Outgoing) > 0 {
		nextNodeID = node.Outgoing[0].TargetRef
	}
	if _, err := r.createTimerTx(tx, engineRepo, exec, node.TimerDefinition, nextNodeID, "intermediate"); err != nil {
		return fmt.Errorf("failed to create timer: %w", err)
	}

	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ProcessInstanceID, exec.ID, "waiting"); err != nil {
		return fmt.Errorf("failed to update execution status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit intermediate timer transaction: %w", err)
	}

	service.LogAuditErr("execution waited", r.auditService.LogExecutionWaited(exec.ID, exec.ProcessInstanceID, node.ID, "timer"))
	return nil
}
