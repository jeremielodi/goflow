package runtime

import (
	"context"
	"fmt"

	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
)

// executeErrorBoundaryNode is called when an error boundary event fires.
// It advances execution to the first outgoing node (the error-handling path).
func (r *Runtime) executeErrorBoundaryNode(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) (string, error) {
	if len(node.Outgoing) == 0 {
		return "", fmt.Errorf("error boundary event %s has no outgoing flow", node.ID)
	}
	nextNodeID := node.Outgoing[0].TargetRef

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.UpdateExecutionNodeTx(tx, exec.ID, nextNodeID); err != nil {
		return "", fmt.Errorf("failed to update execution node: %w", err)
	}
	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "active"); err != nil {
		return "", fmt.Errorf("failed to update execution status: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit error boundary transaction: %w", err)
	}

	return nextNodeID, nil
}
