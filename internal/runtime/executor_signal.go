package runtime

import (
	"context"
	"fmt"

	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
)

// executeSignalCatchEvent parks the execution until a matching signal is broadcast.
func (r *Runtime) executeSignalCatchEvent(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) error {
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("signal catch: begin tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO event_subscriptions
		  (process_instance_id, execution_id, event_type, event_name)
		VALUES ($1, $2, 'signal', $3)
	`, exec.ProcessInstanceID, exec.ID, node.SignalRef)
	if err != nil {
		return fmt.Errorf("signal catch: insert subscription: %w", err)
	}

	_, err = tx.Exec(`
		UPDATE executions SET status = 'waiting', updated_at = NOW() WHERE id = $1
	`, exec.ID)
	if err != nil {
		return fmt.Errorf("signal catch: set waiting: %w", err)
	}

	return tx.Commit()
}

// executeSignalBoundaryEvent advances the execution to the boundary event's outgoing node.
func (r *Runtime) executeSignalBoundaryEvent(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) error {
	if len(node.Outgoing) == 0 {
		return fmt.Errorf("signal boundary event %s has no outgoing flow", node.ID)
	}
	next := node.Outgoing[0].TargetRef

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("signal boundary: begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.UpdateExecutionNodeTx(tx, exec.ID, next); err != nil {
		return fmt.Errorf("signal boundary: update node: %w", err)
	}
	return tx.Commit()
}
