package runtime

import (
	"context"
	"fmt"

	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
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
		UPDATE executions SET status = 'waiting', updated_at = NOW() WHERE process_instance_id = $1 AND id = $2
	`, exec.ProcessInstanceID, exec.ID)
	if err != nil {
		return fmt.Errorf("signal catch: set waiting: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	service.LogAuditErr("execution waited", r.auditService.LogExecutionWaited(exec.ID, exec.ProcessInstanceID, node.ID, "signal"))
	return nil
}
