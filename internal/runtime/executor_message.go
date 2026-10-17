package runtime

import (
	"context"
	"fmt"

	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// executeMessageCatchEvent parks the execution until a matching message arrives.
func (r *Runtime) executeMessageCatchEvent(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) error {
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("message catch: begin tx: %w", err)
	}
	defer tx.Rollback()

	err = insertMessageSubscriptionTx(tx, exec.ProcessInstanceID, exec.ID, node.MessageRef, r.messageCorrelationKey(node, exec.ProcessInstanceID), "")
	if err != nil {
		return fmt.Errorf("message catch: insert subscription: %w", err)
	}

	_, err = tx.Exec(`
		UPDATE executions SET status = 'waiting', updated_at = NOW() WHERE process_instance_id = $1 AND id = $2
	`, exec.ProcessInstanceID, exec.ID)
	if err != nil {
		return fmt.Errorf("message catch: set waiting: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	service.LogAuditErr("execution waited", r.auditService.LogExecutionWaited(exec.ID, exec.ProcessInstanceID, node.ID, "message"))
	return nil
}
