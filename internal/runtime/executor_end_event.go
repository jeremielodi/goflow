package runtime

import (
	"context"
	"fmt"

	"github.com/jeremielodi/goflow/internal/repository"
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

	return nil
}
