package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
)

func (r *Runtime) executeServiceTask(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, variables map[string]interface{}) error {
	if node.MultiInstance != nil && exec.ParentExecutionID == nil {
		return r.handleMultiInstance(ctx, exec, node, variables)
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ID, "active", node.ID); err != nil {
		return fmt.Errorf("failed to update execution: %w", err)
	}

	if node.JobType == nil {
		return fmt.Errorf("service task %s has no job type", node.ID)
	}

	payload, err := json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("failed to marshal variables: %w", err)
	}

	jobID := uuid.New()
	if err := engineRepo.CreateJobTx(tx, jobID, exec.ProcessInstanceID, exec.ID, *node.JobType, payload); err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}

	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting"); err != nil {
		return fmt.Errorf("failed to update execution status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	r.auditService.LogJobCreated(exec.ProcessInstanceID, jobID, *node.JobType)
	return nil
}
