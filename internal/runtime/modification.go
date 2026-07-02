package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// MoveInstruction moves a running token from one BPMN element to another
// within the same process instance (Camunda 8's "process instance
// modification"). The source execution is cancelled — along with any open
// task/job attached to it — and a fresh execution is created at the target
// element, preserving process variables (they live at the instance level,
// not per-execution, so nothing needs to be copied).
type MoveInstruction struct {
	SourceElementID string
	TargetElementID string
}

// ModifyInstance applies a batch of token moves to a running process
// instance and resumes execution from each newly created token. Returns the
// IDs of the newly created executions.
func (r *Runtime) ModifyInstance(ctx context.Context, instanceID uuid.UUID, moves []MoveInstruction) ([]uuid.UUID, error) {
	engineRepo := repository.NewEngineRepository(r.DB, r.dispatcher)
	instanceRepo := repository.NewProcessInstanceRepository(r.DB)

	var newExecutionIDs []uuid.UUID

	for _, move := range moves {
		if _, ok := r.Graph.Nodes[move.TargetElementID]; !ok {
			return newExecutionIDs, fmt.Errorf("target element %q not found in process graph", move.TargetElementID)
		}

		executions, err := instanceRepo.GetActiveExecutions(instanceID)
		if err != nil {
			return newExecutionIDs, fmt.Errorf("failed to load active executions: %w", err)
		}

		var matched []repository.Execution
		for _, e := range executions {
			if e.CurrentElementID == move.SourceElementID {
				matched = append(matched, e)
			}
		}
		if len(matched) == 0 {
			return newExecutionIDs, fmt.Errorf("no active token found at element %q", move.SourceElementID)
		}

		for _, exec := range matched {
			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return newExecutionIDs, fmt.Errorf("failed to begin transaction: %w", err)
			}

			if err := engineRepo.CompleteExecutionTx(tx, exec.ID); err != nil {
				tx.Rollback()
				return newExecutionIDs, fmt.Errorf("failed to cancel source execution: %w", err)
			}
			// Cancel any open task/job still attached to the cancelled
			// execution so nothing dangling remains reachable once the
			// token has moved elsewhere.
			var cancelledTaskIDs []uuid.UUID
			if err := tx.Select(&cancelledTaskIDs, `
				UPDATE public.tasks SET status = 'cancelled', updated_at = NOW()
				WHERE execution_id = $1 AND status IN ('created', 'claimed')
				RETURNING id
			`, exec.ID); err != nil {
				tx.Rollback()
				return newExecutionIDs, fmt.Errorf("failed to cancel open task: %w", err)
			}
			if _, err := tx.Exec(`
				UPDATE public.jobs SET status = 'cancelled', updated_at = NOW()
				WHERE execution_id = $1 AND status = 'pending'
			`, exec.ID); err != nil {
				tx.Rollback()
				return newExecutionIDs, fmt.Errorf("failed to cancel pending job: %w", err)
			}

			newExecID := uuid.New()
			if err := instanceRepo.CreateExecutionTx(tx, newExecID, instanceID, move.TargetElementID, time.Now()); err != nil {
				tx.Rollback()
				return newExecutionIDs, fmt.Errorf("failed to create execution at target element: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return newExecutionIDs, fmt.Errorf("failed to commit modification: %w", err)
			}

			for _, taskID := range cancelledTaskIDs {
				service.LogAuditErr("task cancelled", r.auditService.LogTaskCancelled(taskID, instanceID, "process instance modification (move token)"))
			}
			service.LogAuditErr("execution created", r.auditService.LogExecutionCreated(newExecID, instanceID, nil, move.TargetElementID))

			newExecutionIDs = append(newExecutionIDs, newExecID)
		}
	}

	for _, execID := range newExecutionIDs {
		if err := r.ExecuteExecution(ctx, execID); err != nil {
			return newExecutionIDs, fmt.Errorf("failed to resume execution %s: %w", execID, err)
		}
	}

	return newExecutionIDs, nil
}
