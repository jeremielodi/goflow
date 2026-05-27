// runtime/parallel_gateway_executor.go

package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type ParallelGatewayExecutor struct {
	db         *sqlx.DB
	engineRepo *repository.EngineRepository
}

func NewParallelGatewayExecutor(db *sqlx.DB) *ParallelGatewayExecutor {
	return &ParallelGatewayExecutor{
		db:         db,
		engineRepo: repository.NewEngineRepository(db),
	}
}

// ExecuteFork handles the fork side of a parallel gateway
func (p *ParallelGatewayExecutor) ExecuteFork(
	ctx context.Context,
	execID uuid.UUID,
	gatewayNode *engine.Node,
	variables map[string]interface{},
) error {
	// Get current execution
	exec, err := p.engineRepo.GetActiveExecution(ctx, execID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}
	if exec == nil {
		return fmt.Errorf("execution not found: %s", execID)
	}

	if len(gatewayNode.Outgoing) == 0 {
		return fmt.Errorf("parallel gateway has no outgoing flows")
	}

	tx, err := p.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create child executions for each outgoing flow
	for i, flow := range gatewayNode.Outgoing {
		childExecID := uuid.New()
		pathID := fmt.Sprintf("%s_%d", exec.ID.String(), i)

		// Create branch variables (copy or reference)
		branchVars, _ := json.Marshal(variables)

		// Insert child execution
		err = p.engineRepo.CreateExecutionTx(tx, &repository.Execution{
			ID:                childExecID,
			ProcessInstanceID: exec.ProcessInstanceID,
			ParentExecutionID: &exec.ID,
			CurrentElementID:  flow.TargetRef,
			Status:            "active",
			IsActive:          true,
			PathID:            pathID,
		})
		if err != nil {
			return fmt.Errorf("failed to create child execution: %w", err)
		}

		// Store branch variables
		_, err = tx.Exec(`
            INSERT INTO public.execution_variables (execution_id, variables, created_at)
            VALUES ($1, $2, NOW())
        `, childExecID, branchVars)
		if err != nil {
			return fmt.Errorf("failed to store branch variables: %w", err)
		}
	}

	// Mark parent execution as waiting (it will be resumed after all children complete)
	err = p.engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting")
	if err != nil {
		return fmt.Errorf("failed to update parent execution: %w", err)
	}

	fmt.Printf("Parallel fork at gateway %s: created %d child executions\n",
		gatewayNode.ID, len(gatewayNode.Outgoing))

	return tx.Commit()
}

// ExecuteJoin handles the join side of a parallel gateway
func (p *ParallelGatewayExecutor) ExecuteJoin(
	ctx context.Context,
	execID uuid.UUID,
	gatewayNode *engine.Node,
) error {
	// Get current execution
	exec, err := p.engineRepo.GetActiveExecution(ctx, execID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}
	if exec == nil {
		return fmt.Errorf("execution not found: %s", execID)
	}

	if exec.ParentExecutionID == nil {
		return fmt.Errorf("join gateway called on execution with no parent")
	}

	tx, err := p.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Find the gateway state
	var gatewayState models.GatewayState
	err = tx.Get(&gatewayState, `
        SELECT id, expected_incoming, received_incoming, status, joined_flows
        FROM public.gateway_state
        WHERE process_instance_id = $1 AND gateway_id = $2 AND status = 'waiting'
        FOR UPDATE
    `, exec.ProcessInstanceID, gatewayNode.ID)

	isFirstJoin := err != nil

	if isFirstJoin {
		// First child to reach the join gateway
		// Count how many child executions should reach this gateway
		childCount, err := p.getChildExecutionCount(tx, exec.ParentExecutionID)
		if err != nil {
			return err
		}

		// Create gateway state
		joinedFlows := []string{exec.PathID}
		joinedFlowsJSON, _ := json.Marshal(joinedFlows)

		gatewayState = models.GatewayState{
			ID:                uuid.New(),
			ProcessInstanceID: exec.ProcessInstanceID,
			GatewayID:         gatewayNode.ID,
			ExpectedIncoming:  childCount,
			ReceivedIncoming:  1,
			Status:            "waiting",
			JoinedFlows:       joinedFlowsJSON,
		}

		err = p.engineRepo.CreateGatewayStateTx(tx, &gatewayState)
		if err != nil {
			return fmt.Errorf("failed to create gateway state: %w", err)
		}
	} else {
		// Update existing gateway state
		var joinedFlows []string
		json.Unmarshal(gatewayState.JoinedFlows, &joinedFlows)
		joinedFlows = append(joinedFlows, exec.PathID)
		joinedFlowsJSON, _ := json.Marshal(joinedFlows)

		newReceived := gatewayState.ReceivedIncoming + 1

		_, err = tx.Exec(`
            UPDATE public.gateway_state
            SET received_incoming = $1, joined_flows = $2
            WHERE id = $3
        `, newReceived, joinedFlowsJSON, gatewayState.ID)
		if err != nil {
			return fmt.Errorf("failed to update gateway state: %w", err)
		}

		gatewayState.ReceivedIncoming = newReceived
	}

	// Mark current child execution as completed
	err = p.engineRepo.CompleteChildExecutionTx(tx, exec.ID)
	if err != nil {
		return fmt.Errorf("failed to complete child execution: %w", err)
	}

	// Check if all children have joined
	if gatewayState.ReceivedIncoming == gatewayState.ExpectedIncoming {
		// All children have arrived - resume parent execution
		parentExecID := exec.ParentExecutionID

		// Merge variables from all branches
		mergedVariables, err := p.mergeBranchVariables(tx, parentExecID)
		if err != nil {
			return fmt.Errorf("failed to merge branch variables: %w", err)
		}

		// Update parent execution with merged variables
		if len(mergedVariables) > 0 {
			err = p.engineRepo.MergeProcessVariablesTx(tx, exec.ProcessInstanceID, mergedVariables)
			if err != nil {
				return fmt.Errorf("failed to merge variables: %w", err)
			}
		}

		// Reactivate parent execution and move to next node
		nextNodeID := p.getNextNodeAfterGateway(gatewayNode)
		err = p.engineRepo.UpdateExecutionStatusAndNodeTx(tx, *parentExecID, "active", nextNodeID)
		if err != nil {
			return fmt.Errorf("failed to resume parent execution: %w", err)
		}

		// Mark gateway as completed
		_, err = tx.Exec(`
            UPDATE public.gateway_state
            SET status = 'completed', completed_at = NOW()
            WHERE id = $1
        `, gatewayState.ID)
		if err != nil {
			return fmt.Errorf("failed to complete gateway: %w", err)
		}

		fmt.Printf("Parallel join at gateway %s completed: all %d branches joined\n",
			gatewayNode.ID, gatewayState.ExpectedIncoming)
	}

	return tx.Commit()
}

// Helper: Get number of child executions
func (p *ParallelGatewayExecutor) getChildExecutionCount(tx *sqlx.Tx, parentExecID *uuid.UUID) (int, error) {
	if parentExecID == nil {
		return 0, fmt.Errorf("parent execution ID is nil")
	}

	var count int
	err := tx.Get(&count, `
        SELECT COUNT(*)
        FROM public.executions
        WHERE parent_execution_id = $1 AND is_active = true
    `, parentExecID)
	return count, err
}

// Helper: Merge variables from all parallel branches
func (p *ParallelGatewayExecutor) mergeBranchVariables(
	tx *sqlx.Tx,
	parentExecID *uuid.UUID,
) (map[string]interface{}, error) {
	if parentExecID == nil {
		return make(map[string]interface{}), nil
	}

	// Get all child executions
	var childExecs []struct {
		ID        string
		Variables json.RawMessage
	}

	err := tx.Select(&childExecs, `
        SELECT e.id, ev.variables
        FROM public.executions e
        JOIN public.execution_variables ev ON ev.execution_id = e.id
        WHERE e.parent_execution_id = $1 AND e.is_active = false
    `, parentExecID)
	if err != nil {
		return nil, err
	}

	merged := make(map[string]interface{})

	for _, child := range childExecs {
		var vars map[string]interface{}
		if err := json.Unmarshal(child.Variables, &vars); err != nil {
			continue
		}

		// Merge strategy: later branches override earlier ones
		for k, v := range vars {
			merged[k] = v
		}
	}

	return merged, nil
}

// Helper: Get next node after gateway
func (p *ParallelGatewayExecutor) getNextNodeAfterGateway(gatewayNode *engine.Node) string {
	if len(gatewayNode.Outgoing) > 0 {
		return gatewayNode.Outgoing[0].TargetRef
	}
	return ""
}

// ResumeParentExecution resumes the parent after all children complete
func (p *ParallelGatewayExecutor) ResumeParentExecution(
	ctx context.Context,
	parentExecID uuid.UUID,
	processInstanceID uuid.UUID,
	nextNodeID string,
	variables map[string]interface{},
) error {
	tx, err := p.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	err = p.engineRepo.UpdateExecutionStatusAndNodeTx(tx, parentExecID, "active", nextNodeID)
	if err != nil {
		return fmt.Errorf("failed to update execution: %w", err)
	}

	if len(variables) > 0 {
		err = p.engineRepo.MergeProcessVariablesTx(tx, processInstanceID, variables)
		if err != nil {
			return fmt.Errorf("failed to merge variables: %w", err)
		}
	}

	fmt.Printf("Parent execution %s resumed, moving to node %s\n", parentExecID, nextNodeID)

	return tx.Commit()
}
