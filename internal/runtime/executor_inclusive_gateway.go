package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type InclusiveGatewayExecutor struct {
	db         *sqlx.DB
	engineRepo *repository.EngineRepository
}

func NewInclusiveGatewayExecutor(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *InclusiveGatewayExecutor {
	return &InclusiveGatewayExecutor{
		db:         db,
		engineRepo: repository.NewEngineRepository(db, dispatcher),
	}
}

// ExecuteFork evaluates conditions and creates child executions only for matching flows.
func (e *InclusiveGatewayExecutor) ExecuteFork(
	ctx context.Context,
	execID uuid.UUID,
	gatewayNode *engine.Node,
	variables map[string]interface{},
) error {
	exec, err := e.engineRepo.GetActiveExecution(ctx, execID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}
	if exec == nil {
		return fmt.Errorf("execution not found: %s", execID)
	}

	// Evaluate conditions — collect only matching outgoing flows.
	var activeBranches []engine.Flow
	for _, flow := range gatewayNode.Outgoing {
		condition := strings.TrimSpace(flow.Condition)
		if condition == "" {
			activeBranches = append(activeBranches, flow)
			continue
		}
		ok, err := common.EvaluateCondition(condition, variables)
		if err != nil {
			return fmt.Errorf("condition evaluation failed for flow %s: %w", flow.ID, err)
		}
		if ok {
			activeBranches = append(activeBranches, flow)
		}
	}

	if len(activeBranches) == 0 {
		return fmt.Errorf("inclusive gateway %s: no condition matched", gatewayNode.ID)
	}

	tx, err := e.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	branchVars, _ := json.Marshal(variables)

	for i, flow := range activeBranches {
		childExecID := uuid.New()
		pathID := fmt.Sprintf("%s_%d", exec.ID.String(), i)

		if err = e.engineRepo.CreateExecutionTx(tx, &repository.Execution{
			ID:                childExecID,
			ProcessInstanceID: exec.ProcessInstanceID,
			ParentExecutionID: &exec.ID,
			CurrentElementID:  flow.TargetRef,
			Status:            "active",
			IsActive:          true,
			PathID:            &pathID,
		}); err != nil {
			return fmt.Errorf("failed to create child execution: %w", err)
		}

		if _, err = tx.Exec(`
			INSERT INTO public.execution_variables (execution_id, variables, created_at)
			VALUES ($1, $2, NOW())
		`, childExecID, branchVars); err != nil {
			return fmt.Errorf("failed to store branch variables: %w", err)
		}
	}

	if err = e.engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting"); err != nil {
		return fmt.Errorf("failed to mark parent as waiting: %w", err)
	}

	fmt.Printf("Inclusive fork at gateway %s: %d/%d branches active\n",
		gatewayNode.ID, len(activeBranches), len(gatewayNode.Outgoing))

	return tx.Commit()
}

// ExecuteJoin waits for all activated branches then resumes parent.
// Logic is identical to the parallel gateway join — expected_incoming is set from
// the actual number of child executions created at fork time.
func (e *InclusiveGatewayExecutor) ExecuteJoin(
	ctx context.Context,
	execID uuid.UUID,
	gatewayNode *engine.Node,
) error {
	exec, err := e.engineRepo.GetActiveExecution(ctx, execID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}
	if exec == nil {
		return fmt.Errorf("execution not found: %s", execID)
	}
	if exec.ParentExecutionID == nil {
		return fmt.Errorf("join gateway called on execution with no parent")
	}

	tx, err := e.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var gatewayState models.GatewayState
	err = tx.Get(&gatewayState, `
		SELECT id, expected_incoming, received_incoming, status, joined_flows
		FROM public.gateway_state
		WHERE process_instance_id = $1 AND gateway_id = $2 AND status = 'waiting'
		FOR UPDATE
	`, exec.ProcessInstanceID, gatewayNode.ID)

	isFirstJoin := err != nil

	if isFirstJoin {
		childCount, err := e.getChildExecutionCount(tx, exec.ParentExecutionID)
		if err != nil {
			return err
		}

		joinedFlows := []string{*exec.PathID}
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

		if err = e.engineRepo.CreateGatewayStateTx(tx, &gatewayState); err != nil {
			return fmt.Errorf("failed to create gateway state: %w", err)
		}
	} else {
		var joinedFlows []string
		json.Unmarshal(gatewayState.JoinedFlows, &joinedFlows)
		if exec.PathID != nil {
			joinedFlows = append(joinedFlows, *exec.PathID)
		}
		joinedFlowsJSON, _ := json.Marshal(joinedFlows)
		newReceived := gatewayState.ReceivedIncoming + 1

		if _, err = tx.Exec(`
			UPDATE public.gateway_state
			SET received_incoming = $1, joined_flows = $2
			WHERE id = $3
		`, newReceived, joinedFlowsJSON, gatewayState.ID); err != nil {
			return fmt.Errorf("failed to update gateway state: %w", err)
		}

		gatewayState.ReceivedIncoming = newReceived
	}

	if err = e.engineRepo.CompleteChildExecutionTx(tx, exec.ID); err != nil {
		return fmt.Errorf("failed to complete child execution: %w", err)
	}

	if gatewayState.ReceivedIncoming == gatewayState.ExpectedIncoming {
		mergedVariables, err := e.mergeBranchVariables(tx, exec.ParentExecutionID)
		if err != nil {
			return fmt.Errorf("failed to merge branch variables: %w", err)
		}
		if len(mergedVariables) > 0 {
			if err = e.engineRepo.MergeProcessVariablesTx(tx, exec.ProcessInstanceID, mergedVariables); err != nil {
				return fmt.Errorf("failed to merge variables: %w", err)
			}
		}

		nextNodeID := gatewayNode.Outgoing[0].TargetRef
		if err = e.engineRepo.UpdateExecutionStatusAndNodeTx(tx, *exec.ParentExecutionID, "active", nextNodeID); err != nil {
			return fmt.Errorf("failed to resume parent execution: %w", err)
		}

		if _, err = tx.Exec(`
			UPDATE public.gateway_state SET status = 'completed', completed_at = NOW()
			WHERE id = $1
		`, gatewayState.ID); err != nil {
			return fmt.Errorf("failed to complete gateway state: %w", err)
		}

		fmt.Printf("Inclusive join at gateway %s completed: all %d branches joined\n",
			gatewayNode.ID, gatewayState.ExpectedIncoming)
	}

	return tx.Commit()
}

func (e *InclusiveGatewayExecutor) getChildExecutionCount(tx *sqlx.Tx, parentExecID *uuid.UUID) (int, error) {
	if parentExecID == nil {
		return 0, fmt.Errorf("parent execution ID is nil")
	}
	var count int
	err := tx.Get(&count, `
		SELECT COUNT(*) FROM public.executions
		WHERE parent_execution_id = $1 AND is_active = true
	`, parentExecID)
	return count, err
}

func (e *InclusiveGatewayExecutor) mergeBranchVariables(
	tx *sqlx.Tx,
	parentExecID *uuid.UUID,
) (map[string]interface{}, error) {
	if parentExecID == nil {
		return make(map[string]interface{}), nil
	}

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
		for k, v := range vars {
			merged[k] = v
		}
	}
	return merged, nil
}
