package runtime

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

// executeSubProcess enters an embedded sub-process: the current execution
// parks on the sub-process node (and owns its boundary timers), and a child
// execution starts at the sub-process's inner start event — the same
// parent-waits-for-children shape a parallel fork uses.
func (r *Runtime) executeSubProcess(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph, variables map[string]interface{}) error {
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ProcessInstanceID, exec.ID, "waiting", node.ID); err != nil {
		return fmt.Errorf("failed to park execution on sub-process: %w", err)
	}

	if err := r.armBoundaryEventsTx(tx, engineRepo, graph, exec, node); err != nil {
		return err
	}
	if err := r.armEventSubProcessesTx(tx, engineRepo, graph, node.ID, exec.ProcessInstanceID, &exec.ID); err != nil {
		return err
	}
	// Input mappings are the sub-process scope's local variables.
	if inputs := common.EvaluateIOMappings(node.InputMappings, variables); len(inputs) > 0 {
		if err := repository.SetLocalVariablesTx(tx, exec.ProcessInstanceID, exec.ID, inputs); err != nil {
			return fmt.Errorf("sub-process %s: input mappings: %w", node.ID, err)
		}
	}

	childID := uuid.New()
	pathID := exec.ID.String() + "_" + node.ID
	if err := engineRepo.CreateExecutionTx(tx, &repository.Execution{
		ID:                childID,
		ProcessInstanceID: exec.ProcessInstanceID,
		ParentExecutionID: &exec.ID,
		CurrentElementID:  node.StartNodeID,
		Status:            "active",
		IsActive:          true,
		PathID:            &pathID,
	}); err != nil {
		return fmt.Errorf("failed to create sub-process execution: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit sub-process entry: %w", err)
	}

	service.LogAuditErr("execution waited", r.auditService.LogExecutionWaited(exec.ID, exec.ProcessInstanceID, node.ID, "subProcess"))
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(childID, exec.ProcessInstanceID, node.ID, node.StartNodeID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(exec.ProcessInstanceID.String(), childID.String(), node.ID, node.StartNodeID)
	}

	return r.ExecuteExecution(ctx, childID)
}

// executeSubProcessEnd handles an end event inside an embedded sub-process.
// It ends only this token; once nothing else is running inside the
// sub-process, the execution parked on the sub-process node resumes on the
// sub-process's outgoing flow.
func (r *Runtime) executeSubProcessEnd(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph, variables map[string]interface{}) error {
	subNode, ok := graph.Nodes[node.ParentScope]
	if !ok {
		return fmt.Errorf("end event %s: sub-process %s not found", node.ID, node.ParentScope)
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Locks the scope execution: concurrent branches ending inside the same
	// sub-process take turns, so exactly one of them sees no live work left.
	scopeID, err := engineRepo.FindScopeExecutionTx(tx, exec.ProcessInstanceID, exec.ID, subNode.ID)
	if err != nil {
		return fmt.Errorf("end event %s: %w", node.ID, err)
	}
	if err := engineRepo.CompleteExecutionTx(tx, exec.ProcessInstanceID, exec.ID); err != nil {
		return fmt.Errorf("failed to complete execution: %w", err)
	}

	live, err := engineRepo.HasLiveWorkUnderTx(tx, exec.ProcessInstanceID, scopeID, graph.TriggerNodes())
	if err != nil {
		return fmt.Errorf("failed to check sub-process completion: %w", err)
	}
	if live {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
		r.logExecutionCompleted(exec, node.ID)
		return nil
	}

	if subNode.Type == engine.EventSubProcessType {
		r.logExecutionCompleted(exec, node.ID)
		return r.finishEventSubProcess(ctx, engineRepo, tx, exec.ProcessInstanceID, scopeID, subNode, graph, variables)
	}

	next, err := common.ResolveNext(subNode, variables)
	if err != nil {
		return fmt.Errorf("leaving sub-process %s: %w", subNode.ID, err)
	}
	if err := engineRepo.CloseScopeTx(tx, exec.ProcessInstanceID, scopeID); err != nil {
		return fmt.Errorf("failed to close sub-process executions: %w", err)
	}
	if err := leaveScopeVariablesTx(tx, exec.ProcessInstanceID, scopeID, subNode); err != nil {
		return fmt.Errorf("leaving sub-process %s: %w", subNode.ID, err)
	}
	// The scope execution leaves the sub-process: its boundary events are disarmed.
	if err := engineRepo.DisarmBoundaryTriggersTx(tx, exec.ProcessInstanceID, scopeID); err != nil {
		return fmt.Errorf("failed to disarm sub-process boundary events: %w", err)
	}
	// The sub-process completed: it can now be compensated.
	if subNode.CompensationHandler != "" {
		if err := engineRepo.RecordCompensableTx(tx, exec.ProcessInstanceID, scopeID, subNode.ID, subNode.CompensationHandler); err != nil {
			return fmt.Errorf("failed to record compensation: %w", err)
		}
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ProcessInstanceID, scopeID, "active", next); err != nil {
		return fmt.Errorf("failed to resume execution after sub-process: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit sub-process completion: %w", err)
	}

	r.logExecutionCompleted(exec, node.ID)
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(scopeID, exec.ProcessInstanceID, subNode.ID, next, variables))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(exec.ProcessInstanceID.String(), scopeID.String(), subNode.ID, next)
	}

	return r.ExecuteExecution(ctx, scopeID)
}

func (r *Runtime) logExecutionCompleted(exec *repository.Execution, nodeID string) {
	service.LogAuditErr("execution completed", r.auditService.LogExecutionCompleted(exec.ID, exec.ProcessInstanceID, nodeID))
	if p := eventlog.Global(); p != nil {
		p.ExecutionCompleted(exec.ProcessInstanceID.String(), exec.ID.String(), nodeID)
	}
}

// leaveScopeVariablesTx runs as the execution holding an embedded
// sub-process's scope leaves it: its output mappings, evaluated in that
// scope, are set from the enclosing scope, and its local variables go —
// unless it is an inner instance of a multi-instance sub-process, whose
// scope still serves its outputElement (completeInnerInstance clears it).
func leaveScopeVariablesTx(tx *sqlx.Tx, instanceID, scopeID uuid.UUID, subNode *engine.Node) error {
	var mapped map[string]interface{}
	if len(subNode.OutputMappings) > 0 {
		view, err := repository.VariablesForExecution(tx, instanceID, scopeID)
		if err != nil {
			return err
		}
		mapped = common.EvaluateIOMappings(subNode.OutputMappings, view)
	}
	if subNode.MultiInstance == nil {
		if err := repository.ClearLocalVariablesTx(tx, instanceID, scopeID); err != nil {
			return err
		}
	}
	if len(mapped) == 0 {
		return nil
	}
	var parent *uuid.UUID
	if err := tx.Get(&parent, `SELECT parent_execution_id FROM public.executions WHERE process_instance_id = $1 AND id = $2`, instanceID, scopeID); err != nil {
		return err
	}
	from := uuid.Nil
	if parent != nil {
		from = *parent
	}
	return repository.SetVariablesTx(tx, instanceID, from, mapped)
}
