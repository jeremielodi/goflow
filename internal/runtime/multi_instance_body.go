package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/feel"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

// Multi-instance activities, as in Camunda 8. The execution reaching the
// activity becomes its body: it waits on the activity (its boundary events
// attach to it) while inner instances — child executions on the same
// activity, one per element of the input collection or per loop count —
// run the activity itself, all at once or one after another. Each inner
// instance has local variables: its element (inputElement) and
// loopCounter. When an inner instance leaves the activity, whatever the
// activity is (a task of any kind, a sub-process, a call activity), the
// runtime loop hands it to completeInnerInstance: its outputElement goes
// into the body's outputCollection, the completion condition is checked,
// and once every inner instance is done (or the condition holds) the body
// leaves the activity with outputCollection set in the enclosing scope.

// innerInstance is an execution running one iteration of a multi-instance
// activity.
type innerInstance struct {
	BodyID     uuid.UUID
	LoopIndex  int
	ActivityID string
}

// lookupInnerInstance tells whether execID runs an iteration of a
// multi-instance activity that is still going.
func lookupInnerInstance(q sqlx.Queryer, instanceID, execID uuid.UUID) (*innerInstance, error) {
	var child struct {
		BodyID    uuid.UUID `db:"parent_execution_id"`
		LoopIndex int       `db:"loop_index"`
	}
	err := sqlx.Get(q, &child, `
		SELECT parent_execution_id, loop_index FROM public.multi_instance_children
		WHERE child_execution_id = $1 AND status = 'active'
	`, execID)
	if err != nil {
		return nil, nil // not an inner instance
	}
	var activityID string
	if err := sqlx.Get(q, &activityID, `
		SELECT activity_id FROM public.multi_instance_executions
		WHERE process_instance_id = $1 AND execution_id = $2 AND status = 'active'
	`, instanceID, child.BodyID); err != nil {
		return nil, nil
	}
	return &innerInstance{BodyID: child.BodyID, LoopIndex: child.LoopIndex, ActivityID: activityID}, nil
}

func trimFEEL(expr string) string {
	return strings.TrimPrefix(strings.TrimSpace(expr), "=")
}

// multiInstanceItems returns the input collection's elements (nil without
// one) and how many inner instances to run.
func multiInstanceItems(cfg *engine.MultiInstanceConfig, vars map[string]interface{}) ([]interface{}, int, error) {
	if coll := strings.TrimSpace(cfg.Collection()); coll != "" {
		var v interface{}
		if value, ok := vars[coll]; ok && !strings.HasPrefix(coll, "=") {
			v = value // a plain variable name (older models)
		} else {
			var err error
			if v, err = feel.Evaluate(trimFEEL(coll), vars); err != nil {
				return nil, 0, fmt.Errorf("input collection %q: %w", coll, err)
			}
		}
		items, ok := v.([]interface{})
		if !ok {
			return nil, 0, fmt.Errorf("input collection %q is %v, not a list", coll, v)
		}
		return items, len(items), nil
	}
	if card := strings.TrimSpace(cfg.LoopCardinality); card != "" {
		v, err := feel.Evaluate(trimFEEL(card), vars)
		if err != nil {
			return nil, 0, fmt.Errorf("loop cardinality %q: %w", card, err)
		}
		n, ok := v.(float64)
		if !ok || n < 0 || n != math.Trunc(n) {
			return nil, 0, fmt.Errorf("loop cardinality %q is %v, not a whole number", card, v)
		}
		return nil, int(n), nil
	}
	return nil, 0, fmt.Errorf("multi-instance needs an input collection or a loop cardinality")
}

// startMultiInstanceBody makes exec the body of node's multi-instance loop
// and starts its inner instances.
func (r *Runtime) startMultiInstanceBody(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph, variables map[string]interface{}) error {
	cfg := node.MultiInstance
	items, total, err := multiInstanceItems(cfg, variables)
	if err != nil {
		return fmt.Errorf("multi-instance %s: %w", node.ID, err)
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`
		INSERT INTO public.multi_instance_executions
		  (id, process_instance_id, execution_id, activity_id, is_sequential, total_count, completed_count,
		   completion_condition, element_variable, input_collection, output_collection, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9, $10, 'active', NOW(), NOW())
	`, uuid.New(), exec.ProcessInstanceID, exec.ID, node.ID, cfg.IsSequential, total,
		cfg.CompletionCondition, cfg.Element(), cfg.Collection(), cfg.Output()); err != nil {
		return fmt.Errorf("multi-instance %s: %w", node.ID, err)
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ProcessInstanceID, exec.ID, "waiting", node.ID); err != nil {
		return err
	}
	if err := r.armBoundaryEventsTx(tx, engineRepo, graph, exec, node); err != nil {
		return err
	}
	if out := cfg.Output(); out != "" {
		if err := repository.SetLocalVariablesTx(tx, exec.ProcessInstanceID, exec.ID, map[string]interface{}{out: make([]interface{}, total)}); err != nil {
			return err
		}
	}

	if total == 0 {
		next, err := r.finishMultiInstanceBodyTx(tx, engineRepo, exec, node)
		if err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		r.logBodyLeft(exec, node.ID, next)
		return r.ExecuteExecution(ctx, exec.ID)
	}

	count := total
	if cfg.IsSequential {
		count = 1
	}
	inners := make([]uuid.UUID, 0, count)
	for i := 0; i < count; i++ {
		id, err := r.createInnerInstanceTx(tx, engineRepo, exec, node, i, items)
		if err != nil {
			return err
		}
		inners = append(inners, id)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for _, id := range inners {
		if err := r.ExecuteExecution(ctx, id); err != nil {
			return fmt.Errorf("multi-instance %s, instance %s: %w", node.ID, id, err)
		}
	}
	return nil
}

// createInnerInstanceTx creates the inner instance for loop index i.
func (r *Runtime) createInnerInstanceTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, body *repository.Execution, node *engine.Node, i int, items []interface{}) (uuid.UUID, error) {
	id := uuid.New()
	pathID := fmt.Sprintf("%s#%d", body.ID, i+1)
	if err := engineRepo.CreateExecutionTx(tx, &repository.Execution{
		ID:                id,
		ProcessInstanceID: body.ProcessInstanceID,
		ParentExecutionID: &body.ID,
		CurrentElementID:  node.ID,
		Status:            "active",
		IsActive:          true,
		PathID:            &pathID,
	}); err != nil {
		return uuid.Nil, fmt.Errorf("multi-instance %s: creating instance %d: %w", node.ID, i+1, err)
	}
	locals := map[string]interface{}{"loopCounter": i + 1}
	var element interface{}
	if items != nil {
		element = items[i]
		if name := node.MultiInstance.Element(); name != "" {
			locals[name] = element
		}
	}
	elementJSON, _ := json.Marshal(element)
	if _, err := tx.Exec(`
		INSERT INTO public.multi_instance_children (parent_execution_id, child_execution_id, loop_index, element_value, status, created_at)
		VALUES ($1, $2, $3, $4, 'active', NOW())
	`, body.ID, id, i, string(elementJSON)); err != nil {
		return uuid.Nil, err
	}
	return id, repository.SetLocalVariablesTx(tx, body.ProcessInstanceID, id, locals)
}

// completeInnerInstance runs when an inner instance has left its activity.
func (r *Runtime) completeInnerInstance(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, inner *innerInstance, graph *engine.ProcessGraph) error {
	node, ok := graph.Nodes[inner.ActivityID]
	if !ok || node.MultiInstance == nil {
		return fmt.Errorf("multi-instance activity %s not found", inner.ActivityID)
	}
	cfg := node.MultiInstance
	instanceID := exec.ProcessInstanceID

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Inner instances completing together take turns on the body.
	locked, err := engineRepo.LockActivityExecutionTx(tx, instanceID, inner.BodyID)
	body := &repository.Execution{ID: inner.BodyID, ProcessInstanceID: instanceID}
	if err != nil || locked.CurrentElementID != node.ID || locked.Status != "waiting" {
		// The body was interrupted meanwhile: this instance just ends.
		if err := engineRepo.CompleteExecutionTx(tx, instanceID, exec.ID); err != nil {
			return err
		}
		return tx.Commit()
	}

	view, err := repository.VariablesForExecution(tx, instanceID, exec.ID)
	if err != nil {
		return err
	}
	if out := cfg.Output(); out != "" && strings.TrimSpace(cfg.OutputElement) != "" {
		value, err := feel.Evaluate(trimFEEL(cfg.OutputElement), view)
		if err != nil {
			return fmt.Errorf("multi-instance %s: output element %q: %w", node.ID, cfg.OutputElement, err)
		}
		locals, err := repository.LocalVariables(tx, instanceID, body.ID)
		if err != nil {
			return err
		}
		list, _ := locals[out].([]interface{})
		for len(list) <= inner.LoopIndex {
			list = append(list, nil)
		}
		list[inner.LoopIndex] = value
		if err := repository.SetLocalVariablesTx(tx, instanceID, body.ID, map[string]interface{}{out: list}); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(`
		UPDATE public.multi_instance_children SET status = 'completed', completed_at = NOW()
		WHERE child_execution_id = $1
	`, exec.ID); err != nil {
		return err
	}
	if err := engineRepo.CompleteExecutionTx(tx, instanceID, exec.ID); err != nil {
		return err
	}
	if err := repository.ClearLocalVariablesTx(tx, instanceID, exec.ID); err != nil {
		return err
	}

	var counts struct {
		Completed  int `db:"completed"`
		Active     int `db:"active"`
		Terminated int `db:"terminated"`
	}
	if err := tx.Get(&counts, `
		SELECT COUNT(*) FILTER (WHERE status = 'completed') AS completed,
		       COUNT(*) FILTER (WHERE status = 'active') AS active,
		       COUNT(*) FILTER (WHERE status = 'terminated') AS terminated
		FROM public.multi_instance_children WHERE parent_execution_id = $1
	`, body.ID); err != nil {
		return err
	}
	var total int
	if err := tx.Get(&total, `
		SELECT total_count FROM public.multi_instance_executions
		WHERE process_instance_id = $1 AND execution_id = $2 AND status = 'active'
	`, instanceID, body.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		UPDATE public.multi_instance_executions
		SET completed_count = $3, nr_of_completed_instances = $3, nr_of_active_instances = $4, loop_counter = $5, updated_at = NOW()
		WHERE process_instance_id = $1 AND execution_id = $2 AND status = 'active'
	`, instanceID, body.ID, counts.Completed, counts.Active, inner.LoopIndex+1); err != nil {
		return err
	}

	done := counts.Completed+counts.Terminated >= total
	if cond := strings.TrimSpace(cfg.CompletionCondition); cond != "" && !done {
		active := counts.Active
		if cfg.IsSequential {
			active = 0
		}
		condVars := map[string]interface{}{}
		for k, v := range view {
			condVars[k] = v
		}
		for k, v := range map[string]interface{}{
			"numberOfInstances": total, "numberOfActiveInstances": active,
			"numberOfCompletedInstances": counts.Completed, "numberOfTerminatedInstances": counts.Terminated,
			// Camunda 7 names
			"nrOfInstances": total, "nrOfActiveInstances": active, "nrOfCompletedInstances": counts.Completed,
		} {
			condVars[k] = v
		}
		met, err := feel.EvaluateBool(trimFEEL(cond), condVars)
		if err != nil {
			return fmt.Errorf("multi-instance %s: completion condition %q: %w", node.ID, cond, err)
		}
		if met {
			if err := r.terminateInnerInstancesTx(tx, engineRepo, instanceID, body.ID); err != nil {
				return err
			}
			done = true
		}
	}

	if !done {
		var next uuid.UUID
		if cfg.IsSequential {
			items, _, err := multiInstanceItems(cfg, view)
			if err != nil && cfg.Collection() != "" {
				return fmt.Errorf("multi-instance %s: %w", node.ID, err)
			}
			if next, err = r.createInnerInstanceTx(tx, engineRepo, body, node, counts.Completed, items); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		r.logExecutionCompleted(exec, node.ID)
		if next != uuid.Nil {
			return r.ExecuteExecution(ctx, next)
		}
		return nil
	}

	nextNode, err := r.finishMultiInstanceBodyTx(tx, engineRepo, body, node)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Inner instances the completion condition stopped may have been call
	// activities.
	service.TerminateOrphanedCalledInstances(r.DB, r.dispatcher, &instanceID)
	r.logExecutionCompleted(exec, node.ID)
	r.logBodyLeft(body, node.ID, nextNode)
	return r.ExecuteExecution(ctx, body.ID)
}

// terminateInnerInstancesTx stops the inner instances still running (the
// completion condition was met).
func (r *Runtime) terminateInnerInstancesTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, instanceID, bodyID uuid.UUID) error {
	var active []uuid.UUID
	if err := tx.Select(&active, `
		SELECT child_execution_id FROM public.multi_instance_children
		WHERE parent_execution_id = $1 AND status = 'active'
	`, bodyID); err != nil {
		return err
	}
	for _, id := range active {
		tasks, jobs, err := engineRepo.CancelActivityWorkTx(tx, instanceID, id)
		if err != nil {
			return err
		}
		scopeTasks, scopeJobs, err := engineRepo.CancelScopeTx(tx, instanceID, id, uuid.Nil)
		if err != nil {
			return err
		}
		if err := engineRepo.DisarmBoundaryTriggersTx(tx, instanceID, id); err != nil {
			return err
		}
		if err := engineRepo.CompleteExecutionTx(tx, instanceID, id); err != nil {
			return err
		}
		if err := repository.ClearLocalVariablesTx(tx, instanceID, id); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE public.multi_instance_children SET status = 'terminated', completed_at = NOW() WHERE child_execution_id = $1`, id); err != nil {
			return err
		}
		r.logCancelled(instanceID, append(tasks, scopeTasks...), append(jobs, scopeJobs...), "multi-instance completion condition met")
	}
	return nil
}

// finishMultiInstanceBodyTx moves the body out of its activity: the output
// collection goes to the enclosing scope, its boundary events are
// disarmed. Returns where the body went.
func (r *Runtime) finishMultiInstanceBodyTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, body *repository.Execution, node *engine.Node) (string, error) {
	instanceID := body.ProcessInstanceID
	locals, err := repository.LocalVariables(tx, instanceID, body.ID)
	if err != nil {
		return "", err
	}
	if err := repository.ClearLocalVariablesTx(tx, instanceID, body.ID); err != nil {
		return "", err
	}
	if out := node.MultiInstance.Output(); out != "" {
		list, _ := locals[out].([]interface{})
		if list == nil {
			list = []interface{}{}
		}
		if err := repository.SetVariablesTx(tx, instanceID, body.ID, map[string]interface{}{out: list}); err != nil {
			return "", err
		}
	}
	if _, err := tx.Exec(`
		UPDATE public.multi_instance_executions SET status = 'completed', updated_at = NOW()
		WHERE process_instance_id = $1 AND execution_id = $2 AND status = 'active'
	`, instanceID, body.ID); err != nil {
		return "", err
	}
	if err := engineRepo.DisarmBoundaryTriggersTx(tx, instanceID, body.ID); err != nil {
		return "", err
	}
	if node.CompensationHandler != "" {
		if err := engineRepo.RecordCompensableTx(tx, instanceID, body.ID, node.ID, node.CompensationHandler); err != nil {
			return "", err
		}
	}
	next, err := common.ResolveNext(node, nil)
	if err != nil {
		return "", fmt.Errorf("leaving multi-instance %s: %w", node.ID, err)
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, instanceID, body.ID, "active", next); err != nil {
		return "", err
	}
	return next, nil
}

func (r *Runtime) logBodyLeft(body *repository.Execution, from, to string) {
	log.Printf("multi-instance %s completed", from)
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(body.ID, body.ProcessInstanceID, from, to, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(body.ProcessInstanceID.String(), body.ID.String(), from, to)
	}
}

// outputScope is where a synchronous activity's results are set from: its
// own scope, or with output mappings (already applied to the results) its
// enclosing scope, as in Camunda 8.
func outputScope(exec *repository.Execution, node *engine.Node) uuid.UUID {
	if len(node.OutputMappings) > 0 {
		if exec.ParentExecutionID != nil {
			return *exec.ParentExecutionID
		}
		return uuid.Nil
	}
	return exec.ID
}
