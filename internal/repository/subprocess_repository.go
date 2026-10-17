package repository

import (
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// Embedded sub-process support. Entering a sub-process parks the entering
// execution (the "scope execution") on the sub-process node and creates a
// child execution at the inner start event; anything inside — forks,
// multi-instance children — hangs below that child in the
// parent_execution_id tree.
//
// These helpers walk that tree in Go from one per-instance query instead of
// a WITH RECURSIVE query, which Citus can't plan against a distributed
// executions table.

type executionLink struct {
	ID                uuid.UUID  `db:"id"`
	ParentExecutionID *uuid.UUID `db:"parent_execution_id"`
	CurrentElementID  string     `db:"current_element_id"`
	Status            string     `db:"status"`
}

func loadExecutionTreeTx(tx *sqlx.Tx, instanceID uuid.UUID) (map[uuid.UUID]*executionLink, error) {
	var rows []executionLink
	if err := tx.Select(&rows, `
		SELECT id, parent_execution_id, current_element_id, status
		FROM public.executions
		WHERE process_instance_id = $1
	`, instanceID); err != nil {
		return nil, err
	}
	byID := make(map[uuid.UUID]*executionLink, len(rows))
	for i := range rows {
		byID[rows[i].ID] = &rows[i]
	}
	return byID, nil
}

// descendants returns the IDs of every execution below root (root excluded).
func descendants(tree map[uuid.UUID]*executionLink, root uuid.UUID) []uuid.UUID {
	children := map[uuid.UUID][]uuid.UUID{}
	for _, e := range tree {
		if e.ParentExecutionID != nil {
			children[*e.ParentExecutionID] = append(children[*e.ParentExecutionID], e.ID)
		}
	}
	var out []uuid.UUID
	stack := append([]uuid.UUID(nil), children[root]...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		out = append(out, id)
		stack = append(stack, children[id]...)
	}
	return out
}

// FindScopeExecutionTx returns the nearest ancestor of execID that is parked
// on the sub-process node scopeNodeID, and locks its row so concurrent
// branches finishing inside the same sub-process are serialized.
func (r *EngineRepository) FindScopeExecutionTx(tx *sqlx.Tx, instanceID, execID uuid.UUID, scopeNodeID string) (uuid.UUID, error) {
	tree, err := loadExecutionTreeTx(tx, instanceID)
	if err != nil {
		return uuid.Nil, err
	}
	self, ok := tree[execID]
	if !ok {
		return uuid.Nil, fmt.Errorf("execution %s not found", execID)
	}
	for parent := self.ParentExecutionID; parent != nil; {
		p, ok := tree[*parent]
		if !ok {
			break
		}
		if p.CurrentElementID == scopeNodeID && p.Status == "waiting" {
			var locked uuid.UUID
			if err := tx.Get(&locked, `
				SELECT id FROM public.executions
				WHERE process_instance_id = $1 AND id = $2
				FOR UPDATE
			`, instanceID, p.ID); err != nil {
				return uuid.Nil, err
			}
			return locked, nil
		}
		parent = p.ParentExecutionID
	}
	return uuid.Nil, fmt.Errorf("no execution parked on sub-process %s above execution %s", scopeNodeID, execID)
}

// HasLiveWorkUnderTx reports whether anything inside the scope execution is
// still running. A descendant counts when it is active/waiting and has no
// children of its own: a fork parent whose branches all finished is left
// "waiting" with only completed children, and must not keep the
// sub-process open. Executions parked on a node in ignoreNodes don't count.
func (r *EngineRepository) HasLiveWorkUnderTx(tx *sqlx.Tx, instanceID, scopeExecID uuid.UUID, ignoreNodes map[string]bool) (bool, error) {
	tree, err := loadExecutionTreeTx(tx, instanceID)
	if err != nil {
		return false, err
	}
	return anyLiveLeaf(tree, descendants(tree, scopeExecID), ignoreNodes), nil
}

// CloseScopeTx marks every still-open execution below the scope execution
// as completed — the leftover fork parents and armed event sub-process
// triggers a finished sub-process leaves behind — and removes those
// triggers' timers and subscriptions.
func (r *EngineRepository) CloseScopeTx(tx *sqlx.Tx, instanceID, scopeExecID uuid.UUID) error {
	tree, err := loadExecutionTreeTx(tx, instanceID)
	if err != nil {
		return err
	}
	ids := descendants(tree, scopeExecID)
	if len(ids) == 0 {
		return nil
	}
	arr := pq.Array(uuidStrings(ids))
	if err := r.DisarmBoundaryTriggersTx(tx, instanceID, ids...); err != nil {
		return err
	}
	if _, err = tx.Exec(`
		UPDATE public.executions
		SET status = 'completed', is_active = false, updated_at = NOW()
		WHERE process_instance_id = $1 AND id = ANY($2::uuid[]) AND status IN ('active', 'waiting')
	`, instanceID, arr); err != nil {
		return err
	}
	return deleteWaitStateTx(tx, instanceID, arr)
}

// deleteWaitStateTx removes the untriggered timers and unconsumed message/
// signal subscriptions of the given executions.
func deleteWaitStateTx(tx *sqlx.Tx, instanceID uuid.UUID, execIDs interface{}) error {
	if _, err := tx.Exec(`
		DELETE FROM public.timer_jobs
		WHERE process_instance_id = $1 AND execution_id = ANY($2::uuid[]) AND is_triggered = false
	`, instanceID, execIDs); err != nil {
		return err
	}
	_, err := tx.Exec(`
		DELETE FROM public.event_subscriptions
		WHERE process_instance_id = $1 AND execution_id = ANY($2::uuid[]) AND consumed_at IS NULL AND event_type <> 'compensable'
	`, instanceID, execIDs)
	return err
}

// CancelScopeTx interrupts everything below rootExecID except keepExecID
// (uuid.Nil keeps nothing): terminates the executions, cancels their open
// user tasks and pending/locked jobs, and deletes their untriggered timers
// and subscriptions. Used when an interrupting boundary event or event
// sub-process fires on a sub-process. Returns the cancelled task and job
// IDs for audit logging; a no-op when there is nothing to interrupt.
func (r *EngineRepository) CancelScopeTx(tx *sqlx.Tx, instanceID, rootExecID, keepExecID uuid.UUID) (cancelledTasks, cancelledJobs []uuid.UUID, err error) {
	tree, err := loadExecutionTreeTx(tx, instanceID)
	if err != nil {
		return nil, nil, err
	}
	ids := slices.DeleteFunc(descendants(tree, rootExecID), func(id uuid.UUID) bool { return id == keepExecID })
	if len(ids) == 0 {
		return nil, nil, nil
	}
	arr := pq.Array(uuidStrings(ids))
	if err = r.DisarmBoundaryTriggersTx(tx, instanceID, ids...); err != nil {
		return nil, nil, err
	}

	if _, err = tx.Exec(`
		UPDATE public.executions
		SET status = 'terminated', is_active = false, updated_at = NOW()
		WHERE process_instance_id = $1 AND id = ANY($2::uuid[]) AND status IN ('active', 'waiting')
	`, instanceID, arr); err != nil {
		return nil, nil, err
	}
	if err = tx.Select(&cancelledTasks, `
		UPDATE public.tasks
		SET status = 'cancelled', updated_at = NOW()
		WHERE process_instance_id = $1 AND execution_id = ANY($2::uuid[]) AND status IN ('created', 'claimed')
		RETURNING id
	`, instanceID, arr); err != nil {
		return nil, nil, err
	}
	if err = tx.Select(&cancelledJobs, `
		UPDATE public.jobs
		SET status = 'cancelled', updated_at = NOW()
		WHERE process_instance_id = $1 AND execution_id = ANY($2::uuid[]) AND status IN ('pending', 'locked')
		RETURNING id
	`, instanceID, arr); err != nil {
		return nil, nil, err
	}
	if err = deleteWaitStateTx(tx, instanceID, arr); err != nil {
		return nil, nil, err
	}
	return cancelledTasks, cancelledJobs, nil
}

func uuidStrings(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

// TerminateOtherWorkTx stops every token of the instance except keepExecID,
// for a terminate end event outside any sub-process: terminates the other
// executions, cancels open user tasks and pending/locked jobs, and deletes
// untriggered timers. Returns cancelled task and job IDs for audit logging.
func (r *EngineRepository) TerminateOtherWorkTx(tx *sqlx.Tx, instanceID, keepExecID uuid.UUID) (cancelledTasks, cancelledJobs []uuid.UUID, err error) {
	if _, err = tx.Exec(`
		UPDATE public.executions
		SET status = 'terminated', is_active = false, updated_at = NOW()
		WHERE process_instance_id = $1 AND id <> $2 AND status IN ('active', 'waiting')
	`, instanceID, keepExecID); err != nil {
		return nil, nil, err
	}
	if err = tx.Select(&cancelledTasks, `
		UPDATE public.tasks
		SET status = 'cancelled', updated_at = NOW()
		WHERE process_instance_id = $1 AND status IN ('created', 'claimed')
		RETURNING id
	`, instanceID); err != nil {
		return nil, nil, err
	}
	if err = tx.Select(&cancelledJobs, `
		UPDATE public.jobs
		SET status = 'cancelled', updated_at = NOW()
		WHERE process_instance_id = $1 AND status IN ('pending', 'locked')
		RETURNING id
	`, instanceID); err != nil {
		return nil, nil, err
	}
	if _, err = tx.Exec(`
		DELETE FROM public.timer_jobs
		WHERE process_instance_id = $1 AND is_triggered = false
	`, instanceID); err != nil {
		return nil, nil, err
	}
	if _, err = tx.Exec(`
		DELETE FROM public.event_subscriptions
		WHERE process_instance_id = $1 AND consumed_at IS NULL AND event_type <> 'compensable'
		  AND (execution_id IS NULL OR execution_id <> $2)
	`, instanceID, keepExecID); err != nil {
		return nil, nil, err
	}
	return cancelledTasks, cancelledJobs, nil
}

// LockProcessInstanceTx locks the process instance row, serializing the
// tokens of one instance that reach an end event at the same moment.
func (r *EngineRepository) LockProcessInstanceTx(tx *sqlx.Tx, instanceID uuid.UUID) error {
	var id uuid.UUID
	return tx.Get(&id, `SELECT id FROM public.process_instances WHERE id = $1 FOR UPDATE`, instanceID)
}

// HasLiveWorkInInstanceTx reports whether any token of the instance is
// still running — an active/waiting execution with no children of its own
// (see HasLiveWorkUnderTx). Executions parked on a node in ignoreNodes
// don't count.
func (r *EngineRepository) HasLiveWorkInInstanceTx(tx *sqlx.Tx, instanceID uuid.UUID, ignoreNodes map[string]bool) (bool, error) {
	tree, err := loadExecutionTreeTx(tx, instanceID)
	if err != nil {
		return false, err
	}
	all := make([]uuid.UUID, 0, len(tree))
	for id := range tree {
		all = append(all, id)
	}
	return anyLiveLeaf(tree, all, ignoreNodes), nil
}

// anyLiveLeaf reports whether one of ids is an active/waiting execution
// with no children. Armed triggers — executions waiting on a node in
// ignoreNodes — are neither live work nor children that make their parent
// a non-leaf.
func anyLiveLeaf(tree map[uuid.UUID]*executionLink, ids []uuid.UUID, ignoreNodes map[string]bool) bool {
	armed := func(e *executionLink) bool {
		return e.Status == "waiting" && ignoreNodes[e.CurrentElementID]
	}
	hasChildren := map[uuid.UUID]bool{}
	for _, e := range tree {
		if e.ParentExecutionID != nil && !armed(e) {
			hasChildren[*e.ParentExecutionID] = true
		}
	}
	for _, id := range ids {
		e := tree[id]
		if (e.Status == "active" || e.Status == "waiting") && !hasChildren[id] && !armed(e) {
			return true
		}
	}
	return false
}

// CreateErrorTriggerTx creates the trigger execution for an error event
// sub-process (eventSubNodeID) that caught a BPMN error, under the
// execution of the scope it sits in — resolved from throwingExecID, nil at
// process level — and returns its ID. Running it (runtime's
// executeEventSubProcess) interrupts the scope and starts the body.
func (r *EngineRepository) CreateErrorTriggerTx(tx *sqlx.Tx, instanceID, throwingExecID uuid.UUID, eventSubNodeID, scope string) (uuid.UUID, error) {
	var parent *uuid.UUID
	if scope != "" {
		scopeExecID, err := r.FindScopeExecutionTx(tx, instanceID, throwingExecID, scope)
		if err != nil {
			return uuid.Nil, err
		}
		parent = &scopeExecID
	}
	id := uuid.New()
	pathID := id.String() + "_" + eventSubNodeID
	if err := r.CreateExecutionTx(tx, &Execution{
		ID:                id,
		ProcessInstanceID: instanceID,
		ParentExecutionID: parent,
		CurrentElementID:  eventSubNodeID,
		Status:            "active",
		IsActive:          true,
		PathID:            &pathID,
	}); err != nil {
		return uuid.Nil, fmt.Errorf("failed to create error event sub-process trigger: %w", err)
	}
	return id, nil
}
