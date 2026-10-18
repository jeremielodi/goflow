package runtime

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// throwCompensation compensates, from the compensation throw event node,
// the completed activities of its scope that have a compensation handler
// (or only node.CompensateActivity): one handler execution per recorded
// completion, all started at once, while the throwing token waits. When
// the last handler finishes (executeCompensationHandlerEnd) the token is
// resumed on the throw event, finds nothing left to compensate and carries
// on. It reports whether the token carries on now, which is when there is
// nothing to compensate.
//
// A throw event inside an event sub-process compensates the scope the
// event sub-process belongs to — the usual "error, then undo what was
// done" pattern.
func (r *Runtime) throwCompensation(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph) (bool, error) {
	scope := node.ParentScope
	if s, ok := graph.Nodes[scope]; ok && s.Type == engine.EventSubProcessType {
		scope = s.ParentScope
	}
	// What to compensate: the activities with a compensation handler —
	// and, as in Camunda 8, inside an embedded sub-process without a
	// handler of its own, the activities it completed (nested
	// compensation, recursively).
	var activities []string
	var collect func(n *engine.Node)
	collect = func(n *engine.Node) {
		switch {
		case n.CompensationHandler != "":
			activities = append(activities, n.ID)
		case n.Type == engine.SubProcessType:
			for _, inner := range graph.Nodes {
				if inner.ParentScope == n.ID {
					collect(inner)
				}
			}
		}
	}
	for _, n := range graph.Nodes {
		if n.ParentScope == scope && (node.CompensateActivity == "" || n.ID == node.CompensateActivity) {
			collect(n)
		}
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	pending, err := engineRepo.TakeCompensablesTx(tx, exec.ProcessInstanceID, activities)
	if err != nil {
		return false, fmt.Errorf("compensation event %s: %w", node.ID, err)
	}
	if len(pending) == 0 {
		return true, tx.Commit()
	}

	pathID := repository.CompensationHandlerPathID(exec.ID)
	var handlers []uuid.UUID
	for _, p := range pending {
		id := uuid.New()
		if err := engineRepo.CreateExecutionTx(tx, &repository.Execution{
			ID:                id,
			ProcessInstanceID: exec.ProcessInstanceID,
			CurrentElementID:  p.HandlerID,
			Status:            "active",
			IsActive:          true,
			PathID:            &pathID,
		}); err != nil {
			return false, fmt.Errorf("failed to start compensation handler %s: %w", p.HandlerID, err)
		}
		handlers = append(handlers, id)
	}
	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ProcessInstanceID, exec.ID, "waiting"); err != nil {
		return false, fmt.Errorf("failed to park compensation throw event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit compensation: %w", err)
	}

	log.Printf("↩️ compensation thrown by %s: %d handler(s) started", node.ID, len(handlers))
	service.LogAuditErr("execution waited", r.auditService.LogExecutionWaited(exec.ID, exec.ProcessInstanceID, node.ID, "compensation"))
	for i, id := range handlers {
		service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(id, exec.ProcessInstanceID, node.ID, pending[i].HandlerID, nil))
		if p := eventlog.Global(); p != nil {
			p.ExecutionMoved(exec.ProcessInstanceID.String(), id.String(), node.ID, pending[i].HandlerID)
		}
		if err := r.ExecuteExecution(ctx, id); err != nil {
			return false, fmt.Errorf("compensation handler %s: %w", pending[i].HandlerID, err)
		}
	}
	return false, nil
}

// executeCompensationHandlerEnd ends a compensation handler execution; the
// last one to finish resumes the throw event that started them.
func (r *Runtime) executeCompensationHandlerEnd(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) error {
	throwerID, ok := repository.ParseCompensationHandlerPathID(exec.PathID)

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.CompleteExecutionTx(tx, exec.ProcessInstanceID, exec.ID); err != nil {
		return fmt.Errorf("failed to complete compensation handler: %w", err)
	}
	if !ok {
		return tx.Commit()
	}
	// Serializes handlers finishing at the same moment.
	thrower, err := engineRepo.LockActivityExecutionTx(tx, exec.ProcessInstanceID, throwerID)
	if err != nil || thrower.Status != "waiting" {
		return tx.Commit() // the thrower was cancelled meanwhile
	}
	running, err := engineRepo.CountRunningCompensationHandlersTx(tx, exec.ProcessInstanceID, throwerID)
	if err != nil {
		return fmt.Errorf("failed to count compensation handlers: %w", err)
	}
	if running > 0 {
		r.logExecutionCompleted(exec, node.ID)
		return tx.Commit()
	}
	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ProcessInstanceID, throwerID, "active"); err != nil {
		return fmt.Errorf("failed to resume compensation throw event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	r.logExecutionCompleted(exec, node.ID)
	return r.ExecuteExecution(ctx, throwerID)
}
