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
	"github.com/jmoiron/sqlx"
)

// Timer, message and signal boundary events (error boundaries are matched
// when the error is thrown, see engine.ProcessGraph.FindErrorCatcher).
//
// When an activity starts waiting, each boundary attached to it is armed by
// a trigger execution parked on the boundary node, owning the boundary's
// timer or subscription (see repository.BoundaryTriggerPathID). The
// existing timer/message/signal machinery resumes the trigger on the
// boundary node, and fireBoundary takes over. Leaving the activity disarms
// its triggers; a trigger that fires anyway for an activity already left
// is discarded.

func isBoundaryTrigger(n *engine.Node) bool {
	switch n.Type {
	case engine.BoundaryTimerEventType, engine.MessageBoundaryEventType, engine.SignalBoundaryEventType, engine.ConditionalBoundaryEventType:
		return true
	}
	return false
}

// armBoundaryEventsTx arms every timer/message/signal boundary event
// attached to activity, which exec is now waiting in.
func (r *Runtime) armBoundaryEventsTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, graph *engine.ProcessGraph, exec *repository.Execution, activity *engine.Node) error {
	for _, n := range graph.Nodes {
		if n.AttachedToRef != activity.ID || !isBoundaryTrigger(n) {
			continue
		}
		pathID := repository.BoundaryTriggerPathID(n.ID, exec.ID)
		trigger := &repository.Execution{
			ID:                uuid.New(),
			ProcessInstanceID: exec.ProcessInstanceID,
			CurrentElementID:  n.ID,
			Status:            "waiting",
			IsActive:          true,
			PathID:            &pathID,
		}
		if err := engineRepo.CreateExecutionTx(tx, trigger); err != nil {
			return fmt.Errorf("arming boundary event %s: %w", n.ID, err)
		}
		if err := r.subscribeBoundaryTx(tx, engineRepo, trigger, n); err != nil {
			return fmt.Errorf("arming boundary event %s: %w", n.ID, err)
		}
	}
	return nil
}

// subscribeBoundaryTx creates the timer or subscription that resumes a
// boundary trigger on its boundary node.
func (r *Runtime) subscribeBoundaryTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, trigger *repository.Execution, n *engine.Node) error {
	var err error
	switch n.Type {
	case engine.BoundaryTimerEventType:
		_, err = r.createTimerTx(tx, engineRepo, trigger, n.TimerDefinition, n.ID, "boundary")
	case engine.MessageBoundaryEventType:
		_, err = tx.Exec(`
			INSERT INTO event_subscriptions
			  (process_instance_id, execution_id, event_type, event_name, target_element_id)
			VALUES ($1, $2, 'message', $3, $4)
		`, trigger.ProcessInstanceID, trigger.ID, n.MessageRef, n.ID)
	case engine.SignalBoundaryEventType:
		_, err = tx.Exec(`
			INSERT INTO event_subscriptions
			  (process_instance_id, execution_id, event_type, event_name, target_element_id)
			VALUES ($1, $2, 'signal', $3, $4)
		`, trigger.ProcessInstanceID, trigger.ID, n.SignalRef, n.ID)
	case engine.ConditionalBoundaryEventType:
		err = insertConditionalSubscriptionTx(tx, trigger.ProcessInstanceID, trigger.ID, n.Condition, n.ID, false)
	}
	return err
}

// fireBoundary runs when a boundary trigger's event came. Interrupting: the
// activity's work is cancelled and its execution leaves through the
// boundary. Non-interrupting: a new token leaves through the boundary while
// the activity carries on, and the trigger stays armed for the next
// occurrence.
func (r *Runtime) fireBoundary(ctx context.Context, engineRepo *repository.EngineRepository, trigger *repository.Execution, node *engine.Node, graph *engine.ProcessGraph) error {
	activityExecID, _ := repository.ParseBoundaryTriggerPathID(trigger.PathID)
	instanceID := trigger.ProcessInstanceID

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	act, err := engineRepo.LockActivityExecutionTx(tx, instanceID, activityExecID)
	if err != nil || act.CurrentElementID != node.AttachedToRef || act.Status != "waiting" {
		// The activity was already left: this trigger is stale.
		if err := engineRepo.CompleteExecutionTx(tx, instanceID, trigger.ID); err != nil {
			return fmt.Errorf("failed to discard stale boundary trigger: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
		log.Printf("boundary event %s fired after %s was left — ignored", node.ID, node.AttachedToRef)
		return nil
	}

	if !node.CancelActivity {
		tokenID := uuid.New()
		pathID := tokenID.String()
		if err := engineRepo.CreateExecutionTx(tx, &repository.Execution{
			ID:                tokenID,
			ProcessInstanceID: instanceID,
			ParentExecutionID: act.ParentExecutionID,
			CurrentElementID:  node.ID,
			Status:            "active",
			IsActive:          true,
			PathID:            &pathID,
		}); err != nil {
			return fmt.Errorf("failed to create boundary token: %w", err)
		}
		// Stay armed. A timer cycle reschedules itself on this trigger; a
		// consumed message/signal subscription has to be renewed.
		if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, instanceID, trigger.ID, "waiting", node.ID); err != nil {
			return fmt.Errorf("failed to re-arm boundary trigger: %w", err)
		}
		switch node.Type {
		case engine.BoundaryTimerEventType:
		case engine.ConditionalBoundaryEventType:
			// Fires again only once its condition has been false in between.
			if err := insertConditionalSubscriptionTx(tx, instanceID, trigger.ID, node.Condition, node.ID, true); err != nil {
				return fmt.Errorf("failed to re-arm boundary trigger: %w", err)
			}
		default:
			if err := r.subscribeBoundaryTx(tx, engineRepo, trigger, node); err != nil {
				return fmt.Errorf("failed to re-arm boundary trigger: %w", err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit boundary event: %w", err)
		}
		return r.ExecuteExecution(ctx, tokenID)
	}

	tasks, jobs, err := engineRepo.CancelActivityWorkTx(tx, instanceID, act.ID)
	if err != nil {
		return fmt.Errorf("boundary event %s: cancelling %s: %w", node.ID, node.AttachedToRef, err)
	}
	// An interrupted sub-process loses everything still running inside it.
	scopeTasks, scopeJobs, err := engineRepo.CancelScopeTx(tx, instanceID, act.ID, uuid.Nil)
	if err != nil {
		return fmt.Errorf("boundary event %s: interrupting %s: %w", node.ID, node.AttachedToRef, err)
	}
	if err := engineRepo.CompleteExecutionTx(tx, instanceID, trigger.ID); err != nil {
		return fmt.Errorf("failed to complete boundary trigger: %w", err)
	}
	if err := engineRepo.DisarmBoundaryTriggersTx(tx, instanceID, act.ID); err != nil {
		return fmt.Errorf("failed to disarm the activity's other boundary events: %w", err)
	}
	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, instanceID, act.ID, "active", node.ID); err != nil {
		return fmt.Errorf("failed to move execution to boundary event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit boundary event: %w", err)
	}

	reason := "interrupted by boundary event " + node.ID
	r.logCancelled(instanceID, append(tasks, scopeTasks...), append(jobs, scopeJobs...), reason)
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(act.ID, instanceID, node.AttachedToRef, node.ID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(instanceID.String(), act.ID.String(), node.AttachedToRef, node.ID)
	}
	if attached, ok := graph.Nodes[node.AttachedToRef]; ok && attached.Type == engine.CallActivityType {
		r.terminateCalledInstances(act.ID, reason)
	}
	return r.ExecuteExecution(ctx, act.ID)
}

// terminateCalledInstances ends the child process instances a call
// activity started, when an interrupting boundary event cancels it.
func (r *Runtime) terminateCalledInstances(callExecID uuid.UUID, reason string) {
	var children []uuid.UUID
	if err := r.DB.Select(&children, `
		SELECT id FROM process_instances WHERE parent_execution_id = $1 AND status = 'running'
	`, callExecID); err != nil {
		log.Printf("failed to find instances called by execution %s: %v", callExecID, err)
		return
	}
	for _, id := range children {
		if err := service.TerminateProcessInstance(r.DB, r.dispatcher, id, reason); err != nil {
			log.Printf("failed to terminate called instance %s: %v", id, err)
		}
	}
}
