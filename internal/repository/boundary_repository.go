package repository

import (
	"strings"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// Boundary event triggers. While an activity waits, each timer/message/
// signal boundary event attached to it is armed by a trigger execution
// parked on the boundary node, owning the boundary's timer or
// subscription. A trigger has no parent execution — if it were a child of
// the activity's execution, the fork/join code that counts an execution's
// children would count it — so the activity's execution ID is encoded in
// its path_id instead: "boundary:<boundary node ID>:<activity execution ID>".

const boundaryTriggerPrefix = "boundary:"

// BoundaryTriggerPathID is the path_id of the trigger arming boundaryNodeID
// for the activity running in activityExecID.
func BoundaryTriggerPathID(boundaryNodeID string, activityExecID uuid.UUID) string {
	return boundaryTriggerPrefix + boundaryNodeID + ":" + activityExecID.String()
}

// ParseBoundaryTriggerPathID returns the activity execution a boundary
// trigger watches; ok is false when pathID isn't a boundary trigger's.
func ParseBoundaryTriggerPathID(pathID *string) (activityExecID uuid.UUID, ok bool) {
	if pathID == nil || !strings.HasPrefix(*pathID, boundaryTriggerPrefix) {
		return uuid.Nil, false
	}
	i := strings.LastIndex(*pathID, ":")
	id, err := uuid.Parse((*pathID)[i+1:])
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// DisarmBoundaryTriggersTx removes the armed boundary triggers of the given
// activity executions, with their timers and subscriptions: called when an
// activity is left, so its boundary events can no longer fire. Also drops
// boundary timers created directly on the activity's execution by older
// versions of the engine.
func (r *EngineRepository) DisarmBoundaryTriggersTx(tx *sqlx.Tx, instanceID uuid.UUID, activityExecIDs ...uuid.UUID) error {
	if len(activityExecIDs) == 0 {
		return nil
	}
	activities := pq.Array(uuidStrings(activityExecIDs))

	var triggers []string
	if err := tx.Select(&triggers, `
		UPDATE public.executions
		SET status = 'completed', is_active = false, updated_at = NOW()
		WHERE process_instance_id = $1 AND status = 'waiting'
		  AND path_id LIKE 'boundary:%'
		  AND split_part(path_id, ':', 3) = ANY($2::text[])
		RETURNING id::text
	`, instanceID, activities); err != nil {
		return err
	}
	if len(triggers) > 0 {
		if err := deleteWaitStateTx(tx, instanceID, pq.Array(triggers)); err != nil {
			return err
		}
	}
	_, err := tx.Exec(`
		DELETE FROM public.timer_jobs
		WHERE process_instance_id = $1 AND execution_id = ANY($2::uuid[])
		  AND event_type = 'boundary' AND is_triggered = false
	`, instanceID, activities)
	return err
}

// ActivityExecution is the state of the execution a boundary trigger
// watches, read under a row lock when the trigger fires.
type ActivityExecution struct {
	ID                uuid.UUID  `db:"id"`
	ParentExecutionID *uuid.UUID `db:"parent_execution_id"`
	CurrentElementID  string     `db:"current_element_id"`
	Status            string     `db:"status"`
}

// LockActivityExecutionTx loads and locks the execution a boundary trigger
// watches, so a boundary firing and the activity completing can't both win.
func (r *EngineRepository) LockActivityExecutionTx(tx *sqlx.Tx, instanceID, execID uuid.UUID) (*ActivityExecution, error) {
	var e ActivityExecution
	if err := tx.Get(&e, `
		SELECT id, parent_execution_id, current_element_id, status
		FROM public.executions
		WHERE process_instance_id = $1 AND id = $2
		FOR UPDATE
	`, instanceID, execID); err != nil {
		return nil, err
	}
	return &e, nil
}

// CancelActivityWorkTx cancels the open user tasks and pending/locked jobs
// of one activity execution, for an interrupting boundary event.
func (r *EngineRepository) CancelActivityWorkTx(tx *sqlx.Tx, instanceID, execID uuid.UUID) (cancelledTasks, cancelledJobs []uuid.UUID, err error) {
	if err = tx.Select(&cancelledTasks, `
		UPDATE public.tasks
		SET status = 'cancelled', updated_at = NOW()
		WHERE process_instance_id = $1 AND execution_id = $2 AND status IN ('created', 'claimed')
		RETURNING id
	`, instanceID, execID); err != nil {
		return nil, nil, err
	}
	if err = tx.Select(&cancelledJobs, `
		UPDATE public.jobs
		SET status = 'cancelled', updated_at = NOW()
		WHERE process_instance_id = $1 AND execution_id = $2 AND status IN ('pending', 'locked')
		RETURNING id
	`, instanceID, execID); err != nil {
		return nil, nil, err
	}
	return cancelledTasks, cancelledJobs, nil
}
