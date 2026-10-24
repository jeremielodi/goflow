package repository

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/jeremielodi/goflow/internal/models"
)

// distributedClaims: jobs and timers are Citus-distributed tables. Citus
// refuses SELECT ... FOR UPDATE on a distributed table unless the query
// targets one process instance, so a claim can't lock "the next pending
// rows". It picks candidates without a lock, then claims each with an
// UPDATE on its own shard that only succeeds while the row is still
// unclaimed — two claimers never get the same row, the loser just moves
// on to the next candidate.
var distributedClaims atomic.Bool

// SetDistributedClaims switches the claim queries to the Citus form (set
// at boot when CITUS_ENABLED).
func SetDistributedClaims(on bool) { distributedClaims.Store(on) }

// claimJobsTx is FetchAndLockJobsTx's claim on Citus.
func claimJobsTx(tx *sqlx.Tx, jobType, workerID string, lockedUntil time.Time, max int) ([]LockedJob, error) {
	var candidates []struct {
		ID                string `db:"id"`
		ProcessInstanceID string `db:"process_instance_id"`
	}
	if err := tx.Select(&candidates, `
		SELECT id, process_instance_id FROM public.jobs
		WHERE job_type = $1 AND status = 'pending'
		  AND (locked_by IS NULL OR locked_until < $3)
		ORDER BY random()
		LIMIT $2`, jobType, max*3, time.Now()); err != nil {
		return nil, err
	}
	var jobs []LockedJob
	for _, c := range candidates {
		if len(jobs) == max {
			break
		}
		var claimed []LockedJob
		if err := tx.Select(&claimed, `
			UPDATE public.jobs
			SET status = 'locked', locked_by = $1, locked_until = $2
			WHERE process_instance_id = $3 AND id = $4 AND status = 'pending'
			  AND (locked_by IS NULL OR locked_until < $5)
			RETURNING id, payload, retries, execution_id, process_instance_id, status, created_at`,
			workerID, lockedUntil, c.ProcessInstanceID, c.ID, time.Now()); err != nil {
			return nil, err
		}
		jobs = append(jobs, claimed...)
	}
	return jobs, nil
}

// claimDueTimers is ClaimDueTimers on Citus.
func claimDueTimers(ctx context.Context, db *sqlx.DB, limit int) ([]models.TimerJob, error) {
	var candidates []struct {
		ID                string `db:"id"`
		ProcessInstanceID string `db:"process_instance_id"`
	}
	if err := db.SelectContext(ctx, &candidates, `
		SELECT id, process_instance_id FROM public.timer_jobs
		WHERE is_triggered = false AND due_at <= NOW()
		ORDER BY due_at ASC
		LIMIT $1`, limit); err != nil {
		return nil, err
	}
	var timers []models.TimerJob
	for _, c := range candidates {
		var claimed []models.TimerJob
		if err := db.SelectContext(ctx, &claimed, `
			UPDATE public.timer_jobs
			SET is_triggered = true, triggered_at = NOW()
			WHERE process_instance_id = $1 AND id = $2 AND is_triggered = false
			RETURNING id, process_instance_id, execution_id, event_type, due_at,
			          payload, is_triggered, created_at, triggered_at,
			          COALESCE(cycle_count, 0) AS cycle_count, COALESCE(total_cycles, 1) AS total_cycles,
			          COALESCE(repeat_interval, '') AS repeat_interval`,
			c.ProcessInstanceID, c.ID); err != nil {
			return nil, err
		}
		timers = append(timers, claimed...)
	}
	return timers, nil
}

// appVariableNotify: on Citus, the variables tables are distributed and
// can't carry the NOTIFY trigger of migration 004 (a trigger would run on
// the workers, far from the coordinator's LISTEN). The engine then sends
// the notification itself after every variables write.
var appVariableNotify atomic.Bool

// SetAppVariableNotify makes the engine send goflow_variables
// notifications itself (set at boot when CITUS_ENABLED).
func SetAppVariableNotify(on bool) { appVariableNotify.Store(on) }

// notifyVariables tells the conditional watcher that instanceID's
// variables changed: delivered when q's transaction commits, as the
// trigger's would be. A no-op where the trigger does it.
func notifyVariables(q sqlx.Execer, instanceID interface{ String() string }, err error) error {
	if err != nil || !appVariableNotify.Load() {
		return err
	}
	_, err = q.Exec(`SELECT pg_notify('goflow_variables', $1)`, instanceID.String())
	return err
}
