package repository

import (
	"log"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// JobAvailableChannel is the Postgres NOTIFY channel used to broadcast job
// creation across every GoFlow instance sharing this database, not just the
// instance that created the job. This is what lets a horizontally-scaled
// deployment (multiple stateless app instances behind a load balancer, one
// Postgres) still get near-instant gRPC ActivateJobs wake-ups on any
// instance, instead of only the one that happened to run the workflow step.
const JobAvailableChannel = "goflow_job_available"

// jobWakeChans implements a simple broadcast-per-job-type wake-up: closing a
// channel wakes every current waiter for that type, and a fresh channel is
// installed immediately after so the next round of waiters has something to
// select on. Used to let gRPC's ActivateJobs long-poll react to new jobs
// near-instantly instead of only on its fallback poll interval.
var (
	jobWakeMu    sync.Mutex
	jobWakeChans = make(map[string]chan struct{})
)

// NotifyJobAvailable broadcasts, via Postgres NOTIFY, that a job of jobType
// was created — waking any ActivateJobs callers currently waiting on it, on
// every GoFlow instance connected to this database (including this one,
// which receives its own notification back through the same LISTEN
// connection started by StartJobWakeListener). Must only be called after the
// job's creating transaction has committed — waking a waiter before the row
// is visible would just send it back to an empty query.
func NotifyJobAvailable(db *sqlx.DB, jobType string) error {
	_, err := db.Exec(`SELECT pg_notify($1, $2)`, JobAvailableChannel, jobType)
	return err
}

// JobWakeChannel returns the current wake channel for jobType. Callers
// should grab this *before* checking the DB for available jobs, so a notify
// that races with that check is never missed — the channel returned here is
// only closed by a wake (local or via NOTIFY) that happens after this point.
func JobWakeChannel(jobType string) <-chan struct{} {
	jobWakeMu.Lock()
	defer jobWakeMu.Unlock()
	ch, ok := jobWakeChans[jobType]
	if !ok {
		ch = make(chan struct{})
		jobWakeChans[jobType] = ch
	}
	return ch
}

// wakeLocal closes the current wake channel for jobType (if any waiter has
// requested one) and installs a fresh one for the next round of waiters.
func wakeLocal(jobType string) {
	jobWakeMu.Lock()
	defer jobWakeMu.Unlock()
	if ch, ok := jobWakeChans[jobType]; ok {
		close(ch)
	}
	jobWakeChans[jobType] = make(chan struct{})
}

// StartJobWakeListener opens a dedicated Postgres LISTEN connection (via
// lib/pq's reconnecting Listener — LISTEN state is tied to one backend
// connection, so this can't reuse the pooled *sqlx.DB) and translates every
// NOTIFY on JobAvailableChannel into a local wake-up via wakeLocal. Call
// once per process, after the main DB connection is established. If the
// listener connection drops and reconnects, any notifications sent during
// the gap are missed — this is safe because ActivateJobs also polls on a
// fixed fallback interval as a safety net (a job's lock expiring, for
// example, never fires a notify at all).
func StartJobWakeListener(connStr string) (*pq.Listener, error) {
	listener := pq.NewListener(connStr, 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf("job wake listener: %v", err)
		}
	})
	if err := listener.Listen(JobAvailableChannel); err != nil {
		listener.Close()
		return nil, err
	}

	go func() {
		for n := range listener.Notify {
			if n == nil {
				// A reconnect occurred; nothing to replay, the fallback
				// poll interval covers anything missed during the gap.
				continue
			}
			wakeLocal(n.Extra)
		}
	}()

	return listener, nil
}
