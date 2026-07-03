package repository

import "sync"

// jobWakeChans implements a simple broadcast-per-job-type wake-up: closing a
// channel wakes every current waiter for that type, and a fresh channel is
// installed immediately after so the next round of waiters has something to
// select on. Used to let gRPC's ActivateJobs long-poll react to new jobs
// near-instantly instead of only on its fallback poll interval.
var (
	jobWakeMu    sync.Mutex
	jobWakeChans = make(map[string]chan struct{})
)

// NotifyJobAvailable wakes any ActivateJobs callers currently waiting on
// jobType. Must only be called after the job's creating transaction has
// committed — waking a waiter before the row is visible would just send it
// back to an empty query.
func NotifyJobAvailable(jobType string) {
	jobWakeMu.Lock()
	defer jobWakeMu.Unlock()
	if ch, ok := jobWakeChans[jobType]; ok {
		close(ch)
	}
	jobWakeChans[jobType] = make(chan struct{})
}

// JobWakeChannel returns the current wake channel for jobType. Callers
// should grab this *before* checking the DB for available jobs, so a notify
// that races with that check is never missed — the channel returned here is
// only closed by a NotifyJobAvailable call that happens after this point.
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
