// service/timer_scheduler.go
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/cmdbus"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	timerpkg "github.com/jeremielodi/goflow/internal/timer"
	"github.com/jmoiron/sqlx"
)

// CmdFireTimer fires a due timer of a process instance: a command (see
// package cmdbus), so in partitioned mode it runs on the replica owning
// the instance's partition, whichever replica's scheduler claimed it.
const CmdFireTimer = "FireTimer"

type TimerScheduler struct {
	db           *sqlx.DB
	dispatcher   *events.TaskEventDispatcher
	engineRepo   *repository.EngineRepository
	stopChan     chan bool
	resumer      func(ctx context.Context, execID uuid.UUID) error
	auditService *AuditService
	// startTimer fires a timer start event (runtime.PostStartTimer; the
	// runtime package can't be imported from here).
	startTimer func(ctx context.Context, timer models.TimerJob) error
}

// SetStartTimerHandler sets what fires timer start events.
func (s *TimerScheduler) SetStartTimerHandler(h func(ctx context.Context, timer models.TimerJob) error) {
	s.startTimer = h
}

func NewTimerScheduler(db *sqlx.DB, resumer func(ctx context.Context, execID uuid.UUID) error, dispatcher *events.TaskEventDispatcher) *TimerScheduler {
	s := &TimerScheduler{
		db:           db,
		dispatcher:   dispatcher,
		engineRepo:   repository.NewEngineRepository(db, dispatcher),
		stopChan:     make(chan bool),
		resumer:      resumer,
		auditService: NewAuditService(db),
	}
	cmdbus.Register(CmdFireTimer, func(ctx context.Context, raw json.RawMessage) (any, error) {
		var timer models.TimerJob
		if err := json.Unmarshal(raw, &timer); err != nil {
			return nil, err
		}
		s.fireTimer(ctx, timer)
		return nil, nil
	})
	return s
}

// Start begins the timer scheduler poller
func (s *TimerScheduler) Start(ctx context.Context) {
	// Twice a second: a timer fires at most ~0.5 s late, and a cycle
	// (each occurrence armed when the previous one fires) keeps up.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	log.Println("Timer scheduler started")

	for {
		select {
		case <-ticker.C:
			s.processDueTimers(ctx)
		case <-s.stopChan:
			log.Println("Timer scheduler stopped")
			return
		case <-ctx.Done():
			log.Println("Timer scheduler context cancelled")
			return
		}
	}
}

// Stop stops the timer scheduler
func (s *TimerScheduler) Stop() {
	s.stopChan <- true
}

// processDueTimers atomically claims and fires all due timers. Claiming
// (EngineRepository.ClaimDueTimers) stays global and unconditional for
// every instance, partitioned or not — it only flips timer_jobs.is_triggered,
// never touches executions, so there's nothing here for Kafka partition
// ownership to race against (see the design doc for the full reasoning).
func (s *TimerScheduler) processDueTimers(ctx context.Context) {
	const batch = 20
	for {
		timers, err := s.engineRepo.ClaimDueTimers(ctx, batch)
		if err != nil {
			log.Printf("Error claiming due timers: %v", err)
			return
		}

		for _, timer := range timers {
			log.Printf("🔥 Processing timer %s (due_at: %s)", timer.ID, timer.DueAt)
			s.triggerTimer(ctx, timer)
		}
		// A full batch: more may be due, don't wait for the next tick.
		if len(timers) < batch || ctx.Err() != nil {
			return
		}
	}
}

// triggerTimer hands a claimed timer to whoever fires it: the owner of
// its instance's partition (directly in direct mode), or, for a timer
// start event, the start handler.
func (s *TimerScheduler) triggerTimer(ctx context.Context, timer models.TimerJob) {
	log.Printf("Triggering timer %s for process instance %s", timer.ID, timer.ProcessInstanceID)

	if timer.EventType == TimerStartEventType {
		if s.startTimer == nil {
			log.Printf("timer %s: no timer start handler set", timer.ID)
			return
		}
		if err := s.startTimer(ctx, timer); err != nil {
			log.Printf("timer start %s: %v", timer.ID, err)
		}
		return
	}

	if err := cmdbus.Post(ctx, cmdbus.Key(timer.ProcessInstanceID), CmdFireTimer, timer); err != nil {
		log.Printf("timer %s: %v", timer.ID, err)
		if cmdbus.Partitioned() {
			// Never reached its partition: claim it again on the next poll.
			if _, uerr := s.db.Exec(`UPDATE public.timer_jobs SET is_triggered = false, triggered_at = NULL WHERE id = $1`, timer.ID); uerr != nil {
				log.Printf("timer %s: re-arming after a failed send: %v", timer.ID, uerr)
			}
		}
	}
}

// fireTimer moves the timer's execution on, arms a cycle's next
// occurrence and continues the process.
func (s *TimerScheduler) fireTimer(ctx context.Context, timer models.TimerJob) {
	var execID uuid.UUID
	if timer.ExecutionID != nil {
		execID = *timer.ExecutionID
	}

	if execID != uuid.Nil {
		targetNodeID, err := ParseTimerTargetNode(timer.Payload)
		if err != nil {
			log.Printf("Failed to resume execution for timer %s: %v", timer.ID, err)
			return
		}
		if _, _, err := ResumeTimerExecution(s.db, s.dispatcher, timer.ProcessInstanceID, execID, targetNodeID); err != nil {
			if errors.Is(err, ErrStaleTimer) {
				// Disarmed after it was claimed: nothing to resume, and a
				// cycle must not arm its next occurrence.
				log.Printf("timer %s: its execution moved on meanwhile — ignored", timer.ID)
				return
			}
			log.Printf("Failed to resume execution for timer %s: %v", timer.ID, err)
			return
		}
	}

	s.scheduleNextCycleIfNeeded(ctx, timer)

	LogAuditErr("timer triggered", s.auditService.LogTimerTriggered(timer.ProcessInstanceID, timer.ID, timer.EventType))
	if p := eventlog.Global(); p != nil {
		p.TimerTriggered(timer.ProcessInstanceID.String(), timer.ID.String(), timer.EventType)
	}

	// Continue execution after ResumeTimerExecution's own transaction
	// committed.
	if execID != uuid.Nil && s.resumer != nil {
		log.Printf("🔄 Continuing execution %s after timer", execID)
		if err := s.resumer(ctx, execID); err != nil {
			log.Printf("❌ Failed to continue execution after timer: %v", err)
		}
	}
}

// scheduleNextCycleIfNeeded inserts the next timer_jobs row for a cycle
// timer.
func (s *TimerScheduler) scheduleNextCycleIfNeeded(ctx context.Context, timer models.TimerJob) {
	var payload map[string]interface{}
	if len(timer.Payload) > 0 {
		json.Unmarshal(timer.Payload, &payload)
	}
	isCycle, _ := payload["is_cycle"].(bool)
	if !isCycle {
		return
	}

	totalCycles, _ := payload["total_cycles"].(float64)
	repeatInterval, _ := payload["repeat_interval"].(string)
	currentCycle, _ := payload["current_cycle"].(float64)

	nextCycle := int(currentCycle) + 1
	totalCyclesInt := int(totalCycles)

	// Timers armed since internal/timer: the cycle computes its own next
	// occurrence (calendar durations, explicit start/end, cron).
	if kind, _ := payload["timer_kind"].(string); kind != "" {
		value, _ := payload["timer_value"].(string)
		t, err := timerpkg.Parse(timerpkg.Kind(kind), value)
		if err != nil {
			log.Printf("timer %s: %v", timer.ID, err)
			return
		}
		d := timer.DueAt
		prev := time.Date(d.Year(), d.Month(), d.Day(), d.Hour(), d.Minute(), d.Second(), d.Nanosecond(), time.Local)
		next, ok := t.Next(prev, nextCycle)
		for ok && next.Before(time.Now().Add(-time.Minute)) { // missed while down
			nextCycle++
			next, ok = t.Next(next, nextCycle)
		}
		if !ok {
			return
		}
		payload["current_cycle"] = nextCycle
		newPayload, _ := json.Marshal(payload)
		tx, err := s.db.BeginTxx(ctx, nil)
		if err != nil {
			log.Printf("timer %s: scheduling the next cycle: %v", timer.ID, err)
			return
		}
		defer tx.Rollback()
		if err := s.engineRepo.CreateTimerWithCycleTx(tx, uuid.New(), timer.ProcessInstanceID, timer.ExecutionID, next.In(time.Local), timer.EventType, newPayload, nextCycle, t.Repetitions, ""); err != nil {
			log.Printf("timer %s: scheduling the next cycle: %v", timer.ID, err)
			return
		}
		if err := tx.Commit(); err != nil {
			log.Printf("timer %s: scheduling the next cycle: %v", timer.ID, err)
		}
		return
	}

	// Convert PT10S to 10s for time.ParseDuration
	intervalStr := repeatInterval
	if strings.HasPrefix(intervalStr, "PT") {
		intervalStr = strings.TrimPrefix(intervalStr, "PT")
		intervalStr = strings.ToLower(intervalStr)
		intervalStr = strings.ReplaceAll(intervalStr, "H", "h")
		intervalStr = strings.ReplaceAll(intervalStr, "M", "m")
		intervalStr = strings.ReplaceAll(intervalStr, "S", "s")
	}

	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		log.Printf("Failed to parse repeat interval: %v, using default 10s", err)
		interval = 10 * time.Second
	}

	// totalCycles == -1 means infinite
	if totalCyclesInt != -1 && nextCycle >= totalCyclesInt {
		log.Printf("✅ Timer cycle completed for timer %s (final cycle)", timer.ID)
		return
	}

	nextDueAt := time.Now().UTC().Add(interval)
	payload["current_cycle"] = nextCycle
	newPayload, _ := json.Marshal(payload)

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction to schedule next cycle for timer %s: %v", timer.ID, err)
		return
	}
	defer tx.Rollback()

	nextTimerID := uuid.New()
	if err := s.engineRepo.CreateTimerWithDurationSeconds(tx, nextTimerID, timer.ProcessInstanceID, timer.ExecutionID, int(interval.Seconds()), timer.EventType, newPayload, nextCycle, totalCyclesInt, repeatInterval); err != nil {
		log.Printf("Failed to schedule next cycle: %v", err)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit next-cycle transaction for timer %s: %v", timer.ID, err)
		return
	}
	log.Printf("🔄 Scheduled cycle %d/%d for timer %s at %s", nextCycle, totalCyclesInt, timer.ID, nextDueAt)
}

// ParseTimerTargetNode extracts the target_node_id an intermediate timer's
// payload was created with (see internal/runtime/executor_timer.go's
// executeIntermediateTimer) — shared by TimerScheduler's own legacy path
// and internal/partition.TimerRouter, which needs it before producing a
// TimerFiredCommand.
func ParseTimerTargetNode(payload []byte) (string, error) {
	var parsed map[string]interface{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &parsed); err != nil {
			return "", fmt.Errorf("failed to parse timer payload: %w", err)
		}
	}
	targetNodeID, ok := parsed["target_node_id"].(string)
	if !ok {
		return "", fmt.Errorf("target_node_id not found in timer payload")
	}
	return targetNodeID, nil
}

// ResumeTimerExecution resumes an execution that was waiting for a timer:
// cancels any user tasks left in "created" state on it (see the doc
// comment on internal/runtime/executor_timer.go for why there's no
// separate branch for boundary vs. plain intermediate timers — this
// cancellation is a no-op today for any node type the partitioned engine
// currently supports, since it doesn't support user tasks yet), then
// moves current_element_id to targetNodeID — extracted from what used to
// be TimerScheduler's own private resumeExecution method, now shared with
// internal/partition's Partition Processor (see TimerPartitionRouter
// above) the same way SetJobRetries/ResolveIncident were extracted in the
// prior gap-closing pass.
// ErrStaleTimer: the execution a timer would resume no longer waits for it
// (its element was left, the timer disarmed, after the timer was claimed).
var ErrStaleTimer = errors.New("timer is stale: its execution no longer waits")

func ResumeTimerExecution(db *sqlx.DB, dispatcher *events.TaskEventDispatcher, processInstanceID, execID uuid.UUID, targetNodeID string) (fromNodeID string, cancelledTaskIDs []uuid.UUID, err error) {
	engineRepo := repository.NewEngineRepository(db, dispatcher)
	auditService := NewAuditService(db)

	tx, err := db.Beginx()
	if err != nil {
		return "", nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Locked: the timer and whatever left its element meanwhile (an
	// activity completing disarms its boundary timers) take turns. A timer
	// claimed just before being disarmed finds its execution no longer
	// waiting: it is stale.
	var at struct {
		Node   string `db:"current_element_id"`
		Status string `db:"status"`
	}
	if err := tx.Get(&at, `SELECT current_element_id, status FROM public.executions WHERE process_instance_id = $1 AND id = $2 FOR UPDATE`, processInstanceID, execID); err != nil {
		return "", nil, fmt.Errorf("failed to load current node: %w", err)
	}
	if at.Status != "waiting" {
		return at.Node, nil, ErrStaleTimer
	}
	fromNodeID = at.Node

	if err := tx.Select(&cancelledTaskIDs, `
		UPDATE public.tasks
		SET status = 'cancelled', updated_at = NOW()
		WHERE execution_id = $1 AND status = 'created'
		RETURNING id
	`, execID); err != nil {
		return "", nil, fmt.Errorf("failed to cancel user task: %w", err)
	}

	// An interrupting boundary timer on an embedded sub-process fires on the
	// execution parked on the sub-process node: interrupt everything still
	// running inside it. No-op for a timer on a plain activity, whose
	// execution has no children.
	scopeTasks, scopeJobs, err := engineRepo.CancelScopeTx(tx, processInstanceID, execID, uuid.Nil)
	if err != nil {
		return "", nil, fmt.Errorf("failed to interrupt sub-process: %w", err)
	}
	cancelledTaskIDs = append(cancelledTaskIDs, scopeTasks...)

	// An event-based gateway's competing message/signal events lose.
	if _, err := tx.Exec(`
		UPDATE event_subscriptions SET consumed_at = NOW()
		WHERE process_instance_id = $1 AND execution_id = $2 AND consumed_at IS NULL
		  AND event_type IN ('message', 'signal')
	`, processInstanceID, execID); err != nil {
		return "", nil, fmt.Errorf("failed to cancel competing events: %w", err)
	}

	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, processInstanceID, execID, "active", targetNodeID); err != nil {
		return "", nil, fmt.Errorf("failed to update execution: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Execution %s resumed after timer, moving to node %s (user task cancelled)", execID, targetNodeID)

	// processInstanceID is already known from the caller — no need to
	// re-discover it via a scatter-gathered GetExecutionByID lookup the
	// way this function used to (see architecture/citus-distributed-storage-design.md).
	for _, taskID := range cancelledTaskIDs {
		LogAuditErr("task cancelled", auditService.LogTaskCancelled(taskID, processInstanceID, "boundary timer fired"))
	}
	for _, jobID := range scopeJobs {
		LogAuditErr("job cancelled", auditService.LogJobFailed(jobID, processInstanceID, "cancelled: sub-process interrupted by boundary timer", 0))
	}
	LogAuditErr("execution moved", auditService.LogExecutionMoved(execID, processInstanceID, fromNodeID, targetNodeID, nil))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(processInstanceID.String(), execID.String(), fromNodeID, targetNodeID)
	}

	return fromNodeID, cancelledTaskIDs, nil
}
