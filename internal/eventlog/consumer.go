package eventlog

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"github.com/jeremielodi/goflow/internal/eventlog/pb"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// RecentEvent is the JSON-friendly, flattened view of a consumed Envelope
// exposed by the /engine-rest/eventlog/recent debug endpoint.
type RecentEvent struct {
	EventID           string      `json:"eventId"`
	ProcessInstanceID string      `json:"processInstanceId"`
	OccurredAt        time.Time   `json:"occurredAt"`
	Type              string      `json:"type"`
	Detail            interface{} `json:"detail"`
}

const ringBufferSize = 500

// Consumer reads Topic and keeps the last ringBufferSize events in memory,
// exposed via the debug ring buffer, and folds a subset of events (coarse
// process-instance/job state) into the eventlog_projected_* tables via
// projRepo — see architecture/event-sourced-kafka-design.md. This proves
// engine state is reconstructable from the event stream; it is NOT a real
// read-model replacement.
//
// Consumer also backs Phase 1's Command Gateway synchronous bridge (see
// internal/partition/gateway.go): WaitFor registers a channel keyed by
// instance ID, fed by the same per-event loop that already drives record/
// project, so any replica's HTTP handler can block for the domain event
// produced by whichever replica's Partition Processor actually handled the
// command — every replica already runs its own Consumer against the full
// topic, so this requires no new cross-replica coordination.
type Consumer struct {
	mu       sync.Mutex
	ring     []RecentEvent
	reader   *kafka.Reader
	projRepo *repository.EventLogProjectionRepository

	waitersMu sync.Mutex
	waiters   map[string][]*Waiter
}

// globalConsumer mirrors Producer's global-singleton pattern (see
// producer.go) — set once at boot, used by internal/partition/gateway.go's
// Command Gateway bridge without threading a Consumer dependency through
// internal/service.
var globalConsumer *Consumer

func SetGlobalConsumer(c *Consumer) {
	globalConsumer = c
}

func GlobalConsumer() *Consumer {
	return globalConsumer
}

func NewConsumer(brokers []string, groupID string, db *sqlx.DB) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   Topic,
			GroupID: groupID,
		}),
		projRepo: repository.NewEventLogProjectionRepository(db),
		waiters:  make(map[string][]*Waiter),
	}
}

// WaitFor blocks until an event of type kind is observed for instanceID, or
// timeout elapses. Split into Register (fast, synchronous) + Await
// (blocking) rather than one call, so callers can register interest
// *before* producing the command that will eventually cause the event —
// matching the snapshot-before-check discipline repository.JobWakeChannel
// already uses elsewhere in this codebase, to avoid missing an event that
// arrives in the gap between producing a command and starting to wait for
// its result.
func (c *Consumer) Register(instanceID, kind string) *Waiter {
	w := &Waiter{c: c, instanceID: instanceID, kind: kind, ch: make(chan *pb.Envelope, 1)}
	c.waitersMu.Lock()
	c.waiters[instanceID] = append(c.waiters[instanceID], w)
	c.waitersMu.Unlock()
	return w
}

// Waiter is a single registered interest in the next event of Kind for
// InstanceID, returned by Consumer.Register.
type Waiter struct {
	c          *Consumer
	instanceID string
	kind       string
	ch         chan *pb.Envelope
}

// Await blocks until the awaited event arrives, ctx is cancelled, or
// timeout elapses, then unregisters itself either way.
func (w *Waiter) Await(ctx context.Context, timeout time.Duration) (*pb.Envelope, error) {
	defer w.c.unregister(w)
	select {
	case env := <-w.ch:
		return env, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("eventlog: timed out after %s waiting for %s on instance %s", timeout, w.kind, w.instanceID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Cancel unregisters w without ever having received an event — for a
// caller that pre-registered a spare waiter (to avoid a delivery gap while
// re-registering after a mismatched event) but didn't end up needing it.
func (w *Waiter) Cancel() {
	w.c.unregister(w)
}

func (c *Consumer) unregister(w *Waiter) {
	c.waitersMu.Lock()
	defer c.waitersMu.Unlock()
	ws := c.waiters[w.instanceID]
	for i, cur := range ws {
		if cur == w {
			c.waiters[w.instanceID] = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(c.waiters[w.instanceID]) == 0 {
		delete(c.waiters, w.instanceID)
	}
}

// notifyWaiters delivers env to every waiter registered for its instance
// whose kind matches. Best-effort/at-most-once per waiter (buffered
// channel, single send) — a waiter that already timed out and unregistered
// just misses it, which is fine since WaitFor's caller treats a timeout as
// a real error already.
func (c *Consumer) notifyWaiters(instanceID, kind string, env *pb.Envelope) {
	c.waitersMu.Lock()
	defer c.waitersMu.Unlock()
	for _, w := range c.waiters[instanceID] {
		if w.kind == kind {
			select {
			case w.ch <- env:
			default:
			}
		}
	}
}

// Run consumes until ctx is cancelled. Intended to run in a background
// goroutine started once at boot, same lifecycle as
// repository.StartJobWakeListener.
func (c *Consumer) Run(ctx context.Context) {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("eventlog consumer: read error: %v", err)
			continue
		}

		var env pb.Envelope
		if err := proto.Unmarshal(msg.Value, &env); err != nil {
			log.Printf("eventlog consumer: malformed envelope: %v", err)
			continue
		}

		recent := toRecentEvent(&env)
		c.record(recent)
		c.project(&env)
		c.notifyWaiters(env.ProcessInstanceId, recent.Type, &env)
	}
}

// project folds a subset of events (coarse process-instance/job state) into
// the eventlog_projected_* tables. Scoped to what's needed to prove
// "instance/job state is reconstructable from events" — execution-level
// events (moves, gateways, timers, correlation) are visible via the debug
// ring buffer above but not folded into a table here. Errors are logged,
// never fatal to the consumer loop — a dropped projection update is a gap
// in this verification surface, not a problem for the real engine.
func (c *Consumer) project(env *pb.Envelope) {
	switch e := env.Event.(type) {
	case *pb.Envelope_ProcessStarted:
		id, err := uuid.Parse(e.ProcessStarted.ProcessInstanceId)
		if err != nil {
			return
		}
		var defID *uuid.UUID
		if parsed, err := uuid.Parse(e.ProcessStarted.ProcessDefinitionId); err == nil {
			defID = &parsed
		}
		if err := c.projRepo.UpsertInstance(id, defID, e.ProcessStarted.ProcessKey, e.ProcessStarted.Version, "running", env.OccurredAt.AsTime()); err != nil {
			log.Printf("eventlog projector: UpsertInstance failed: %v", err)
		}
	case *pb.Envelope_ProcessCompleted:
		id, err := uuid.Parse(e.ProcessCompleted.ProcessInstanceId)
		if err != nil {
			return
		}
		if err := c.projRepo.UpdateInstanceStatus(id, "completed", env.OccurredAt.AsTime()); err != nil {
			log.Printf("eventlog projector: UpdateInstanceStatus failed: %v", err)
		}
	case *pb.Envelope_ProcessTerminated:
		id, err := uuid.Parse(e.ProcessTerminated.ProcessInstanceId)
		if err != nil {
			return
		}
		if err := c.projRepo.UpdateInstanceStatus(id, "terminated", env.OccurredAt.AsTime()); err != nil {
			log.Printf("eventlog projector: UpdateInstanceStatus failed: %v", err)
		}
	case *pb.Envelope_JobCreated:
		jobID, err := uuid.Parse(e.JobCreated.JobId)
		if err != nil {
			return
		}
		instanceID, err := uuid.Parse(e.JobCreated.ProcessInstanceId)
		if err != nil {
			return
		}
		var execID *uuid.UUID
		if parsed, err := uuid.Parse(e.JobCreated.ExecutionId); err == nil {
			execID = &parsed
		}
		if err := c.projRepo.UpsertJob(jobID, instanceID, execID, e.JobCreated.JobType, "created"); err != nil {
			log.Printf("eventlog projector: UpsertJob failed: %v", err)
		}
	case *pb.Envelope_JobCompleted:
		jobID, err := uuid.Parse(e.JobCompleted.JobId)
		if err != nil {
			return
		}
		instanceID, err := uuid.Parse(e.JobCompleted.ProcessInstanceId)
		if err != nil {
			return
		}
		if err := c.projRepo.UpdateJobStatus(jobID, instanceID, "completed", "", nil); err != nil {
			log.Printf("eventlog projector: UpdateJobStatus failed: %v", err)
		}
	case *pb.Envelope_JobFailed:
		jobID, err := uuid.Parse(e.JobFailed.JobId)
		if err != nil {
			return
		}
		instanceID, err := uuid.Parse(e.JobFailed.ProcessInstanceId)
		if err != nil {
			return
		}
		retries := e.JobFailed.RetriesRemaining
		if err := c.projRepo.UpdateJobStatus(jobID, instanceID, "failed", e.JobFailed.ErrorMessage, &retries); err != nil {
			log.Printf("eventlog projector: UpdateJobStatus failed: %v", err)
		}
	}
}

func (c *Consumer) record(e RecentEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ring = append(c.ring, e)
	if len(c.ring) > ringBufferSize {
		c.ring = c.ring[len(c.ring)-ringBufferSize:]
	}
}

// Recent returns a snapshot of the most recently consumed events, oldest
// first.
func (c *Consumer) Recent() []RecentEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RecentEvent, len(c.ring))
	copy(out, c.ring)
	return out
}

func toRecentEvent(env *pb.Envelope) RecentEvent {
	base := RecentEvent{
		EventID:           env.EventId,
		ProcessInstanceID: env.ProcessInstanceId,
		OccurredAt:        env.OccurredAt.AsTime(),
	}
	switch e := env.Event.(type) {
	case *pb.Envelope_ProcessStarted:
		base.Type = "ProcessStarted"
		base.Detail = e.ProcessStarted
	case *pb.Envelope_JobCreated:
		base.Type = "JobCreated"
		base.Detail = e.JobCreated
	case *pb.Envelope_JobCompleted:
		base.Type = "JobCompleted"
		base.Detail = e.JobCompleted
	case *pb.Envelope_JobFailed:
		base.Type = "JobFailed"
		base.Detail = e.JobFailed
	case *pb.Envelope_IncidentResolved:
		base.Type = "IncidentResolved"
		base.Detail = e.IncidentResolved
	case *pb.Envelope_ProcessTerminated:
		base.Type = "ProcessTerminated"
		base.Detail = e.ProcessTerminated
	case *pb.Envelope_ProcessCompleted:
		base.Type = "ProcessCompleted"
		base.Detail = e.ProcessCompleted
	case *pb.Envelope_ExecutionCreated:
		base.Type = "ExecutionCreated"
		base.Detail = e.ExecutionCreated
	case *pb.Envelope_ExecutionMoved:
		base.Type = "ExecutionMoved"
		base.Detail = e.ExecutionMoved
	case *pb.Envelope_ExecutionCompleted:
		base.Type = "ExecutionCompleted"
		base.Detail = e.ExecutionCompleted
	case *pb.Envelope_GatewayForked:
		base.Type = "GatewayForked"
		base.Detail = e.GatewayForked
	case *pb.Envelope_GatewayJoined:
		base.Type = "GatewayJoined"
		base.Detail = e.GatewayJoined
	case *pb.Envelope_TimerTriggered:
		base.Type = "TimerTriggered"
		base.Detail = e.TimerTriggered
	case *pb.Envelope_MessageCorrelated:
		base.Type = "MessageCorrelated"
		base.Detail = e.MessageCorrelated
	case *pb.Envelope_SignalBroadcast:
		base.Type = "SignalBroadcast"
		base.Detail = e.SignalBroadcast
	default:
		base.Type = "Unknown"
	}
	return base
}
