package eventlog

import (
	"context"
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
// read-model replacement or Phase 1's actual write path.
type Consumer struct {
	mu       sync.Mutex
	ring     []RecentEvent
	reader   *kafka.Reader
	projRepo *repository.EventLogProjectionRepository
}

func NewConsumer(brokers []string, groupID string, db *sqlx.DB) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   Topic,
			GroupID: groupID,
		}),
		projRepo: repository.NewEventLogProjectionRepository(db),
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

		c.record(toRecentEvent(&env))
		c.project(&env)
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
