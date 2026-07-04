package eventlog

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"github.com/jeremielodi/goflow/internal/eventlog/pb"
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

// Consumer reads Topic and keeps the last ringBufferSize events in memory.
// This is Phase 0 shadow-mode verification only — it proves the pipe is
// durable, ordered per instance, and well-formed. It is NOT a real
// projector; that's Phase 1+ (see architecture/event-sourced-kafka-design.md).
type Consumer struct {
	mu     sync.Mutex
	ring   []RecentEvent
	reader *kafka.Reader
}

func NewConsumer(brokers []string, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   Topic,
			GroupID: groupID,
		}),
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
	default:
		base.Type = "Unknown"
	}
	return base
}
