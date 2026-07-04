// Package eventlog is GoFlow's Phase 0 ("shadow mode") event-sourcing
// scaffolding — see architecture/event-sourced-kafka-design.md. It publishes
// a domain event to a Kafka-compatible log (Redpanda in this dev stack)
// right after each existing Postgres transaction commits, purely to
// validate the event backbone. It does not replace, gate, or affect the
// existing write path in any way: every publish call is fire-and-forget —
// it returns immediately without blocking the caller on network I/O, and
// any failure is only logged. This isn't just an error-handling nicety: a
// synchronous publish (even one that only logs on failure) still adds a
// real network round-trip to the caller's critical path, and that latency
// once shifted the timing of a pre-existing, otherwise-benign race in the
// inclusive-gateway join logic into an actual duplicate-key failure —
// exactly the kind of behavioral change Phase 0 must never cause. Firing
// each publish on its own goroutine, decoupled from the caller's context,
// removes that risk entirely.
package eventlog

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jeremielodi/goflow/internal/eventlog/pb"
)

// Topic carries every event type for every process instance, partitioned by
// processInstanceId — mirrors Zeebe's own partitioning model (all events for
// one instance land in the same partition, in order, for its whole
// lifetime).
const Topic = "goflow.process-events"

// publishTimeout bounds each fire-and-forget publish's own background
// context — it is never tied to (or able to block) the caller's request.
const publishTimeout = 5 * time.Second

type Producer struct {
	writer *kafka.Writer
}

// globalProducer is a process-wide singleton, set once at boot (or left nil
// if Phase 0 shadow-mode publishing isn't configured, e.g. in unit tests or
// `go run` outside Docker). This mirrors the codebase's existing pattern for
// cross-cutting, best-effort infrastructure that many unrelated call sites
// need without threading a dependency through every layer — see
// repository.NotifyJobAvailable/JobWakeChannel. Every call site using
// Global() must treat a nil result as a no-op.
var globalProducer *Producer

// SetGlobalProducer installs the process-wide Producer used by call sites
// throughout internal/service and internal/runtime.
func SetGlobalProducer(p *Producer) {
	globalProducer = p
}

// Global returns the installed Producer, or nil if none has been configured.
func Global() *Producer {
	return globalProducer
}

// NewProducer builds a producer against the given brokers (e.g.
// []string{"redpanda:9092"}). The returned Producer is safe for concurrent
// use, matching kafka-go.Writer's own concurrency contract.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        Topic,
			Balancer:     &kafka.Hash{}, // key (processInstanceId) determines partition
			RequiredAcks: kafka.RequireOne,
		},
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// send fires the publish on its own goroutine and returns immediately —
// see the package doc comment for why this must never block the caller.
func (p *Producer) send(instanceID string, env *pb.Envelope) {
	env.EventId = uuid.NewString()
	env.ProcessInstanceId = instanceID
	env.OccurredAt = timestamppb.Now()
	eventType := envelopeEventType(env)

	go func() {
		data, err := proto.Marshal(env)
		if err != nil {
			log.Printf("eventlog: failed to marshal %s event for instance %s: %v", eventType, instanceID, err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
		defer cancel()
		if err := p.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(instanceID),
			Value: data,
		}); err != nil {
			log.Printf("eventlog: failed to publish %s event for instance %s: %v", eventType, instanceID, err)
		}
	}()
}

func envelopeEventType(env *pb.Envelope) string {
	switch env.Event.(type) {
	case *pb.Envelope_ProcessStarted:
		return "ProcessStarted"
	case *pb.Envelope_JobCreated:
		return "JobCreated"
	case *pb.Envelope_JobCompleted:
		return "JobCompleted"
	case *pb.Envelope_JobFailed:
		return "JobFailed"
	case *pb.Envelope_IncidentResolved:
		return "IncidentResolved"
	case *pb.Envelope_ProcessTerminated:
		return "ProcessTerminated"
	case *pb.Envelope_ProcessCompleted:
		return "ProcessCompleted"
	default:
		return "Unknown"
	}
}

func (p *Producer) ProcessStarted(instanceID, definitionID, processKey string, version int32) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_ProcessStarted{ProcessStarted: &pb.ProcessStarted{
			ProcessInstanceId:   instanceID,
			ProcessDefinitionId: definitionID,
			ProcessKey:          processKey,
			Version:             version,
		}},
	})
}

func (p *Producer) JobCreated(instanceID, jobID, executionID, jobType string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_JobCreated{JobCreated: &pb.JobCreated{
			JobId:             jobID,
			ProcessInstanceId: instanceID,
			ExecutionId:       executionID,
			JobType:           jobType,
		}},
	})
}

func (p *Producer) JobCompleted(instanceID, jobID string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_JobCompleted{JobCompleted: &pb.JobCompleted{
			JobId:             jobID,
			ProcessInstanceId: instanceID,
		}},
	})
}

func (p *Producer) JobFailed(instanceID, jobID, errorMessage string, retriesRemaining int32, incidentCreated bool) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_JobFailed{JobFailed: &pb.JobFailed{
			JobId:             jobID,
			ProcessInstanceId: instanceID,
			ErrorMessage:      errorMessage,
			RetriesRemaining:  retriesRemaining,
			IncidentCreated:   incidentCreated,
		}},
	})
}

func (p *Producer) IncidentResolved(instanceID, incidentID, jobID string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_IncidentResolved{IncidentResolved: &pb.IncidentResolved{
			IncidentId:        incidentID,
			JobId:             jobID,
			ProcessInstanceId: instanceID,
		}},
	})
}

func (p *Producer) ProcessTerminated(instanceID, reason string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_ProcessTerminated{ProcessTerminated: &pb.ProcessTerminated{
			ProcessInstanceId: instanceID,
			Reason:            reason,
		}},
	})
}

func (p *Producer) ProcessCompleted(instanceID string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_ProcessCompleted{ProcessCompleted: &pb.ProcessCompleted{
			ProcessInstanceId: instanceID,
		}},
	})
}
