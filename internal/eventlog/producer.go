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
// exactly the kind of behavioral change Phase 0 must never cause. A publish
// only queues the event (see Producer.send); a single sender writes the
// queue in the background, decoupled from the caller.
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

// Producer publishes domain events without ever blocking its caller:
// send queues the event, and a single sender goroutine writes the queue to
// Kafka in order, batching whatever is waiting. Events of one GoFlow
// instance therefore reach their partition in the order they happened
// (they used to be written by one goroutine each, in no particular order).
type Producer struct {
	writer *kafka.Writer
	queue  chan kafka.Message
}

// producerQueue bounds the events waiting to be written; beyond it, events
// are dropped (logged) rather than blocking the engine.
const producerQueue = 10000

// globalProducer is a process-wide singleton, set once at boot (or left nil
// if Phase 0 shadow-mode publishing isn't configured, e.g. in unit tests or
// `go run` outside Docker). This mirrors the codebase's existing pattern for
// process-wide singletons.
var globalProducer *Producer

// SetGlobalProducer installs p as the process-wide producer.
func SetGlobalProducer(p *Producer) {
	globalProducer = p
}

// Global returns the installed Producer, or nil if none has been configured.
func Global() *Producer {
	return globalProducer
}

// NewProducer builds a producer against the given brokers (e.g.
// []string{"redpanda:9092"}) and starts its sender.
func NewProducer(brokers []string) *Producer {
	p := &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        Topic,
			Balancer:     &kafka.Hash{}, // key (processInstanceId) determines partition
			RequiredAcks: kafka.RequireOne,
			BatchTimeout: 5 * time.Millisecond,
		},
		queue: make(chan kafka.Message, producerQueue),
	}
	go p.run()
	return p
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// run writes the queue in order: it takes every event already waiting
// (up to a batch) and writes them in one call.
func (p *Producer) run() {
	const maxBatch = 500
	batch := make([]kafka.Message, 0, maxBatch)
	for msg := range p.queue {
		batch = append(batch[:0], msg)
	collect:
		for len(batch) < maxBatch {
			select {
			case more := <-p.queue:
				batch = append(batch, more)
			default:
				break collect
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
		if err := p.writer.WriteMessages(ctx, batch...); err != nil {
			log.Printf("eventlog: failed to publish %d event(s): %v", len(batch), err)
		}
		cancel()
	}
}

// send queues the event and returns immediately — see the package doc
// comment for why this must never block the caller.
func (p *Producer) send(instanceID string, env *pb.Envelope) {
	env.EventId = uuid.NewString()
	env.ProcessInstanceId = instanceID
	env.OccurredAt = timestamppb.Now()
	data, err := proto.Marshal(env)
	if err != nil {
		log.Printf("eventlog: failed to marshal %s event for instance %s: %v", envelopeEventType(env), instanceID, err)
		return
	}
	select {
	case p.queue <- kafka.Message{Key: []byte(instanceID), Value: data}:
	default:
		log.Printf("eventlog: publish queue full, dropping %s event for instance %s", envelopeEventType(env), instanceID)
	}
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
	case *pb.Envelope_ExecutionCreated:
		return "ExecutionCreated"
	case *pb.Envelope_ExecutionMoved:
		return "ExecutionMoved"
	case *pb.Envelope_ExecutionCompleted:
		return "ExecutionCompleted"
	case *pb.Envelope_GatewayForked:
		return "GatewayForked"
	case *pb.Envelope_GatewayJoined:
		return "GatewayJoined"
	case *pb.Envelope_TimerTriggered:
		return "TimerTriggered"
	case *pb.Envelope_MessageCorrelated:
		return "MessageCorrelated"
	case *pb.Envelope_SignalBroadcast:
		return "SignalBroadcast"
	case *pb.Envelope_JobRetriesSet:
		return "JobRetriesSet"
	case *pb.Envelope_TaskCreated:
		return "TaskCreated"
	case *pb.Envelope_TaskClaimed:
		return "TaskClaimed"
	case *pb.Envelope_TaskUnclaimed:
		return "TaskUnclaimed"
	case *pb.Envelope_TaskCompleted:
		return "TaskCompleted"
	case *pb.Envelope_TaskCancelled:
		return "TaskCancelled"
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

func (p *Producer) ExecutionCreated(instanceID, executionID, parentExecutionID, nodeID string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_ExecutionCreated{ExecutionCreated: &pb.ExecutionCreated{
			ExecutionId:       executionID,
			ProcessInstanceId: instanceID,
			ParentExecutionId: parentExecutionID,
			NodeId:            nodeID,
		}},
	})
}

func (p *Producer) ExecutionMoved(instanceID, executionID, fromNode, toNode string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_ExecutionMoved{ExecutionMoved: &pb.ExecutionMoved{
			ExecutionId:       executionID,
			ProcessInstanceId: instanceID,
			FromNode:          fromNode,
			ToNode:            toNode,
		}},
	})
}

func (p *Producer) ExecutionCompleted(instanceID, executionID, nodeID string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_ExecutionCompleted{ExecutionCompleted: &pb.ExecutionCompleted{
			ExecutionId:       executionID,
			ProcessInstanceId: instanceID,
			NodeId:            nodeID,
		}},
	})
}

func (p *Producer) GatewayForked(instanceID, gatewayID string, childExecutions int32) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_GatewayForked{GatewayForked: &pb.GatewayForked{
			ProcessInstanceId: instanceID,
			GatewayId:         gatewayID,
			ChildExecutions:   childExecutions,
		}},
	})
}

func (p *Producer) GatewayJoined(instanceID, gatewayID string, expectedCount, receivedCount int32) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_GatewayJoined{GatewayJoined: &pb.GatewayJoined{
			ProcessInstanceId: instanceID,
			GatewayId:         gatewayID,
			ExpectedCount:     expectedCount,
			ReceivedCount:     receivedCount,
		}},
	})
}

func (p *Producer) TimerTriggered(instanceID, timerID, timerType string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_TimerTriggered{TimerTriggered: &pb.TimerTriggered{
			ProcessInstanceId: instanceID,
			TimerId:           timerID,
			TimerType:         timerType,
		}},
	})
}

func (p *Producer) MessageCorrelated(instanceID, messageName string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_MessageCorrelated{MessageCorrelated: &pb.MessageCorrelated{
			ProcessInstanceId: instanceID,
			MessageName:       messageName,
		}},
	})
}

// SignalBroadcast has no owning process instance — a broadcast spans zero or
// more instances — so it's published with an empty partition key.
func (p *Producer) SignalBroadcast(signalName string, resumedCount int32) {
	p.send("", &pb.Envelope{
		Event: &pb.Envelope_SignalBroadcast{SignalBroadcast: &pb.SignalBroadcast{
			SignalName:   signalName,
			ResumedCount: resumedCount,
		}},
	})
}

// JobRetriesSet is published unconditionally by service.SetJobRetries,
// unlike IncidentResolved which only fires when an open incident actually
// existed for the job.
func (p *Producer) JobRetriesSet(instanceID, jobID string, retries int32) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_JobRetriesSet{JobRetriesSet: &pb.JobRetriesSet{
			JobId:             jobID,
			ProcessInstanceId: instanceID,
			Retries:           retries,
		}},
	})
}

// Task* mirrors internal/events.TaskEventDispatcher's 5 transitions
// (created, claimed, unclaimed, completed, cancelled) — the only prior
// observability for tasks, entirely separate from this Kafka event log.

func (p *Producer) TaskCreated(instanceID, taskID, executionID, taskDefinitionKey, assignee string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_TaskCreated{TaskCreated: &pb.TaskCreated{
			TaskId:            taskID,
			ProcessInstanceId: instanceID,
			ExecutionId:       executionID,
			TaskDefinitionKey: taskDefinitionKey,
			Assignee:          assignee,
		}},
	})
}

func (p *Producer) TaskClaimed(instanceID, taskID, assignee string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_TaskClaimed{TaskClaimed: &pb.TaskClaimed{
			TaskId:            taskID,
			ProcessInstanceId: instanceID,
			Assignee:          assignee,
		}},
	})
}

func (p *Producer) TaskUnclaimed(instanceID, taskID string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_TaskUnclaimed{TaskUnclaimed: &pb.TaskUnclaimed{
			TaskId:            taskID,
			ProcessInstanceId: instanceID,
		}},
	})
}

func (p *Producer) TaskCompleted(instanceID, taskID string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_TaskCompleted{TaskCompleted: &pb.TaskCompleted{
			TaskId:            taskID,
			ProcessInstanceId: instanceID,
		}},
	})
}

func (p *Producer) TaskCancelled(instanceID, taskID, reason string) {
	p.send(instanceID, &pb.Envelope{
		Event: &pb.Envelope_TaskCancelled{TaskCancelled: &pb.TaskCancelled{
			TaskId:            taskID,
			ProcessInstanceId: instanceID,
			Reason:            reason,
		}},
	})
}
