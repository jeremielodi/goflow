// Phase 1 command production — see architecture/event-sourced-kafka-design.md.
// Unlike Producer (Phase 0's fire-and-forget domain-event publishing, which
// is best-effort observability), CommandProducer writes are the actual
// write path for process definitions flagged write_path = 'partitioned':
// a failed produce here must be surfaced to the HTTP/gRPC caller
// immediately, not logged-and-ignored, so every method is synchronous and
// returns its error.
package eventlog

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"github.com/jeremielodi/goflow/internal/eventlog/pb"
)

// CommandsTopic carries every command for every partitioned-write-path
// process instance, keyed by processInstanceId — must use the same key as
// Topic (goflow.process-events) so a given instance's commands and the
// domain events they produce are reasoned about consistently, even though
// they live on separate topics.
const CommandsTopic = "goflow.process-commands"

type CommandProducer struct {
	writer *kafka.Writer
}

var globalCommandProducer *CommandProducer

func SetGlobalCommandProducer(p *CommandProducer) {
	globalCommandProducer = p
}

// GlobalCommand returns the installed CommandProducer, or nil if Phase 1's
// partitioned write path isn't configured. Every call site must treat nil
// as "the partitioned write path is unavailable" and fail the request
// rather than silently falling back to the legacy path.
func GlobalCommand() *CommandProducer {
	return globalCommandProducer
}

func NewCommandProducer(brokers []string) *CommandProducer {
	return &CommandProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        CommandsTopic,
			Balancer: &kafka.Hash{},
			// Commands are the partitioned write path's only record of a
			// request until it's processed, so wait for every in-sync
			// replica (identical to RequireOne on a single broker).
			RequiredAcks: kafka.RequireAll,
		},
	}
}

func (p *CommandProducer) Close() error {
	return p.writer.Close()
}

func (p *CommandProducer) StartProcess(ctx context.Context, instanceID, execID, definitionID uuid.UUID, tenantID *string, variablesJSON string) error {
	tenant := ""
	if tenantID != nil {
		tenant = *tenantID
	}
	env := &pb.CommandEnvelope{
		CommandId:         uuid.NewString(),
		ProcessInstanceId: instanceID.String(),
		Command: &pb.CommandEnvelope_StartProcess{StartProcess: &pb.StartProcessCommand{
			ProcessInstanceId:   instanceID.String(),
			ExecutionId:         execID.String(),
			ProcessDefinitionId: definitionID.String(),
			TenantId:            tenant,
			VariablesJson:       variablesJSON,
		}},
	}
	return p.send(ctx, instanceID.String(), env)
}

func (p *CommandProducer) CompleteJob(ctx context.Context, instanceID, jobID uuid.UUID, variablesJSON string) error {
	env := &pb.CommandEnvelope{
		CommandId:         uuid.NewString(),
		ProcessInstanceId: instanceID.String(),
		Command: &pb.CommandEnvelope_CompleteJob{CompleteJob: &pb.CompleteJobCommand{
			JobId:         jobID.String(),
			VariablesJson: variablesJSON,
		}},
	}
	return p.send(ctx, instanceID.String(), env)
}

func (p *CommandProducer) FailJob(ctx context.Context, instanceID, jobID uuid.UUID, retriesRemaining int32, errorMessage, errorCode string) error {
	env := &pb.CommandEnvelope{
		CommandId:         uuid.NewString(),
		ProcessInstanceId: instanceID.String(),
		Command: &pb.CommandEnvelope_FailJob{FailJob: &pb.FailJobCommand{
			JobId:            jobID.String(),
			RetriesRemaining: retriesRemaining,
			ErrorMessage:     errorMessage,
			ErrorCode:        errorCode,
		}},
	}
	return p.send(ctx, instanceID.String(), env)
}

func (p *CommandProducer) SetJobRetries(ctx context.Context, instanceID, jobID uuid.UUID, retries int32) error {
	env := &pb.CommandEnvelope{
		CommandId:         uuid.NewString(),
		ProcessInstanceId: instanceID.String(),
		Command: &pb.CommandEnvelope_SetJobRetries{SetJobRetries: &pb.SetJobRetriesCommand{
			JobId:   jobID.String(),
			Retries: retries,
		}},
	}
	return p.send(ctx, instanceID.String(), env)
}

func (p *CommandProducer) ResolveIncident(ctx context.Context, instanceID, incidentID uuid.UUID) error {
	env := &pb.CommandEnvelope{
		CommandId:         uuid.NewString(),
		ProcessInstanceId: instanceID.String(),
		Command: &pb.CommandEnvelope_ResolveIncident{ResolveIncident: &pb.ResolveIncidentCommand{
			IncidentId: incidentID.String(),
		}},
	}
	return p.send(ctx, instanceID.String(), env)
}

func (p *CommandProducer) FireTimer(ctx context.Context, instanceID, timerID, execID uuid.UUID, targetNodeID string) error {
	env := &pb.CommandEnvelope{
		CommandId:         uuid.NewString(),
		ProcessInstanceId: instanceID.String(),
		Command: &pb.CommandEnvelope_TimerFired{TimerFired: &pb.TimerFiredCommand{
			TimerId:      timerID.String(),
			ExecutionId:  execID.String(),
			TargetNodeId: targetNodeID,
		}},
	}
	return p.send(ctx, instanceID.String(), env)
}

func (p *CommandProducer) Terminate(ctx context.Context, instanceID uuid.UUID, reason string) error {
	env := &pb.CommandEnvelope{
		CommandId:         uuid.NewString(),
		ProcessInstanceId: instanceID.String(),
		Command: &pb.CommandEnvelope_Terminate{Terminate: &pb.TerminateCommand{
			ProcessInstanceId: instanceID.String(),
			Reason:            reason,
		}},
	}
	return p.send(ctx, instanceID.String(), env)
}

func (p *CommandProducer) send(ctx context.Context, key string, env *pb.CommandEnvelope) error {
	data, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("eventlog: failed to marshal command envelope: %w", err)
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: data,
	}); err != nil {
		return fmt.Errorf("eventlog: failed to produce command: %w", err)
	}
	return nil
}
