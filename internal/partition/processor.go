package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/eventlog/pb"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
)

// GroupID identifies the Kafka consumer group every GoFlow replica's
// Partition Processor joins. Using kafka.ConsumerGroup directly (rather
// than the higher-level kafka.Reader) gives one dedicated goroutine per
// locally-assigned partition — see Run — which is what "one owner per
// partition" actually needs, and gets real partition assignment/rebalance
// across replicas from Kafka's own group protocol for free.
const GroupID = "goflow-partition-processor"

// Processor is the write side of Phase 1's partitioned engine — see the
// package doc in validate.go. It never invents new BPMN semantics: every
// command handler below ends by calling the exact same
// service.StartProcessInstance(WithIDs) / runtime.ExecuteExecution /
// service.CompleteJob functions the legacy synchronous HTTP/gRPC paths
// already call, reusing their existing transactional writes and
// audit/eventlog side effects verbatim. What's different is only *how*
// they're invoked: in strict per-partition order, driven by a consumed
// Kafka command instead of directly inside a request handler.
type Processor struct {
	db          *sqlx.DB
	dispatcher  *events.TaskEventDispatcher
	brokers     []string
	group       *kafka.ConsumerGroup
	processRepo *repository.ProcessRepository
}

func NewProcessor(brokers []string, db *sqlx.DB, dispatcher *events.TaskEventDispatcher) (*Processor, error) {
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      GroupID,
		Brokers: brokers,
		Topics:  []string{eventlog.CommandsTopic},
	})
	if err != nil {
		return nil, fmt.Errorf("partition processor: failed to join consumer group: %w", err)
	}
	return &Processor{
		db:          db,
		dispatcher:  dispatcher,
		brokers:     brokers,
		group:       group,
		processRepo: repository.NewProcessRepository(db),
	}, nil
}

// Run joins the consumer group and, for every partition this replica is
// assigned in each generation, starts a dedicated goroutine consuming that
// partition alone — see runPartition. Blocks until ctx is cancelled.
func (p *Processor) Run(ctx context.Context) {
	defer p.group.Close()
	for {
		if ctx.Err() != nil {
			return
		}
		gen, err := p.group.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("partition processor: failed to get next generation: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		for topic, assignments := range gen.Assignments {
			for _, assignment := range assignments {
				partitionID, offset := assignment.ID, assignment.Offset
				gen.Start(func(genCtx context.Context) {
					p.runPartition(genCtx, gen, topic, partitionID, offset)
				})
			}
		}
	}
}

// runPartition owns exactly one partition for the lifetime of the current
// generation — reassigned (to this or another replica) on the next
// rebalance. It has no per-instance in-memory state at all: every command
// handler reads/writes straight through to Postgres via the reused
// service/runtime functions, so a rebalance handoff is always safe —
// there's nothing to warm up or hand off, the next owner just picks up
// wherever those functions already left Postgres.
func (p *Processor) runPartition(ctx context.Context, gen *kafka.Generation, topic string, partitionID int, offset int64) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   p.brokers,
		Topic:     topic,
		Partition: partitionID,
	})
	defer reader.Close()
	reader.SetOffset(offset)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("partition processor: partition %d read error: %v", partitionID, err)
			continue
		}

		if err := p.handle(ctx, msg.Value); err != nil {
			// Not committing on failure means this message is redelivered
			// (to this or whichever replica owns the partition next) —
			// every handler below is written to be safe to redeliver, so
			// this is "retry", not "stuck".
			log.Printf("partition processor: partition %d offset %d failed, will retry: %v", partitionID, msg.Offset, err)
			continue
		}

		if err := gen.CommitOffsets(map[string]map[int]int64{topic: {partitionID: msg.Offset + 1}}); err != nil {
			log.Printf("partition processor: partition %d failed to commit offset %d: %v", partitionID, msg.Offset, err)
		}
	}
}

func (p *Processor) handle(ctx context.Context, data []byte) error {
	var env pb.CommandEnvelope
	if err := proto.Unmarshal(data, &env); err != nil {
		// A malformed command can never succeed on retry — log and
		// acknowledge it (return nil) so it doesn't wedge the partition.
		log.Printf("partition processor: malformed command envelope: %v", err)
		return nil
	}

	switch cmd := env.Command.(type) {
	case *pb.CommandEnvelope_StartProcess:
		return p.handleStartProcess(ctx, cmd.StartProcess)
	case *pb.CommandEnvelope_CompleteJob:
		return p.handleCompleteJob(ctx, cmd.CompleteJob)
	case *pb.CommandEnvelope_FailJob:
		return p.handleFailJob(ctx, cmd.FailJob)
	default:
		log.Printf("partition processor: unknown command type for instance %s", env.ProcessInstanceId)
		return nil
	}
}

func (p *Processor) handleStartProcess(ctx context.Context, cmd *pb.StartProcessCommand) error {
	instanceID, err := uuid.Parse(cmd.ProcessInstanceId)
	if err != nil {
		return fmt.Errorf("invalid process_instance_id %q: %w", cmd.ProcessInstanceId, err)
	}
	execID, err := uuid.Parse(cmd.ExecutionId)
	if err != nil {
		return fmt.Errorf("invalid execution_id %q: %w", cmd.ExecutionId, err)
	}
	definitionID, err := uuid.Parse(cmd.ProcessDefinitionId)
	if err != nil {
		return fmt.Errorf("invalid process_definition_id %q: %w", cmd.ProcessDefinitionId, err)
	}

	var tenantID *string
	if cmd.TenantId != "" {
		t := cmd.TenantId
		tenantID = &t
	}
	variables := map[string]interface{}{}
	if cmd.VariablesJson != "" {
		if err := json.Unmarshal([]byte(cmd.VariablesJson), &variables); err != nil {
			return fmt.Errorf("invalid variables JSON: %w", err)
		}
	}

	def, err := p.processRepo.FindProcessDefinitionByID(definitionID)
	if err != nil {
		return fmt.Errorf("load definition %s: %w", definitionID, err)
	}
	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return fmt.Errorf("parse graph: %w", err)
	}

	startErr := service.StartProcessInstanceWithIDs(p.db, &graph, definitionID, instanceID, execID, tenantID, variables)
	switch {
	case startErr == nil:
		rt := runtime.NewRuntime(&graph, p.db, p.dispatcher)
		if err := rt.ExecuteExecution(ctx, execID); err != nil {
			return fmt.Errorf("execute instance %s: %w", instanceID, err)
		}
		return nil

	case errors.Is(startErr, service.ErrProcessAlreadyStarted):
		// Redelivery after a crash between StartProcessInstanceWithIDs's
		// commit (which already published ProcessStarted — the Command
		// Gateway's wait may have already unblocked the original caller)
		// and ExecuteExecution finishing. Resume iff status == 'active':
		// every wait-state executor (executeServiceTask -> 'waiting',
		// executeEndEvent -> 'completed') flips status away from 'active'
		// atomically with its own side effect, in the same transaction —
		// unlike current_element_id, which ExecuteExecution's gateway-walk
		// case updates BEFORE the next node's own handler commits (so a
		// crash between "gateway resolved to taskA" and "taskA's job
		// committed" would leave current_element_id == taskA's ID with no
		// job yet — checking current_element_id against the start node
		// alone would misdiagnose this as "already fully applied" and
		// permanently strand the instance). status == 'active' is the only
		// state where nothing durable has happened yet at the current node.
		var status string
		if qerr := p.db.Get(&status, `SELECT status FROM public.executions WHERE id = $1`, execID); qerr != nil {
			return fmt.Errorf("check execution state for %s: %w", execID, qerr)
		}
		if status == "active" {
			rt := runtime.NewRuntime(&graph, p.db, p.dispatcher)
			if err := rt.ExecuteExecution(ctx, execID); err != nil {
				return fmt.Errorf("resume instance %s: %w", instanceID, err)
			}
		}
		return nil

	default:
		return fmt.Errorf("start process instance %s: %w", instanceID, startErr)
	}
}

func (p *Processor) handleCompleteJob(ctx context.Context, cmd *pb.CompleteJobCommand) error {
	jobID, err := uuid.Parse(cmd.JobId)
	if err != nil {
		return fmt.Errorf("invalid job_id %q: %w", cmd.JobId, err)
	}
	variables := map[string]interface{}{}
	if cmd.VariablesJson != "" {
		if err := json.Unmarshal([]byte(cmd.VariablesJson), &variables); err != nil {
			return fmt.Errorf("invalid variables JSON: %w", err)
		}
	}

	result, err := service.CompleteJob(p.db, jobID, variables)
	if errors.Is(err, service.ErrJobInvalidState) {
		// service.CompleteJob's own transaction (mark job completed +
		// advance the execution pointer) already committed on a prior
		// attempt, but the SUBSEQUENT ExecuteExecution walk below is a
		// separate commit — a crash in between is real and redeliverable,
		// the mirror image of handleStartProcess's ErrProcessAlreadyStarted
		// gap above. Check whether that walk still needs to happen.
		return p.resumeIfStranded(ctx, jobID)
	}
	if err != nil {
		return fmt.Errorf("complete job %s: %w", jobID, err)
	}
	if result.ProcessCompleted {
		// service.CompleteJob only completes the process itself when the
		// service task node has zero outgoing flows — not either of this
		// engine's validated shapes (every task flows to a gateway or an
		// end event via an explicit sequence flow), but handled anyway for
		// robustness.
		return nil
	}

	// The normal case for both validated shapes: every branch task's sole
	// outgoing flow targets the end event directly (ValidateShape enforces
	// this), so service.CompleteJob merely advanced the execution's
	// pointer to that end node — exactly like the legacy REST/gRPC
	// handlers, resuming via runtime.ExecuteExecution is what actually
	// reaches it and completes the process.
	rt := runtime.NewRuntime(result.Graph, p.db, p.dispatcher)
	if err := rt.ExecuteExecution(ctx, result.ExecutionID); err != nil {
		return fmt.Errorf("resume execution after completing job %s: %w", jobID, err)
	}
	return nil
}

func (p *Processor) handleFailJob(ctx context.Context, cmd *pb.FailJobCommand) error {
	jobID, err := uuid.Parse(cmd.JobId)
	if err != nil {
		return fmt.Errorf("invalid job_id %q: %w", cmd.JobId, err)
	}

	// service.FailJob has no ErrJobInvalidState-style sentinel for "this
	// job's outcome was already applied by a prior attempt" the way
	// CompleteJob does — check the job's status ourselves before ever
	// calling it, since re-running FailJob on an already-failed/already-
	// reset job would redo its Postgres writes (harmless for the plain
	// status fields, but the boundary-trigger branch would re-run
	// UpdateExecutionNodeTx and could overwrite an execution that already
	// advanced further via a completed resume).
	var status string
	if err := p.db.Get(&status, `SELECT status FROM public.jobs WHERE id = $1`, jobID); err != nil {
		return fmt.Errorf("check job state for %s: %w", jobID, err)
	}
	if status != "locked" {
		return p.resumeIfStranded(ctx, jobID)
	}

	result, err := service.FailJob(p.db, jobID, int(cmd.RetriesRemaining), cmd.ErrorMessage, cmd.ErrorCode)
	if err != nil {
		return fmt.Errorf("fail job %s: %w", jobID, err)
	}
	if !result.BoundaryTriggered {
		// Either a plain retry reset (job goes back to pending, a worker
		// will fetch it again later) or a permanent failure with an
		// incident created (service.FailJob's own transaction already
		// committed everything atomically) — neither needs a resume.
		return nil
	}

	// service.FailJob's boundary-trigger branch already committed moving
	// the execution's pointer to the boundary event's node — exactly like
	// service.CompleteJob's non-terminal case, resuming via
	// runtime.ExecuteExecution is what actually walks from there (the
	// ErrorBoundaryEventType case in runtime.go is already generic, same
	// as every other node type reused by this package).
	rt := runtime.NewRuntime(result.Graph, p.db, p.dispatcher)
	if err := rt.ExecuteExecution(ctx, result.ExecutionID); err != nil {
		return fmt.Errorf("resume execution after boundary-triggered failure for job %s: %w", jobID, err)
	}
	return nil
}

// resumeIfStranded handles redelivery where a job's terminal state was
// already reached by a prior attempt (CompleteJobCommand's
// ErrJobInvalidState branch, or FailJobCommand's own "job no longer
// locked" check — see handleFailJob) but the SUBSEQUENT ExecuteExecution
// walk may not have run yet. If the owning execution is still 'active'
// (see handleStartProcess's identical reasoning), that walk never happened
// and must be resumed now; otherwise a prior attempt already carried it
// through (or the job's outcome never needed one, e.g. a plain retry reset
// or an incident with no matching boundary) and this is a safe no-op.
func (p *Processor) resumeIfStranded(ctx context.Context, jobID uuid.UUID) error {
	var row struct {
		ExecutionID       uuid.UUID `db:"execution_id"`
		ProcessInstanceID uuid.UUID `db:"process_instance_id"`
		Status            string    `db:"status"`
	}
	err := p.db.Get(&row, `
		SELECT j.execution_id, j.process_instance_id, e.status
		FROM public.jobs j
		JOIN public.executions e ON e.id = j.execution_id
		WHERE j.id = $1
	`, jobID)
	if err != nil {
		return fmt.Errorf("check execution state for job %s: %w", jobID, err)
	}
	if row.Status != "active" {
		return nil
	}

	engineRepo := repository.NewEngineRepository(p.db, p.dispatcher)
	graph, err := engineRepo.GetProcessGraphByInstanceID(row.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("load graph to resume job %s: %w", jobID, err)
	}
	rt := runtime.NewRuntime(graph, p.db, p.dispatcher)
	if err := rt.ExecuteExecution(ctx, row.ExecutionID); err != nil {
		return fmt.Errorf("resume execution after already-completed job %s: %w", jobID, err)
	}
	return nil
}
