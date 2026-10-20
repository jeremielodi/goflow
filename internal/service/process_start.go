// service/process_start.go
package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// ErrProcessAlreadyStarted is returned by StartProcessInstanceWithIDs when
// instanceID already has a process_instances row — see that function's doc
// for why this is a normal, expected outcome for its caller
// (internal/partition.Processor), not an error condition for callers using
// the plain StartProcessInstance (which always generates a fresh ID and so
// can never hit this).
var ErrProcessAlreadyStarted = fmt.Errorf("process instance already started")

// StartProcessInstance creates a process instance, its initial variables,
// and its first execution (parked at the start event) in one transaction,
// then logs PROCESS_STARTED + EXECUTION_CREATED. It does not execute the
// instance — callers own that (some run it synchronously and surface the
// error to an HTTP caller, the async worker path runs it in a goroutine),
// so this only returns the new IDs.
//
// This consolidates logic that was previously duplicated across four
// independent call sites (v1 StartProcess, start by definition id, v2
// CreateProcessInstance, and the async worker's processStartProcess) —
// duplication that already once caused two of those four paths to silently
// miss audit instrumentation.
func StartProcessInstance(
	db *sqlx.DB,
	graph *engine.ProcessGraph,
	processDefinitionID uuid.UUID,
	tenantID *string,
	variables map[string]interface{},
) (instanceID, execID uuid.UUID, err error) {
	instanceID = uuid.New()
	execID = uuid.New()
	if err := startProcessInstance(db, graph, processDefinitionID, instanceID, execID, tenantID, variables, ""); err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return instanceID, execID, nil
}

// StartProcessInstanceWithIDs is StartProcessInstance for callers that must
// choose the instance/execution IDs themselves before this function runs —
// specifically internal/partition.Processor, whose Command Gateway
// counterpart already committed to an instanceID (it's the Kafka partition
// key) before producing the command that eventually calls this. Returns
// ErrProcessAlreadyStarted if instanceID already exists: this is the
// expected shape of a redelivered command after a crash between this
// function's commit and its caller finishing the rest of the sequence
// (e.g. runtime.ExecuteExecution) — not a real failure. Callers must check
// for it with errors.Is and decide how to resume from there.
func StartProcessInstanceWithIDs(
	db *sqlx.DB,
	graph *engine.ProcessGraph,
	processDefinitionID, instanceID, execID uuid.UUID,
	tenantID *string,
	variables map[string]interface{},
	businessKey ...string,
) error {
	key := ""
	if len(businessKey) > 0 {
		key = businessKey[0]
	}
	return startProcessInstance(db, graph, processDefinitionID, instanceID, execID, tenantID, variables, key)
}

// StartProcessInstanceAt is StartProcessInstance starting at a given start
// event — a message, signal or timer start event — instead of the
// process's none start event.
func StartProcessInstanceAt(
	db *sqlx.DB,
	graph *engine.ProcessGraph,
	processDefinitionID uuid.UUID,
	tenantID *string,
	variables map[string]interface{},
	startNodeID string,
) (instanceID, execID uuid.UUID, err error) {
	instanceID = uuid.New()
	execID = uuid.New()
	if err := startProcessInstanceAt(db, processDefinitionID, instanceID, execID, tenantID, variables, startNodeID, ""); err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return instanceID, execID, nil
}

func startProcessInstance(
	db *sqlx.DB,
	graph *engine.ProcessGraph,
	processDefinitionID, instanceID, execID uuid.UUID,
	tenantID *string,
	variables map[string]interface{},
	businessKey string,
) error {
	var startNodeID string
	for _, node := range graph.Nodes {
		if node.IsProcessStart() {
			startNodeID = node.ID
			break
		}
	}
	if startNodeID == "" {
		return fmt.Errorf("no start event found in process definition")
	}
	return startProcessInstanceAt(db, processDefinitionID, instanceID, execID, tenantID, variables, startNodeID, businessKey)
}

func startProcessInstanceAt(
	db *sqlx.DB,
	processDefinitionID, instanceID, execID uuid.UUID,
	tenantID *string,
	variables map[string]interface{},
	startNodeID string,
	businessKey string,
) error {
	if variables == nil {
		variables = map[string]interface{}{}
	}

	instanceRepo := repository.NewProcessInstanceRepository(db)

	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now()

	if err = instanceRepo.CreateProcessInstanceTx(tx, instanceID, processDefinitionID, now, tenantID); err != nil {
		var pqErr *pq.Error
		var pgErr *pgconn.PgError // the pgx driver
		if (errors.As(err, &pqErr) && pqErr.Code == "23505") || (errors.As(err, &pgErr) && pgErr.Code == "23505") {
			return ErrProcessAlreadyStarted
		}
		return fmt.Errorf("failed to create process instance: %w", err)
	}
	if businessKey != "" {
		if _, err = tx.Exec(`UPDATE public.process_instances SET business_key = $1 WHERE id = $2`, businessKey, instanceID); err != nil {
			return fmt.Errorf("failed to set the business key: %w", err)
		}
	}
	if err = instanceRepo.CreateVariablesTx(tx, instanceID, variables, now); err != nil {
		return fmt.Errorf("failed to save process variables: %w", err)
	}

	if err = instanceRepo.CreateExecutionTx(tx, execID, instanceID, startNodeID, now); err != nil {
		return fmt.Errorf("failed to create execution: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	auditService := NewAuditService(db)
	LogAuditErr("process started", auditService.LogProcessStarted(instanceID, variables, nil))
	LogAuditErr("execution created", auditService.LogExecutionCreated(execID, instanceID, nil, startNodeID))

	if p := eventlog.Global(); p != nil {
		// Phase 0 shadow-mode event (see architecture/event-sourced-kafka-design.md).
		// processKey/version aren't available in this function's scope without
		// an extra lookup, so they're left blank here — the shadow consumer
		// only needs the event type + processInstanceId to verify the pipe.
		p.ProcessStarted(instanceID.String(), processDefinitionID.String(), "", 0)
		p.ExecutionCreated(instanceID.String(), execID.String(), "", startNodeID)
	}

	return nil
}
