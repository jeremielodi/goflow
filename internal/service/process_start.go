// service/process_start.go
package service

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// StartProcessInstance creates a process instance, its initial variables,
// and its first execution (parked at the start event) in one transaction,
// then logs PROCESS_STARTED + EXECUTION_CREATED. It does not execute the
// instance — callers own that (some run it synchronously and surface the
// error to an HTTP caller, the async worker path runs it in a goroutine),
// so this only returns the new IDs.
//
// This consolidates logic that was previously duplicated across four
// independent call sites (v1 StartProcess, StartByDefinitionID, v2
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
	var startNodeID string
	for _, node := range graph.Nodes {
		if node.Type == engine.StartEventType {
			startNodeID = node.ID
			break
		}
	}
	if startNodeID == "" {
		return uuid.Nil, uuid.Nil, fmt.Errorf("no start event found in process definition")
	}

	if variables == nil {
		variables = map[string]interface{}{}
	}

	instanceRepo := repository.NewProcessInstanceRepository(db)

	tx, err := db.Beginx()
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	instanceID = uuid.New()
	now := time.Now()

	if err = instanceRepo.CreateProcessInstanceTx(tx, instanceID, processDefinitionID, now, tenantID); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to create process instance: %w", err)
	}
	if err = instanceRepo.CreateVariablesTx(tx, instanceID, variables, now); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to save process variables: %w", err)
	}

	execID = uuid.New()
	if err = instanceRepo.CreateExecutionTx(tx, execID, instanceID, startNodeID, now); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to create execution: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	auditService := NewAuditService(db)
	LogAuditErr("process started", auditService.LogProcessStarted(instanceID, variables, nil))
	LogAuditErr("execution created", auditService.LogExecutionCreated(execID, instanceID, nil, startNodeID))

	return instanceID, execID, nil
}
