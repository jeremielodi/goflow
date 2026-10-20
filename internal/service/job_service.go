// internal/service/job_service.go
package service

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// ErrJobNotFound and ErrJobInvalidState let callers (REST controllers,
// the gRPC gateway) distinguish these two cases from a generic failure
// via errors.Is, while still getting a human-readable message from
// err.Error().
var (
	ErrJobNotFound     = fmt.Errorf("job not found")
	ErrJobInvalidState = fmt.Errorf("job is not in a lockable state")
)

type CompleteJobResult struct {
	ProcessInstanceID uuid.UUID
	ProcessCompleted  bool
	// Graph/ExecutionID are set when ProcessCompleted is false — the
	// caller must resume execution itself (runtime.NewRuntime(Graph, db,
	// dispatcher).ExecuteExecution(ctx, ExecutionID)) since internal/
	// runtime already imports internal/service, so this package cannot
	// call into runtime directly without an import cycle. This mirrors
	// StartProcessInstance, which leaves execution to its callers too.
	Graph       *engine.ProcessGraph
	ExecutionID uuid.UUID
}

// CompleteJob completes a locked external task/job — extracted from what
// used to be the entire inline body of
// internal/api/external_task_controller.go's CompleteTask handler, now
// shared with the gRPC gateway's CompleteJob RPC. variables is already
// normalized to simple (non-typed) map[string]interface{} form.
func CompleteJob(db *sqlx.DB, jobID uuid.UUID, variables map[string]interface{}) (*CompleteJobResult, error) {
	externalTaskRepo := repository.NewExternalTaskRepository(db)
	auditService := NewAuditService(db)

	tx, err := db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	jobWithExec, err := externalTaskRepo.GetJobWithExecutionTx(tx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch job details: %w", err)
	}
	if jobWithExec == nil {
		return nil, ErrJobNotFound
	}
	if jobWithExec.Status != "locked" {
		return nil, fmt.Errorf("%w: current status %s", ErrJobInvalidState, jobWithExec.Status)
	}

	processInstanceID := jobWithExec.ProcessInstanceID
	executionID := jobWithExec.ExecutionID
	currentElementID := jobWithExec.CurrentElementID

	graphJSON, err := externalTaskRepo.GetProcessDefinitionGraphTx(tx, processInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to load process definition: %w", err)
	}
	var graph engine.ProcessGraph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return nil, fmt.Errorf("failed to parse process graph: %w", err)
	}
	currentNode, err := externalTaskRepo.GetNodeByID(&graph, currentElementID)
	if err != nil {
		return nil, fmt.Errorf("current node not found: %w", err)
	}

	// The job's variables, with the activity's output mappings, as in
	// Camunda 8.
	if err := ApplyActivityOutputTx(tx, processInstanceID, executionID, currentNode, variables); err != nil {
		return nil, fmt.Errorf("failed to update variables: %w", err)
	}

	if err := externalTaskRepo.CompleteJobTx(tx, processInstanceID, jobID); err != nil {
		return nil, fmt.Errorf("failed to complete job: %w", err)
	}

	// Where the token goes next. Without an outgoing flow it ends in the
	// activity's scope (engine.ImplicitEnd), and the runtime takes it from
	// there — ending a sub-process, an iteration of a multi-instance
	// activity (the runtime notices an inner instance leaving its
	// activity), or the process.
	var nextNodeID string
	switch currentNode.Type {
	case engine.ServiceTaskType:
		if nextNodeID, err = common.ResolveNext(currentNode, nil); err != nil {
			return nil, err
		}

	case engine.BoundaryTimerEventType:
		if nextNodeID, err = common.ResolveNext(currentNode, nil); err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("node %s is not a service task or boundary event, type: %s", currentNode.ID, currentNode.Type)
	}

	// The job's activity is left: its boundary events can no longer fire,
	// and it can now be compensated.
	completionRepo := repository.NewEngineRepository(db, nil)
	if err := completionRepo.DisarmBoundaryTriggersTx(tx, processInstanceID, executionID); err != nil {
		return nil, fmt.Errorf("failed to disarm boundary events: %w", err)
	}
	if currentNode.CompensationHandler != "" {
		if err := completionRepo.RecordCompensableTx(tx, processInstanceID, executionID, currentNode.ID, currentNode.CompensationHandler); err != nil {
			return nil, fmt.Errorf("failed to record compensation: %w", err)
		}
	}
	if err := externalTaskRepo.UpdateExecutionCurrentNodeTx(tx, processInstanceID, executionID, nextNodeID); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	LogAuditErr("job completed", auditService.LogJobCompleted(jobID, processInstanceID, variables))
	if len(variables) > 0 {
		LogAuditErr("variables merged", auditService.LogVariablesMerged(processInstanceID, variables, "jobComplete"))
	}
	LogAuditErr("execution moved", auditService.LogExecutionMoved(executionID, processInstanceID, currentElementID, nextNodeID, variables))

	if p := eventlog.Global(); p != nil {
		p.JobCompleted(processInstanceID.String(), jobID.String())
		p.ExecutionMoved(processInstanceID.String(), executionID.String(), currentElementID, nextNodeID)
	}

	return &CompleteJobResult{ProcessInstanceID: processInstanceID, ProcessCompleted: false, Graph: &graph, ExecutionID: executionID}, nil
}

type FailJobResult struct {
	PermanentlyFailed bool
	BoundaryTriggered bool
	// Graph/ExecutionID are set when BoundaryTriggered is true — the
	// caller should resume execution itself (see CompleteJobResult for
	// why: avoiding an internal/runtime <-> internal/service import
	// cycle). Unlike CompleteJob, a resume failure here is non-fatal —
	// the failure was already durably recorded, so log and move on,
	// matching the original inline behavior.
	Graph       *engine.ProcessGraph
	ExecutionID uuid.UUID
	// PropagateError is set when the BPMN error is caught by a call
	// activity above ProcessInstanceID: the caller must hand it up with
	// runtime.PropagateErrorToCaller (no incident was raised).
	PropagateError    bool
	ProcessInstanceID uuid.UUID
}

// FailJob records a job failure — extracted from what used to be the
// entire inline body of
// internal/api/external_task_controller.go's HandleFailure handler, now
// shared with the gRPC gateway's FailJob RPC.
//
// retriesRemaining is the number of retries to leave AFTER this failure
// (Zeebe's own convention) — callers using Camunda 7's "current retry
// count" convention must decrement before calling this (see
// ExternalTaskController.HandleFailure, which does req.Retries-1 before
// calling this so its own REST semantics are unchanged).
func FailJob(db *sqlx.DB, jobID uuid.UUID, retriesRemaining int, errorMessage, errorCode string) (*FailJobResult, error) {
	externalTaskRepo := repository.NewExternalTaskRepository(db)
	incidentRepo := repository.NewIncidentRepository(db)
	auditService := NewAuditService(db)

	tx, err := db.Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Load job context up front — needed for audit logging on every
	// branch, as well as the boundary-check/incident-creation below.
	jobCtxEarly, jobCtxErr := externalTaskRepo.GetJobWithExecutionTx(tx, jobID)
	if jobCtxErr != nil {
		return nil, fmt.Errorf("failed to fetch job details: %w", jobCtxErr)
	}
	if jobCtxEarly == nil {
		return nil, ErrJobNotFound
	}
	processInstanceID := jobCtxEarly.ProcessInstanceID

	result := &FailJobResult{}

	if retriesRemaining <= 0 {
		result.PermanentlyFailed = true

		if err := externalTaskRepo.FailJobPermanentlyTx(tx, processInstanceID, jobID, errorMessage, errorCode); err != nil {
			return nil, fmt.Errorf("failed to mark job as failed: %w", err)
		}

		jobCtx, jobErr := jobCtxEarly, jobCtxErr

		// If an error code was provided, check for a matching error boundary event.
		if errorCode != "" && jobErr == nil && jobCtx != nil {
			graphJSON, graphErr := externalTaskRepo.GetProcessDefinitionGraphTx(tx, jobCtx.ProcessInstanceID)
			if graphErr == nil {
				var graph engine.ProcessGraph
				if jsonErr := json.Unmarshal(graphJSON, &graph); jsonErr == nil {
					node, scope := graph.FindErrorCatcher(jobCtx.CurrentElementID, errorCode)
					if node != nil && node.Type == engine.EventSubProcessType {
						// Caught by an error event sub-process: the caller
						// runs its trigger execution, which interrupts the
						// scope (this job's execution included) and starts
						// the event sub-process.
						engineRepo := repository.NewEngineRepository(db, nil)
						triggerID, trigErr := engineRepo.CreateErrorTriggerTx(tx, jobCtx.ProcessInstanceID, jobCtx.ExecutionID, node.ID, scope)
						if trigErr != nil {
							log.Printf("error event sub-process %s: %v", node.ID, trigErr)
						} else if commitErr := tx.Commit(); commitErr == nil {
							LogAuditErr("job failed", auditService.LogJobFailed(jobID, jobCtx.ProcessInstanceID, errorMessage, 0))
							if p := eventlog.Global(); p != nil {
								p.JobFailed(jobCtx.ProcessInstanceID.String(), jobID.String(), errorMessage, 0, false)
							}
							result.BoundaryTriggered = true
							result.Graph = &graph
							result.ExecutionID = triggerID
							return result, nil
						} else {
							return nil, fmt.Errorf("failed to commit transaction: %w", commitErr)
						}
					} else if node != nil {
						// Caught on the task itself, the job's execution
						// moves to the boundary. Caught on an enclosing
						// sub-process, the execution parked on that
						// sub-process moves instead, and everything still
						// running inside it is interrupted.
						catchingExecID, fromNodeID := jobCtx.ExecutionID, jobCtx.CurrentElementID
						var scopeTasks, scopeJobs []uuid.UUID
						var scopeErr error
						if scope != "" {
							engineRepo := repository.NewEngineRepository(db, nil)
							catchingExecID, scopeErr = engineRepo.FindScopeExecutionTx(tx, jobCtx.ProcessInstanceID, jobCtx.ExecutionID, scope)
							if scopeErr == nil {
								fromNodeID = scope
								scopeTasks, scopeJobs, scopeErr = engineRepo.CancelScopeTx(tx, jobCtx.ProcessInstanceID, catchingExecID, uuid.Nil)
							}
						}
						if scopeErr == nil {
							// The catching activity is left through its error
							// boundary: its other boundary events are disarmed.
							scopeErr = repository.NewEngineRepository(db, nil).DisarmBoundaryTriggersTx(tx, jobCtx.ProcessInstanceID, catchingExecID)
						}
						if scopeErr != nil {
							log.Printf("error boundary %s on sub-process %s: %v", node.ID, scope, scopeErr)
						} else if _, execErr := tx.Exec(`
							UPDATE public.executions
							SET current_element_id = $1, status = 'active', updated_at = NOW()
							WHERE process_instance_id = $2 AND id = $3
						`, node.ID, jobCtx.ProcessInstanceID, catchingExecID); execErr == nil {
							if commitErr := tx.Commit(); commitErr == nil {
								LogAuditErr("job failed", auditService.LogJobFailed(jobID, jobCtx.ProcessInstanceID, errorMessage, 0))
								for _, taskID := range scopeTasks {
									LogAuditErr("task cancelled", auditService.LogTaskCancelled(taskID, jobCtx.ProcessInstanceID, "sub-process interrupted by error boundary"))
								}
								for _, cancelledJobID := range scopeJobs {
									LogAuditErr("job cancelled", auditService.LogJobFailed(cancelledJobID, jobCtx.ProcessInstanceID, "cancelled: sub-process interrupted by error boundary", 0))
								}
								LogAuditErr("execution moved", auditService.LogExecutionMoved(catchingExecID, jobCtx.ProcessInstanceID, fromNodeID, node.ID, nil))
								if p := eventlog.Global(); p != nil {
									p.JobFailed(jobCtx.ProcessInstanceID.String(), jobID.String(), errorMessage, 0, false)
									p.ExecutionMoved(jobCtx.ProcessInstanceID.String(), catchingExecID.String(), fromNodeID, node.ID)
								}
								result.BoundaryTriggered = true
								result.Graph = &graph
								result.ExecutionID = catchingExecID
							}
							return result, nil
						}
					}
				}
			}
		}

		// Nothing in this instance catches the error: a call activity that
		// started it (or one further up) may — the caller then hands it up
		// (runtime.PropagateErrorToCaller). Otherwise, an incident.
		if errorCode != "" && jobErr == nil && jobCtx != nil && CallerCatchesError(tx, jobCtx.ProcessInstanceID, errorCode) {
			result.PropagateError = true
			result.ProcessInstanceID = jobCtx.ProcessInstanceID
		} else if jobErr == nil && jobCtx != nil {
			inc := &models.IncidentCreate{
				ProcessInstanceID: jobCtx.ProcessInstanceID,
				JobID:             &jobID,
				IncidentType:      "failedExternalTask",
				ActivityID:        jobCtx.CurrentElementID,
				ErrorMessage:      errorMessage,
				ErrorCode:         errorCode,
			}
			if _, err := incidentRepo.CreateIncidentTx(tx, inc); err != nil {
				log.Printf("failed to create incident: %v", err)
			}
		}
	} else {
		if err := externalTaskRepo.ResetJobForRetryTx(tx, processInstanceID, jobID, retriesRemaining, errorMessage); err != nil {
			return nil, fmt.Errorf("failed to reset job for retry: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	if jobCtxErr == nil && jobCtxEarly != nil {
		LogAuditErr("job failed", auditService.LogJobFailed(jobID, jobCtxEarly.ProcessInstanceID, errorMessage, retriesRemaining))
		if p := eventlog.Global(); p != nil {
			// incidentCreated is approximated as "permanently failed" here —
			// good enough for Phase 0 shadow verification; the exact incident
			// row's success/failure isn't tracked separately above.
			p.JobFailed(jobCtxEarly.ProcessInstanceID.String(), jobID.String(), errorMessage, int32(retriesRemaining), result.PermanentlyFailed)
		}
	}

	return result, nil
}
