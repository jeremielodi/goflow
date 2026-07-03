// internal/service/job_service.go
package service

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
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
	processInstanceRepo := repository.NewProcessInstanceRepository(db)
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

	// zeebe:ioMapping outputs: if the completed activity declares output
	// mappings, only the mapped (renamed) values propagate to the process
	// scope instead of the raw job variables.
	if graphJSON, gerr := externalTaskRepo.GetProcessDefinitionGraphTx(tx, jobWithExec.ProcessInstanceID); gerr == nil {
		var g engine.ProcessGraph
		if json.Unmarshal(graphJSON, &g) == nil {
			if n, ok := g.Nodes[jobWithExec.CurrentElementID]; ok && len(n.OutputMappings) > 0 {
				variables = common.EvaluateIOMappings(n.OutputMappings, variables)
			}
		}
	}

	// Merge variables.
	existingVars, err := processInstanceRepo.GetVariables(jobWithExec.ProcessInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch variables: %w", err)
	}
	for k, v := range variables {
		existingVars[k] = v
	}
	if err := processInstanceRepo.UpsertVariablesTx(tx, jobWithExec.ProcessInstanceID, existingVars); err != nil {
		return nil, fmt.Errorf("failed to update variables: %w", err)
	}

	if err := externalTaskRepo.CompleteJobTx(tx, jobID); err != nil {
		return nil, fmt.Errorf("failed to complete job: %w", err)
	}

	processInstanceID := jobWithExec.ProcessInstanceID
	executionID := jobWithExec.ExecutionID
	currentElementID := jobWithExec.CurrentElementID

	// Check if this is a multi-instance child.
	var isChild bool
	var parentExecID uuid.UUID
	var miParentMoved bool
	var miParentExecID uuid.UUID
	var miParentFromNode, miParentToNode string

	err = tx.Get(&isChild, `
		SELECT EXISTS(SELECT 1 FROM public.multi_instance_children WHERE child_execution_id = $1)
	`, executionID)

	if err == nil && isChild {
		log.Printf("📦 Multi-instance child completed: %s", executionID)

		if _, err = tx.Exec(`
			UPDATE public.multi_instance_children
			SET status = 'completed', completed_at = NOW()
			WHERE child_execution_id = $1
		`, executionID); err != nil {
			return nil, err
		}

		if err = tx.Get(&parentExecID, `
			SELECT parent_execution_id FROM public.multi_instance_children
			WHERE child_execution_id = $1
		`, executionID); err != nil {
			return nil, err
		}

		if _, err = tx.Exec(`
			UPDATE public.multi_instance_executions
			SET completed_count = (
				SELECT COUNT(*) FROM public.multi_instance_children
				WHERE parent_execution_id = $1 AND status = 'completed'
			),
			updated_at = NOW()
			WHERE execution_id = $1
		`, parentExecID); err != nil {
			log.Printf("Warning: failed to update completed count: %v", err)
		}

		var completedCount, totalCount int
		tx.Get(&completedCount, `
			SELECT COUNT(*) FROM public.multi_instance_children
			WHERE parent_execution_id = $1 AND status = 'completed'
		`, parentExecID)
		tx.Get(&totalCount, `
			SELECT total_count FROM public.multi_instance_executions
			WHERE execution_id = $1
		`, parentExecID)

		log.Printf("📊 Multi-instance progress: %d/%d completed", completedCount, totalCount)

		var isSequential bool
		tx.Get(&isSequential, `
			SELECT is_sequential FROM public.multi_instance_executions
			WHERE execution_id = $1
		`, parentExecID)

		if isSequential && completedCount < totalCount {
			log.Printf("🔄 Sequential: Creating next child %d/%d", completedCount+1, totalCount)

			var activityID string
			tx.Get(&activityID, `
        SELECT activity_id FROM public.multi_instance_executions
        WHERE execution_id = $1
    `, parentExecID)

			if _, err = tx.Exec(`
        UPDATE public.executions
        SET current_element_id = $1, status = 'active', updated_at = NOW()
        WHERE id = $2
    `, activityID, parentExecID); err != nil {
				log.Printf("Warning: failed to update parent: %v", err)
			}

			if _, err = tx.Exec(`
        UPDATE public.multi_instance_executions
        SET loop_counter = $1, updated_at = NOW()
        WHERE execution_id = $2
    `, completedCount+1, parentExecID); err != nil {
				log.Printf("Warning: failed to update loop counter: %v", err)
			}
		}

		if completedCount >= totalCount && totalCount > 0 {
			log.Printf("✅ All %d children completed", totalCount)

			var parentNodeID string
			tx.Get(&parentNodeID, `SELECT current_element_id FROM public.executions WHERE id = $1`, parentExecID)

			graphJSON, err := externalTaskRepo.GetProcessDefinitionGraphTx(tx, jobWithExec.ProcessInstanceID)
			if err == nil {
				var graph engine.ProcessGraph
				json.Unmarshal(graphJSON, &graph)

				if parentNode, ok := graph.Nodes[parentNodeID]; ok && len(parentNode.Outgoing) > 0 {
					nextNodeID := parentNode.Outgoing[0].TargetRef
					if _, err = tx.Exec(`
						UPDATE public.executions
						SET current_element_id = $1, status = 'active', updated_at = NOW()
						WHERE id = $2
					`, nextNodeID, parentExecID); err != nil {
						log.Printf("Warning: failed to move parent: %v", err)
					} else {
						miParentMoved = true
						miParentExecID = parentExecID
						miParentFromNode = parentNodeID
						miParentToNode = nextNodeID
					}
				}
			}
		}
	}

	// Load process graph for normal flow.
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

	var nextNodeID string

	switch currentNode.Type {
	case engine.ServiceTaskType:
		if len(currentNode.Outgoing) == 0 {
			if err := externalTaskRepo.CompleteProcessInstanceTx(tx, processInstanceID); err != nil {
				return nil, err
			}
			if err := externalTaskRepo.CompleteExecutionTx(tx, executionID); err != nil {
				return nil, err
			}
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			LogAuditErr("job completed", auditService.LogJobCompleted(jobID, processInstanceID, variables))
			if len(variables) > 0 {
				LogAuditErr("variables merged", auditService.LogVariablesMerged(processInstanceID, variables, "jobComplete"))
			}
			LogAuditErr("execution completed", auditService.LogExecutionCompleted(executionID, processInstanceID, currentElementID))
			LogAuditErr("process completed", auditService.LogProcessCompleted(processInstanceID, existingVars))
			return &CompleteJobResult{ProcessInstanceID: processInstanceID, ProcessCompleted: true}, nil
		}
		nextNodeID, err = common.ResolveNext(currentNode, existingVars)
		if err != nil {
			return nil, err
		}

	case engine.BoundaryTimerEventType:
		if len(currentNode.Outgoing) == 0 {
			return nil, fmt.Errorf("boundary event has no outgoing flow")
		}
		nextNodeID = currentNode.Outgoing[0].TargetRef

	default:
		return nil, fmt.Errorf("node %s is not a service task or boundary event, type: %s", currentNode.ID, currentNode.Type)
	}

	if err := externalTaskRepo.UpdateExecutionCurrentNodeTx(tx, executionID, nextNodeID); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	LogAuditErr("job completed", auditService.LogJobCompleted(jobID, processInstanceID, variables))
	if len(variables) > 0 {
		LogAuditErr("variables merged", auditService.LogVariablesMerged(processInstanceID, variables, "jobComplete"))
	}
	if miParentMoved {
		LogAuditErr("execution moved", auditService.LogExecutionMoved(miParentExecID, processInstanceID, miParentFromNode, miParentToNode, nil))
	}
	LogAuditErr("execution moved", auditService.LogExecutionMoved(executionID, processInstanceID, currentElementID, nextNodeID, existingVars))

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

	result := &FailJobResult{}

	if retriesRemaining <= 0 {
		result.PermanentlyFailed = true

		if err := externalTaskRepo.FailJobPermanentlyTx(tx, jobID, errorMessage, errorCode); err != nil {
			return nil, fmt.Errorf("failed to mark job as failed: %w", err)
		}

		jobCtx, jobErr := jobCtxEarly, jobCtxErr

		// If an error code was provided, check for a matching error boundary event.
		if errorCode != "" && jobErr == nil && jobCtx != nil {
			graphJSON, graphErr := externalTaskRepo.GetProcessDefinitionGraphTx(tx, jobCtx.ProcessInstanceID)
			if graphErr == nil {
				var graph engine.ProcessGraph
				if jsonErr := json.Unmarshal(graphJSON, &graph); jsonErr == nil {
					for _, node := range graph.Nodes {
						if node.Type == engine.ErrorBoundaryEventType &&
							node.AttachedToRef == jobCtx.CurrentElementID &&
							node.ErrorCode == errorCode {
							if _, execErr := tx.Exec(`
								UPDATE public.executions
								SET current_element_id = $1, status = 'active', updated_at = NOW()
								WHERE id = $2
							`, node.ID, jobCtx.ExecutionID); execErr == nil {
								if commitErr := tx.Commit(); commitErr == nil {
									LogAuditErr("job failed", auditService.LogJobFailed(jobID, jobCtx.ProcessInstanceID, errorMessage, 0))
									LogAuditErr("execution moved", auditService.LogExecutionMoved(jobCtx.ExecutionID, jobCtx.ProcessInstanceID, jobCtx.CurrentElementID, node.ID, nil))
									result.BoundaryTriggered = true
									result.Graph = &graph
									result.ExecutionID = jobCtx.ExecutionID
								}
								return result, nil
							}
							break
						}
					}
				}
			}
		}

		// No boundary event handled the failure — create an incident.
		if jobErr == nil && jobCtx != nil {
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
		if err := externalTaskRepo.ResetJobForRetryTx(tx, jobID, retriesRemaining, errorMessage); err != nil {
			return nil, fmt.Errorf("failed to reset job for retry: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	if jobCtxErr == nil && jobCtxEarly != nil {
		LogAuditErr("job failed", auditService.LogJobFailed(jobID, jobCtxEarly.ProcessInstanceID, errorMessage, retriesRemaining))
	}

	return result, nil
}
