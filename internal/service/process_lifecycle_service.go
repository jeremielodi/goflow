// internal/service/process_lifecycle_service.go
package service

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// ErrProcessInstanceNotFound lets callers (REST controllers, the gRPC
// gateway) distinguish a missing instance from other termination errors via
// errors.Is, matching the ErrJobNotFound/ErrDeployPermissionDenied pattern
// used elsewhere in this package.
var ErrProcessInstanceNotFound = fmt.Errorf("process instance not found")

// TerminateProcessInstance soft-deletes a running process instance —
// extracted from what used to be the entire inline body of
// internal/api/process_instance_controller.go's TerminateProcess handler,
// now shared with the gRPC gateway's CancelProcessInstance RPC. Marks the
// instance (and everything still open under it) terminated instead of
// hard-deleting, cancels open tasks/jobs/timers, then best-effort archives
// into historic_* tables — see internal/service/archive_service.go. A
// failed archive leaves the instance live (already safely marked
// terminated above), never risking data loss.
func TerminateProcessInstance(db *sqlx.DB, dispatcher *events.TaskEventDispatcher, instanceID uuid.UUID, reason string) error {
	processInstanceRepo := repository.NewProcessInstanceRepository(db)
	taskRepo := repository.NewTaskRepository(db, dispatcher)
	engineRepo := repository.NewEngineRepository(db, dispatcher)
	auditService := NewAuditService(db)

	if _, err := processInstanceRepo.FindByID(instanceID); err != nil {
		return fmt.Errorf("%w: %s", ErrProcessInstanceNotFound, instanceID)
	}

	// Add termination reason as a variable.
	variables := map[string]interface{}{
		"terminated":        true,
		"terminatedAt":      time.Now(),
		"terminationReason": reason,
	}
	if err := processInstanceRepo.UpsertVariables(instanceID, variables); err != nil {
		fmt.Printf("Warning: Failed to add termination variables: %v\n", err)
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now()
	if err := processInstanceRepo.UpdateProcessInstanceStatusTx(tx, instanceID, "terminated", &now); err != nil {
		return err
	}

	// Explicitly clean up everything a hard-delete's FK cascade used to
	// clean up implicitly, so nothing is left looking open/active/pending
	// for a process that no longer runs.
	cancelledTaskIDs, err := taskRepo.CancelOpenTasksByProcessInstanceTx(tx, instanceID)
	if err != nil {
		return err
	}
	if err := processInstanceRepo.TerminateActiveExecutionsByProcessInstanceTx(tx, instanceID); err != nil {
		return err
	}
	cancelledJobIDs, err := engineRepo.CancelPendingJobsByProcessInstanceTx(tx, instanceID)
	if err != nil {
		return err
	}
	if err := engineRepo.DeleteTimersByProcessInstanceTx(tx, instanceID); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	LogAuditErr("process terminated", auditService.LogProcessTerminated(instanceID, reason))
	for _, taskID := range cancelledTaskIDs {
		LogAuditErr("task cancelled", auditService.LogTaskCancelled(taskID, instanceID, "process terminated"))
	}
	for _, jobID := range cancelledJobIDs {
		LogAuditErr("job cancelled", auditService.LogJobFailed(jobID, instanceID, "cancelled: process terminated", 0))
	}
	LogAuditErr("timers cancelled", auditService.LogTimerCancelled(instanceID, nil))

	if err := ArchiveAndDeleteProcessInstance(db, instanceID, &reason); err != nil {
		fmt.Printf("Warning: failed to archive terminated process instance %s: %v\n", instanceID, err)
	}

	return nil
}
