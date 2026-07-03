// service/archive_service.go
package service

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// ArchiveAndDeleteProcessInstance copies a finished (completed/terminated)
// process instance's data into the historic_* tables, then deletes its live
// rows (process_instances, cascading to executions/tasks/variables/jobs/
// timer_jobs/audit_logs). Runs in its own transaction, separate from the
// transaction that marked the instance finished — if any step here fails,
// the transaction rolls back and the live row is left exactly as-is
// (already safely completed/terminated), so a failed archive never risks
// losing data. Callers should log a failure but not treat it as fatal.
func ArchiveAndDeleteProcessInstance(db *sqlx.DB, instanceID uuid.UUID, deleteReason *string) error {
	instanceRepo := repository.NewProcessInstanceRepository(db)
	taskRepo := repository.NewTaskRepository(db, nil)
	auditRepo := repository.NewAuditRepository(db)
	archiveRepo := repository.NewHistoricArchiveRepository(db)

	instance, err := instanceRepo.FindByID(instanceID)
	if err != nil {
		return fmt.Errorf("failed to load process instance: %w", err)
	}
	if instance == nil {
		return fmt.Errorf("process instance not found")
	}
	if instance.Status != "completed" && instance.Status != "terminated" {
		return fmt.Errorf("refusing to archive a process instance with status %q (must be completed or terminated)", instance.Status)
	}
	if instance.EndedAt == nil {
		return fmt.Errorf("process instance has no ended_at, cannot compute archive duration")
	}

	tasks, err := taskRepo.FindTasksByProcessInstance(instanceID)
	if err != nil {
		return fmt.Errorf("failed to load tasks: %w", err)
	}
	variables, err := instanceRepo.GetVariables(instanceID)
	if err != nil {
		return fmt.Errorf("failed to load variables: %w", err)
	}
	logs, err := auditRepo.GetProcessAuditLogs(instanceID)
	if err != nil {
		return fmt.Errorf("failed to load audit logs: %w", err)
	}

	steps := BuildTokenHistory(logs, tasks)
	activityRows := make([]repository.HistoricActivityInstanceRow, len(steps))
	for i, step := range steps {
		activityRows[i] = repository.HistoricActivityInstanceRow{
			ProcessInstanceID: instanceID,
			ExecutionID:       step.ExecutionId,
			Action:            step.Action,
			ElementID:         nilIfEmpty(step.ElementId),
			ElementName:       nilIfEmpty(step.ElementName),
			TaskID:            step.TaskId,
			Detail:            nilIfEmpty(step.Detail),
			OccurredAt:        step.Timestamp,
		}
	}

	taskRows := make([]repository.HistoricTaskRow, len(tasks))
	for i, t := range tasks {
		taskRows[i] = repository.HistoricTaskRow{
			ID:                t.ID,
			ProcessInstanceID: instanceID,
			TaskDefinitionKey: t.TaskDefinitionKey,
			TaskName:          nilIfEmpty(t.TaskName),
			Assignee:          t.Assignee,
			Status:            t.Status,
			Priority:          &t.Priority,
			CreatedAt:         t.CreatedAt,
			ClaimedAt:         t.ClaimedAt,
			CompletedAt:       t.CompletedAt,
		}
	}

	durationMillis := instance.EndedAt.Sub(instance.StartedAt).Milliseconds()
	var version *int
	if instance.Version > 0 {
		v := instance.Version
		version = &v
	}

	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	err = archiveRepo.InsertProcessInstanceTx(tx, repository.HistoricProcessInstance{
		ID:                       instance.ID,
		ProcessDefinitionID:      instance.ProcessDefinitionID,
		ProcessDefinitionKey:     instance.ProcessKey,
		ProcessDefinitionVersion: version,
		TenantID:                 instance.TenantID,
		StartedBy:                instance.StartedBy,
		StartedAt:                instance.StartedAt,
		EndedAt:                  *instance.EndedAt,
		DurationMillis:           &durationMillis,
		State:                    instance.Status,
		DeleteReason:             deleteReason,
	})
	if err != nil {
		return fmt.Errorf("failed to insert historic process instance: %w", err)
	}
	if err = archiveRepo.InsertActivityInstancesTx(tx, activityRows); err != nil {
		return fmt.Errorf("failed to insert historic activity instances: %w", err)
	}
	if err = archiveRepo.InsertTasksTx(tx, instanceID, taskRows); err != nil {
		return fmt.Errorf("failed to insert historic tasks: %w", err)
	}
	if err = archiveRepo.InsertVariablesTx(tx, instanceID, variables, *instance.EndedAt); err != nil {
		return fmt.Errorf("failed to insert historic variables: %w", err)
	}
	if err = archiveRepo.CleanupOrphanExecutionStateTx(tx, instanceID); err != nil {
		return fmt.Errorf("failed to clean up orphan execution state: %w", err)
	}
	if err = archiveRepo.DeleteLiveInstanceTx(tx, instanceID); err != nil {
		return fmt.Errorf("failed to delete live process instance: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit archive transaction: %w", err)
	}
	return nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
