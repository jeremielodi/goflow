// service/audit_service.go
package service

import (
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type AuditService struct {
	auditRepo *repository.AuditRepository
}

func NewAuditService(db *sqlx.DB) *AuditService {
	return &AuditService{
		auditRepo: repository.NewAuditRepository(db),
	}
}

// LogProcessStarted logs when a process instance starts
func (s *AuditService) LogProcessStarted(processInstanceID uuid.UUID, variables map[string]interface{}, userID *uuid.UUID) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"variables": variables,
		"startedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		UserID:            userID,
		Action:            models.ActionProcessStarted,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogProcessCompleted logs when a process instance completes
func (s *AuditService) LogProcessCompleted(processInstanceID uuid.UUID, finalVariables map[string]interface{}) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"finalVariables": finalVariables,
		"completedAt":    time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionProcessCompleted,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogExecutionCreated logs when a new execution (token) is created
func (s *AuditService) LogExecutionCreated(executionID, processInstanceID uuid.UUID, parentExecutionID *uuid.UUID, nodeID string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"executionId":       executionID,
		"currentElementId":  nodeID,
		"parentExecutionId": parentExecutionID,
		"createdAt":         time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionExecutionCreated,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogExecutionMoved logs when execution moves to a new node
func (s *AuditService) LogExecutionMoved(executionID, processInstanceID uuid.UUID, fromNode, toNode string, variables map[string]interface{}) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"executionId": executionID,
		"fromNode":    fromNode,
		"toNode":      toNode,
		"variables":   variables,
		"timestamp":   time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionExecutionMoved,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogTaskCreated logs when a user task is created
func (s *AuditService) LogTaskCreated(taskID, processInstanceID uuid.UUID, taskName string, assignee *string, candidateGroup *string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"taskId":         taskID,
		"taskName":       taskName,
		"assignee":       assignee,
		"candidateGroup": candidateGroup,
		"createdAt":      time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		TaskID:            &taskID,
		Action:            models.ActionTaskCreated,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogTaskClaimed logs when a task is claimed. The claimant is recorded as a
// free-text assignee (not a users.id FK): task assignees in this engine are
// often BPMN-expression-derived strings ("${customer}") rather than
// registered platform users, so this can't require a uuid.UUID like most
// other audit methods do.
func (s *AuditService) LogTaskClaimed(taskID, processInstanceID uuid.UUID, assignee string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"taskId":    taskID,
		"claimedBy": assignee,
		"claimedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		TaskID:            &taskID,
		Action:            models.ActionTaskClaimed,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogTaskCompleted logs when a task is completed
func (s *AuditService) LogTaskCompleted(taskID, processInstanceID uuid.UUID, userID *uuid.UUID, formData map[string]interface{}) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"taskId":      taskID,
		"completedBy": userID,
		"formData":    formData,
		"completedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		TaskID:            &taskID,
		UserID:            userID,
		Action:            models.ActionTaskCompleted,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogVariablesUpdated logs when process variables are updated
func (s *AuditService) LogVariablesUpdated(processInstanceID uuid.UUID, oldVars, newVars map[string]interface{}) error {
	oldData, _ := json.Marshal(oldVars)
	newData, _ := json.Marshal(newVars)

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionVariablesUpdated,
		OldData:           oldData,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogGatewayForked logs when a parallel gateway forks
func (s *AuditService) LogGatewayForked(processInstanceID uuid.UUID, gatewayID string, childExecutions int) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"gatewayId":       gatewayID,
		"childExecutions": childExecutions,
		"forkedAt":        time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionGatewayForked,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogGatewayJoined logs when a parallel gateway joins
func (s *AuditService) LogGatewayJoined(processInstanceID uuid.UUID, gatewayID string, expectedCount, receivedCount int) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"gatewayId":     gatewayID,
		"expectedCount": expectedCount,
		"receivedCount": receivedCount,
		"joinedAt":      time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionGatewayJoined,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogTimerCreated logs when a timer is scheduled
func (s *AuditService) LogTimerCreated(processInstanceID uuid.UUID, executionID *uuid.UUID, dueAt time.Time, timerType string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"executionId": executionID,
		"dueAt":       dueAt,
		"timerType":   timerType,
		"createdAt":   time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionTimerCreated,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogTimerTriggered logs when a timer triggers
func (s *AuditService) LogTimerTriggered(processInstanceID uuid.UUID, timerID uuid.UUID, timerType string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"timerId":     timerID,
		"timerType":   timerType,
		"triggeredAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionTimerTriggered,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogJobCreated logs when a job is created
func (s *AuditService) LogJobCreated(processInstanceID uuid.UUID, jobID uuid.UUID, jobType string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"jobId":     jobID,
		"jobType":   jobType,
		"createdAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionJobCreated,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogExecutionCompleted logs when an execution (token) reaches an end event
func (s *AuditService) LogExecutionCompleted(executionID, processInstanceID uuid.UUID, nodeID string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"executionId": executionID,
		"nodeId":      nodeID,
		"completedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionExecutionCompleted,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogExecutionWaited logs when an execution parks at a catch event (timer/message/signal)
func (s *AuditService) LogExecutionWaited(executionID, processInstanceID uuid.UUID, nodeID, waitType string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"executionId": executionID,
		"nodeId":      nodeID,
		"waitType":    waitType,
		"waitingAt":   time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionExecutionWaited,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogTaskUnclaimed logs when a user unclaims a task
func (s *AuditService) LogTaskUnclaimed(taskID, processInstanceID uuid.UUID) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"taskId":      taskID,
		"unclaimedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		TaskID:            &taskID,
		Action:            models.ActionTaskUnclaimed,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogTaskCancelled logs when an open task is cancelled (e.g. a Move Token
// modification cancels the execution it was attached to)
func (s *AuditService) LogTaskCancelled(taskID, processInstanceID uuid.UUID, reason string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"taskId":      taskID,
		"reason":      reason,
		"cancelledAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		TaskID:            &taskID,
		Action:            models.ActionTaskCancelled,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogJobLocked logs when a worker locks a job via fetch-and-lock
func (s *AuditService) LogJobLocked(jobID, processInstanceID uuid.UUID, lockOwner string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"jobId":     jobID,
		"lockOwner": lockOwner,
		"lockedAt":  time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionJobLocked,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogJobCompleted logs when a worker completes a job
func (s *AuditService) LogJobCompleted(jobID, processInstanceID uuid.UUID, result map[string]interface{}) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"jobId":       jobID,
		"result":      result,
		"completedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionJobCompleted,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogJobFailed logs when a job fails, whether it will be retried or has
// permanently failed (retriesLeft distinguishes the two).
func (s *AuditService) LogJobFailed(jobID, processInstanceID uuid.UUID, errorMessage string, retriesLeft int) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"jobId":        jobID,
		"errorMessage": errorMessage,
		"retriesLeft":  retriesLeft,
		"failedAt":     time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionJobFailed,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogVariablesMerged logs when variables from a task/job/gateway/message/
// signal are merged into the process instance's variable set.
func (s *AuditService) LogVariablesMerged(processInstanceID uuid.UUID, merged map[string]interface{}, source string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"merged":    merged,
		"source":    source,
		"mergedAt":  time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionVariablesMerged,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogTimerCancelled logs when a scheduled timer is cancelled (e.g. a boundary
// timer's main task completes first, so the race is cancelled)
func (s *AuditService) LogTimerCancelled(processInstanceID uuid.UUID, executionID *uuid.UUID) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"executionId": executionID,
		"cancelledAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionTimerCancelled,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogMessageCorrelated logs when a published message correlates to a waiting execution
func (s *AuditService) LogMessageCorrelated(processInstanceID uuid.UUID, messageName string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"messageName":   messageName,
		"correlatedAt":  time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionMessageCorrelated,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogSignalBroadcast logs when a signal is broadcast, resuming zero or more
// waiting executions across possibly multiple process instances. Not tied to
// a single process instance, so ProcessInstanceID is left nil.
func (s *AuditService) LogSignalBroadcast(signalName string, resumedCount int) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"signalName":   signalName,
		"resumedCount": resumedCount,
		"broadcastAt":  time.Now(),
	})

	log := &models.AuditLog{
		ID:        uuid.New(),
		Action:    models.ActionSignalBroadcast,
		NewData:   newData,
		CreatedAt: time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogProcessTerminated logs when a process instance is terminated (e.g. by an operator)
func (s *AuditService) LogProcessTerminated(processInstanceID uuid.UUID, reason string) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"reason":       reason,
		"terminatedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionProcessTerminated,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogProcessSuspended logs when a process instance is suspended
func (s *AuditService) LogProcessSuspended(processInstanceID uuid.UUID) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"suspendedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionProcessSuspended,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogProcessResumed logs when a suspended process instance is resumed
func (s *AuditService) LogProcessResumed(processInstanceID uuid.UUID) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"resumedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		Action:            models.ActionProcessResumed,
		NewData:           newData,
		CreatedAt:         time.Now(),
	}

	return s.auditRepo.CreateAuditLog(log)
}

// LogAuditErr is a small helper for the "best-effort, non-blocking" audit
// convention used throughout the engine: a failed audit write must never
// fail the business operation it describes, but it also shouldn't disappear
// silently — log it so a broken audit trail is at least visible in server
// logs. Callers use it as: LogAuditErr("execution moved", s.LogExecutionMoved(...)).
func LogAuditErr(action string, err error) {
	if err != nil {
		log.Printf("audit: failed to log %s: %v", action, err)
	}
}
