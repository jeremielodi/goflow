// service/audit_service.go
package service

import (
	"encoding/json"
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

// LogTaskClaimed logs when a user claims a task
func (s *AuditService) LogTaskClaimed(taskID, processInstanceID uuid.UUID, userID uuid.UUID) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"taskId":    taskID,
		"claimedBy": userID,
		"claimedAt": time.Now(),
	})

	log := &models.AuditLog{
		ID:                uuid.New(),
		ProcessInstanceID: &processInstanceID,
		TaskID:            &taskID,
		UserID:            &userID,
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

// LogTimerTriggered logs when a timer fires
func (s *AuditService) LogTimerTriggered(processInstanceID uuid.UUID, timerID uuid.UUID) error {
	newData, _ := json.Marshal(map[string]interface{}{
		"timerId":     timerID,
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
