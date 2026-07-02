// internal/models/audit.go
package models

import (
	"time"

	"github.com/google/uuid"
)

type AuditAction string

const (
	// Process Instance Actions
	ActionProcessStarted    AuditAction = "PROCESS_STARTED"
	ActionProcessCompleted  AuditAction = "PROCESS_COMPLETED"
	ActionProcessTerminated AuditAction = "PROCESS_TERMINATED"
	ActionProcessSuspended  AuditAction = "PROCESS_SUSPENDED"
	ActionProcessResumed    AuditAction = "PROCESS_RESUMED"

	// Execution Actions
	ActionExecutionCreated   AuditAction = "EXECUTION_CREATED"
	ActionExecutionMoved     AuditAction = "EXECUTION_MOVED"
	ActionExecutionCompleted AuditAction = "EXECUTION_COMPLETED"
	ActionExecutionWaited    AuditAction = "EXECUTION_WAITED"

	// Task Actions
	ActionTaskCreated   AuditAction = "TASK_CREATED"
	ActionTaskClaimed   AuditAction = "TASK_CLAIMED"
	ActionTaskUnclaimed AuditAction = "TASK_UNCLAIMED"
	ActionTaskCompleted AuditAction = "TASK_COMPLETED"
	ActionTaskCancelled AuditAction = "TASK_CANCELLED"

	// Job Actions
	ActionJobCreated   AuditAction = "JOB_CREATED"
	ActionJobLocked    AuditAction = "JOB_LOCKED"
	ActionJobCompleted AuditAction = "JOB_COMPLETED"
	ActionJobFailed    AuditAction = "JOB_FAILED"

	// Variable Actions
	ActionVariablesUpdated AuditAction = "VARIABLES_UPDATED"
	ActionVariablesMerged  AuditAction = "VARIABLES_MERGED"

	// Gateway Actions
	ActionGatewayForked AuditAction = "GATEWAY_FORKED"
	ActionGatewayJoined AuditAction = "GATEWAY_JOINED"

	// Timer Actions
	ActionTimerCreated   AuditAction = "TIMER_CREATED"
	ActionTimerTriggered AuditAction = "TIMER_TRIGGERED"
	ActionTimerCancelled AuditAction = "TIMER_CANCELLED"

	// Message Actions
	ActionMessageSent       AuditAction = "MESSAGE_SENT"
	ActionMessageReceived   AuditAction = "MESSAGE_RECEIVED"
	ActionMessageCorrelated AuditAction = "MESSAGE_CORRELATED"

	// Signal Actions
	ActionSignalBroadcast AuditAction = "SIGNAL_BROADCAST"
)

type AuditLog struct {
	ID                uuid.UUID   `db:"id" json:"id"`
	ProcessInstanceID *uuid.UUID  `db:"process_instance_id" json:"processInstanceId,omitempty"`
	TaskID            *uuid.UUID  `db:"task_id" json:"taskId,omitempty"`
	UserID            *uuid.UUID  `db:"user_id" json:"userId,omitempty"`
	Action            AuditAction `db:"action" json:"action"`
	OldData           []byte      `db:"old_data" json:"oldData,omitempty"`
	NewData           []byte      `db:"new_data" json:"newData,omitempty"`
	CreatedAt         time.Time   `db:"created_at" json:"createdAt"`
}

type AuditLogEntry struct {
	Action      AuditAction            `json:"action"`
	Description string                 `json:"description"`
	OldState    map[string]interface{} `json:"oldState,omitempty"`
	NewState    map[string]interface{} `json:"newState,omitempty"`
	UserID      *uuid.UUID             `json:"userId,omitempty"`
	Timestamp   time.Time              `json:"timestamp"`
}
