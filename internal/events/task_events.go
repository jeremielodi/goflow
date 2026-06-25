// internal/events/task_events.go
package events

import (
	"time"
)

type TaskEventType string

const (
	TaskCreated   TaskEventType = "TASK_CREATED"
	TaskClaimed   TaskEventType = "TASK_CLAIMED"
	TaskUnclaimed TaskEventType = "TASK_UNCLAIMED"
	TaskCompleted TaskEventType = "TASK_COMPLETED"
	TaskFailed    TaskEventType = "TASK_FAILED"
	TaskCancelled TaskEventType = "TASK_CANCELLED"
)

type TaskEvent struct {
	ID                string                 `json:"id"`
	EventType         TaskEventType          `json:"eventType"`
	TaskID            string                 `json:"taskId"`
	ProcessInstanceID string                 `json:"processInstanceId"`
	ProcessKey        string                 `json:"processKey"` // Add this field
	ExecutionID       string                 `json:"executionId"`
	TaskName          string                 `json:"taskName"`
	Assignee          *string                `json:"assignee,omitempty"`
	CandidateGroup    *string                `json:"candidateGroup,omitempty"`
	OldStatus         *string                `json:"oldStatus,omitempty"`
	NewStatus         string                 `json:"newStatus"`
	Timestamp         time.Time              `json:"timestamp"`
	Variables         map[string]interface{} `json:"variables,omitempty"`
	UserID            string                 `json:"userId,omitempty"`
	Comment           string                 `json:"comment,omitempty"`
}
