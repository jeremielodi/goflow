// internal/events/task_events.go
package events

import (
	"time"
)

// TaskEventType représente le type d'événement de tâche
type TaskEventType string

const (
	TaskCreated   TaskEventType = "TASK_CREATED"
	TaskClaimed   TaskEventType = "TASK_CLAIMED"
	TaskCompleted TaskEventType = "TASK_COMPLETED"
	TaskFailed    TaskEventType = "TASK_FAILED"
	TaskCancelled TaskEventType = "TASK_CANCELLED"
)

// TaskEvent représente un événement de tâche
type TaskEvent struct {
	ID                string                 `json:"id"`
	EventType         TaskEventType          `json:"eventType"`
	TaskID            string                 `json:"taskId"`
	ProcessInstanceID string                 `json:"processInstanceId"`
	ExecutionID       string                 `json:"executionId"`
	TaskName          string                 `json:"taskName"`
	Assignee          *string                `json:"assignee,omitempty"`
	CandidateGroup    *string                `json:"candidateGroup,omitempty"`
	OldStatus         string                 `json:"oldStatus,omitempty"`
	NewStatus         string                 `json:"newStatus"`
	Timestamp         time.Time              `json:"timestamp"`
	Variables         map[string]interface{} `json:"variables,omitempty"`
	UserID            string                 `json:"userId,omitempty"`
	Comment           string                 `json:"comment,omitempty"`
}
