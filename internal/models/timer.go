// internal/models/timer.go
package models

import (
	"time"

	"github.com/google/uuid"
)

type TimerJob struct {
	ID                uuid.UUID  `db:"id" json:"id"`
	ProcessInstanceID uuid.UUID  `db:"process_instance_id" json:"processInstanceId"`
	ExecutionID       *uuid.UUID `db:"execution_id" json:"executionId,omitempty"`
	EventType         string     `db:"event_type" json:"eventType"` // intermediate, boundary, start
	DueAt             time.Time  `db:"due_at" json:"dueAt"`
	Payload           []byte     `db:"payload" json:"payload"`
	IsTriggered       bool       `db:"is_triggered" json:"isTriggered"`
	CreatedAt         time.Time  `db:"created_at" json:"createdAt"`
	TriggeredAt       *time.Time `db:"triggered_at" json:"triggeredAt,omitempty"`
}

type TimerDefinition struct {
	Duration     string `json:"duration,omitempty"`     // ISO 8601 duration: PT1M, PT1H, PT1D
	Cycle        string `json:"cycle,omitempty"`        // For repeating timers: R/PT1M
	TimeDate     string `json:"timeDate,omitempty"`     // Fixed date/time: 2024-12-31T23:59:59
	TimeCycle    string `json:"timeCycle,omitempty"`    // Alternative cycle format
	TimeDuration string `json:"timeDuration,omitempty"` // Alternative duration format
}

type TimerEventType string

const (
	TimerIntermediate TimerEventType = "intermediate"
	TimerBoundary     TimerEventType = "boundary"
	TimerStart        TimerEventType = "start"
)
