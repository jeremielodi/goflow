package models

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Job represents an external task job
type Job struct {
	ID                uuid.UUID       `db:"id" json:"id"`
	JobType           string          `db:"job_type" json:"jobType"`
	Status            string          `db:"status" json:"status"`
	Payload           json.RawMessage `db:"payload" json:"payload"`
	Retries           int             `db:"retries" json:"retries"`
	LockedBy          sql.NullString  `db:"locked_by" json:"lockedBy,omitempty"`
	LockedUntil       sql.NullTime    `db:"locked_until" json:"lockedUntil,omitempty"`
	ErrorMessage      sql.NullString  `db:"error_message" json:"errorMessage,omitempty"`
	ExecutionID       uuid.UUID       `db:"execution_id" json:"executionId"`
	ProcessInstanceID uuid.UUID       `db:"process_instance_id" json:"processInstanceId"`
	CreatedAt         time.Time       `db:"created_at" json:"createdAt"`
	UpdatedAt         time.Time       `db:"updated_at" json:"updatedAt"`
	CompletedAt       sql.NullTime    `db:"completed_at" json:"completedAt,omitempty"`
}

// JobCreateModel represents the data needed to create a new job
type JobCreateModel struct {
	ID                uuid.UUID       `json:"id"`
	JobType           string          `json:"jobType"`
	Status            string          `json:"status"`
	Payload           json.RawMessage `json:"payload"`
	Retries           int             `json:"retries"`
	ExecutionID       uuid.UUID       `json:"executionId"`
	ProcessInstanceID uuid.UUID       `json:"processInstanceId"`
}

// JobUpdateModel represents the data needed to update a job
type JobUpdateModel struct {
	Status       *string          `json:"status,omitempty"`
	LockedBy     *string          `json:"lockedBy,omitempty"`
	LockedUntil  *time.Time       `json:"lockedUntil,omitempty"`
	ErrorMessage *string          `json:"errorMessage,omitempty"`
	Retries      *int             `json:"retries,omitempty"`
	CompletedAt  *time.Time       `json:"completedAt,omitempty"`
	Payload      *json.RawMessage `json:"payload,omitempty"`
}

// JobWithExecution represents a job joined with its execution context
type JobWithExecution struct {
	Job
	CurrentElementID string `db:"current_element_id" json:"currentElementId"`
}
