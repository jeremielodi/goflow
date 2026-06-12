package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Task struct {
	ID                uuid.UUID        `db:"id" json:"id"`
	ProcessInstanceID uuid.UUID        `db:"process_instance_id" json:"processInstanceId"`
	ExecutionID       *uuid.UUID       `db:"execution_id" json:"executionId,omitempty"`
	TaskDefinitionKey string           `db:"task_definition_key" json:"taskDefinitionKey"`
	TaskName          string           `db:"task_name" json:"taskName,omitempty"`
	Assignee          *string          `db:"assignee" json:"assignee,omitempty"`
	CandidateGroup    *string          `db:"candidate_group" json:"candidateGroup,omitempty"`
	Status            string           `db:"status" json:"status"` // created, claimed, completed
	FormData          *json.RawMessage `db:"form_data" json:"formData,omitempty"`
	CreatedAt         time.Time        `db:"created_at" json:"createdAt"`
	ClaimedAt         *time.Time       `db:"claimed_at" json:"claimedAt,omitempty"`
	CompletedAt       *time.Time       `db:"completed_at" json:"completedAt,omitempty"`
}
