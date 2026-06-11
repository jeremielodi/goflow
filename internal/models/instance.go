package models

import (
	"time"

	"github.com/google/uuid"
)

type ProcessInstance struct {
	ID                  uuid.UUID  `db:"id" json:"id"`
	ProcessDefinitionID uuid.UUID  `db:"process_definition_id" json:"processDefinitionId"`
	Status              string     `db:"status" json:"status"`
	StartedBy           *string    `db:"started_by" json:"startedBy,omitempty"`
	TransactionScopeID  *uuid.UUID `db:"transaction_scope_id" json:"transactionScopeId,omitempty"`
	StartedAt           time.Time  `db:"started_at" json:"startedAt"`
	EndedAt             *time.Time `db:"ended_at" json:"endedAt,omitempty"`
	ProcessKey          string     `db:"process_key" json:"processKey,omitempty"`
	ProcessName         string     `db:"process_name" json:"processName,omitempty"`
	Version             int        `db:"version" json:"version,omitempty"`
	UpdatedAt           *time.Time `db:"updated_at" json:"updatedAt,omitempty"`
}
