package models

import (
	"encoding/json"
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
	TenantID            *string    `db:"tenant_id" json:"tenantId,omitempty"`
	BusinessKey         *string    `db:"business_key" json:"businessKey"`
	UpdatedAt           *time.Time `db:"updated_at" json:"updatedAt,omitempty"`
}

// MarshalJSON adds Camunda 7's ProcessInstanceDto fields (definitionId,
// ended, suspended, caseInstanceId, links) to GoFlow's own, so a Camunda
// 7 client reads instances unchanged.
func (p ProcessInstance) MarshalJSON() ([]byte, error) {
	type plain ProcessInstance
	return json.Marshal(struct {
		plain
		DefinitionID   uuid.UUID     `json:"definitionId"`
		Ended          bool          `json:"ended"`
		Suspended      bool          `json:"suspended"`
		CaseInstanceID *string       `json:"caseInstanceId"`
		Links          []interface{} `json:"links"`
	}{
		plain:        plain(p),
		DefinitionID: p.ProcessDefinitionID,
		Ended:        p.Status == "completed" || p.Status == "terminated",
		Suspended:    p.Status == "suspended",
		Links:        []interface{}{},
	})
}
