package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Deployment model (matches table columns)
type Deployment struct {
	ID         uuid.UUID  `db:"id"`
	Name       string     `db:"name"`
	DeployedBy *uuid.UUID `db:"deployed_by"`
	Status     string     `db:"status"` // "active" | "inactive"
	CreatedAt  time.Time  `db:"created_at"`
}

// DeploymentCreateModel for INSERT
type DeploymentCreateModel struct {
	Name       string
	DeployedBy *uuid.UUID
	Status     string
}

// ProcessDefinition model
type ProcessDefinition struct {
	ID           uuid.UUID       `db:"id"            json:"id"`
	DeploymentID uuid.UUID       `db:"deployment_id" json:"deploymentId"`
	ProcessKey   string          `db:"process_key"   json:"key"`
	ProcessName  *string         `db:"process_name"  json:"name,omitempty"`
	Version      int             `db:"version"       json:"version"`
	IsActive     bool            `db:"is_active"     json:"isActive"`
	BpmnXML      string          `db:"bpmn_xml"      json:"bpmnXml,omitempty"`
	ParsedGraph  json.RawMessage `db:"parsed_graph"  json:"-"`
	CreatedAt    time.Time       `db:"created_at"    json:"createdAt"`
}

// ProcessDefinitionCreateModel for INSERT
type ProcessDefinitionCreateModel struct {
	DeploymentID uuid.UUID
	ProcessKey   string
	ProcessName  *string
	TenantID     *string
	Version      int
	IsActive     bool
	BpmnXML      string
	ParsedGraph  json.RawMessage
	EngineType   string
}

// DMNDecision model
type DMNDecision struct {
	ID           uuid.UUID       `db:"id"            json:"id"`
	DeploymentID uuid.UUID       `db:"deployment_id" json:"deploymentId"`
	DecisionKey  string          `db:"decision_key"  json:"key"`
	DecisionName *string         `db:"decision_name" json:"name,omitempty"`
	Version      int             `db:"version"       json:"version"`
	IsActive     bool            `db:"is_active"      json:"isActive"`
	DmnXML       string          `db:"dmn_xml"        json:"dmnXml,omitempty"`
	ParsedTable  json.RawMessage `db:"parsed_table"   json:"-"`
	CreatedAt    time.Time       `db:"created_at"     json:"createdAt"`
}

// DMNDecisionCreateModel for INSERT
type DMNDecisionCreateModel struct {
	DeploymentID uuid.UUID
	DecisionKey  string
	DecisionName *string
	DmnXML       string
	ParsedTable  json.RawMessage
}
