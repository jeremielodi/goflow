package parser

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Process struct {
	ID           string `xml:"id,attr"`
	IsExecutable bool   `xml:"isExecutable,attr"`

	StartEvents             []StartEvent             `xml:"startEvent"`
	EndEvents               []EndEvent               `xml:"endEvent"`
	Tasks                   []Task                   `xml:"task"`
	UserTasks               []UserTask               `xml:"userTask"`
	ServiceTasks            []ServiceTask            `xml:"serviceTask"`
	ExclusiveGateways       []ExclusiveGateway       `xml:"exclusiveGateway"`
	ParallelGateways        []ParallelGateway        `xml:"parallelGateway"`
	IntermediateCatchEvents []IntermediateCatchEvent `xml:"intermediateCatchEvent"`

	SequenceFlows []SequenceFlow `xml:"sequenceFlow"`
}

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
	ID           uuid.UUID       `db:"id"`
	DeploymentID uuid.UUID       `db:"deployment_id"`
	ProcessKey   string          `db:"process_key"`
	ProcessName  *string         `db:"process_name"`
	Version      int             `db:"version"`
	IsActive     bool            `db:"is_active"`
	BpmnXML      string          `db:"bpmn_xml"`
	ParsedGraph  json.RawMessage `db:"parsed_graph"` // JSONB
	CreatedAt    time.Time       `db:"created_at"`
}

// ProcessDefinitionCreateModel for INSERT
type ProcessDefinitionCreateModel struct {
	DeploymentID uuid.UUID
	ProcessKey   string
	ProcessName  *string
	Version      int
	IsActive     bool
	BpmnXML      string
	ParsedGraph  json.RawMessage
}
