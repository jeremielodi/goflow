package parser

import (
	"encoding/json"
	"encoding/xml"
	"time"

	"github.com/google/uuid"
)

type Process struct {
	ID                      string                   `xml:"id,attr"`
	IsExecutable            bool                     `xml:"isExecutable,attr"`
	Name                    string                   `xml:"name,attr"`
	StartEvents             []StartEvent             `xml:"startEvent"`
	UserTasks               []UserTask               `xml:"userTask"`
	ServiceTasks            []ServiceTask            `xml:"serviceTask"`
	EndEvents               []EndEvent               `xml:"endEvent"`
	ExclusiveGateways       []ExclusiveGateway       `xml:"exclusiveGateway"`
	ParallelGateways        []ParallelGateway        `xml:"parallelGateway"`
	IntermediateCatchEvents []IntermediateCatchEvent `xml:"intermediateCatchEvent"`
	SequenceFlows           []SequenceFlow           `xml:"sequenceFlow"`
	IntermediateTimerEvents []IntermediateTimerEvent
	BoundaryTimerEvents     []BoundaryTimerEvent
	BoundaryEvents          []BoundaryEvent `xml:"boundaryEvent"`
}

type IntermediateTimerEvent struct {
	ID              string
	Name            string
	TimerDefinition *TimerDefinition
}

type BoundaryTimerEvent struct {
	ID              string
	AttachedToRef   string
	CancelActivity  bool
	TimerDefinition *TimerDefinition
}

// TimerDefinition represents BPMN timer event definition
type TimerDefinition struct {
	RawXML       string
	TimeDuration string // PT1M, PT1H, PT1D, etc.
	TimeDate     string // 2024-12-31T23:59:59
	TimeCycle    string // R/PT1M, R/PT24H, etc.
}

type ProcessDefinitionInfo struct {
	ID           string `json:"id"`
	Key          string `json:"key"`
	Version      int    `json:"version"`
	ResourceName string `json:"resourceName"`
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
	EngineType   string          `db:"engine_type"`
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

type Definitions struct {
	XMLName   xml.Name  `xml:"definitions"`
	Processes []Process `xml:"process"`
}

type Task struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
}

type StartEvent struct {
	ID                   string                `xml:"id,attr"`
	Outgoing             []string              `xml:"outgoing"`
	TimerEventDefinition *TimerEventDefinition `xml:"timerEventDefinition"`
	// Add a helper field for processed timer definition
	TimerDefinition *TimerDefinition `xml:"-"`
}

type EndEvent struct {
	ID       string   `xml:"id,attr"`
	Incoming []string `xml:"incoming"`
}

type CamundaExtensions struct {
	Assignee        string `xml:"assignee,attr"`
	CandidateGroups string `xml:"candidateGroups,attr"`
	TaskDefinition  *struct {
		Topic string `xml:"topic,attr"`
	} `xml:"taskDefinition"`
}

type CamundaTaskDefinition struct {
	Topic string `xml:"topic,attr"`
}

// ZeebeExtensions (for Camunda 8)
type ZeebeExtensions struct {
	TaskDefinition *ZeebeTaskDefinition `xml:"taskDefinition"`
	Assignment     *ZeebeAssignment     `xml:"assignment"`
}

type ZeebeTaskDefinition struct {
	Type string `xml:"type,attr"`
}

type ZeebeAssignment struct {
	Assignee string `xml:"assignee,attr"`
}

type UserTask struct {
	ID                     string             `xml:"id,attr"`
	Name                   string             `xml:"name,attr"`
	Incoming               []string           `xml:"incoming"`
	Outgoing               []string           `xml:"outgoing"`
	CamundaExt             *CamundaExtensions `xml:"extensionElements>camunda:taskListener?omitempty"`
	CamundaAssignee        string             `xml:"http://camunda.org/schema/1.0/bpmn assignee,attr"`
	CamundaCandidateGroups string             `xml:"http://camunda.org/schema/1.0/bpmn candidateGroups,attr"`
	CamundaFormKey         string             `xml:"http://camunda.org/schema/1.0/bpmn formKey,attr"`
	ZeebeExt               *ZeebeExtensions   `xml:"extensionElements>zeebe:assignment"?`
}

type ServiceTask struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`

	CamundaExt *CamundaExtensions `xml:"extensionElements>camunda:taskListener"?`
	ZeebeExt   *ZeebeExtensions   `xml:"extensionElements>zeebe:assignment"?`

	CamundaType  string `xml:"http://camunda.org/schema/1.0/bpmn type,attr"`
	CamundaTopic string `xml:"http://camunda.org/schema/1.0/bpmn topic,attr"`
}

type ExclusiveGateway struct {
	ID       string   `xml:"id,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
}

type ParallelGateway struct {
	ID       string   `xml:"id,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
}

type IntermediateCatchEvent struct {
	ID       string   `xml:"id,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`

	MessageEventDefinition *MessageEventDefinition `xml:"messageEventDefinition"`
	TimerEventDefinition   *TimerEventDefinition   `xml:"timerEventDefinition"`
	TimerDefinition        *TimerDefinition        `xml:"-"`
}

type BoundaryEvent struct {
	ID                     string                  `xml:"id,attr"`
	AttachedToRef          string                  `xml:"attachedToRef,attr"`
	CancelActivity         bool                    `xml:"cancelActivity,attr"`
	Name                   string                  `xml:"name,attr"`
	Outgoing               []string                `xml:"outgoing"`
	TimerEventDefinition   *TimerEventDefinition   `xml:"timerEventDefinition"`
	MessageEventDefinition *MessageEventDefinition `xml:"messageEventDefinition"`
	TimerDefinition        *TimerDefinition        `xml:"-"`
}

type MessageEventDefinition struct {
	ID string `xml:"id,attr"`
}

type TimerEventDefinition struct {
	ID           string  `xml:"id,attr"`
	TimeDuration *string `xml:"timeDuration"`
	TimeDate     *string `xml:"timeDate"`
	TimeCycle    *string `xml:"timeCycle"`
}

// GetRawXML returns the raw XML representation of the timer definition
func (t *TimerEventDefinition) GetRawXML() string {
	if t == nil {
		return ""
	}

	if t.TimeDuration != nil {
		return "<timeDuration>" + *t.TimeDuration + "</timeDuration>"
	}
	if t.TimeDate != nil {
		return "<timeDate>" + *t.TimeDate + "</timeDate>"
	}
	if t.TimeCycle != nil {
		return "<timeCycle>" + *t.TimeCycle + "</timeCycle>"
	}
	return ""
}

// GetTimerDefinition returns a parsed timer definition
func (t *TimerEventDefinition) GetTimerDefinition() *TimerDefinition {
	if t == nil {
		return nil
	}

	def := &TimerDefinition{
		RawXML: t.GetRawXML(),
	}

	if t.TimeDuration != nil {
		def.TimeDuration = *t.TimeDuration
	}
	if t.TimeDate != nil {
		def.TimeDate = *t.TimeDate
	}
	if t.TimeCycle != nil {
		def.TimeCycle = *t.TimeCycle
	}

	return def
}

type ConditionExpression struct {
	Text string `xml:",chardata"`
}

type SequenceFlow struct {
	ID        string `xml:"id,attr"`
	SourceRef string `xml:"sourceRef,attr"`
	TargetRef string `xml:"targetRef,attr"`
	Condition string `xml:"conditionExpression"`
	Name      string `xml:"name,attr"`
}

// Update the ParseBPMN function to populate TimerDefinition
func ParseBPMN(data []byte) (*Definitions, error) {
	var defs Definitions

	err := xml.Unmarshal(data, &defs)
	if err != nil {
		return nil, err
	}

	// Post-process to populate TimerDefinition for start events
	for i := range defs.Processes {
		process := &defs.Processes[i]

		// Process start events
		for j := range process.StartEvents {
			startEvent := &process.StartEvents[j]
			if startEvent.TimerEventDefinition != nil {
				startEvent.TimerDefinition = &TimerDefinition{
					RawXML:       getTimerRawXML(startEvent.TimerEventDefinition),
					TimeDuration: getStringValue(startEvent.TimerEventDefinition.TimeDuration),
					TimeDate:     getStringValue(startEvent.TimerEventDefinition.TimeDate),
					TimeCycle:    getStringValue(startEvent.TimerEventDefinition.TimeCycle),
				}
			}
		}

		// Process intermediate catch events (timers)
		for j := range process.IntermediateCatchEvents {
			event := &process.IntermediateCatchEvents[j]
			if event.TimerEventDefinition != nil {
				event.TimerDefinition = &TimerDefinition{
					RawXML:       getTimerRawXML(event.TimerEventDefinition),
					TimeDuration: getStringValue(event.TimerEventDefinition.TimeDuration),
					TimeDate:     getStringValue(event.TimerEventDefinition.TimeDate),
					TimeCycle:    getStringValue(event.TimerEventDefinition.TimeCycle),
				}
			}
		}

		// Process boundary events
		for j := range process.BoundaryEvents {
			event := &process.BoundaryEvents[j]
			if event.TimerEventDefinition != nil {
				event.TimerDefinition = &TimerDefinition{
					RawXML:       getTimerRawXML(event.TimerEventDefinition),
					TimeDuration: getStringValue(event.TimerEventDefinition.TimeDuration),
					TimeDate:     getStringValue(event.TimerEventDefinition.TimeDate),
					TimeCycle:    getStringValue(event.TimerEventDefinition.TimeCycle),
				}
			}
		}
	}
	PopulateTimerEvents(&defs)

	return &defs, nil
}

// Helper functions
func getTimerRawXML(timerDef *TimerEventDefinition) string {
	if timerDef == nil {
		return ""
	}

	if timerDef.TimeDuration != nil {
		return "<timeDuration>" + *timerDef.TimeDuration + "</timeDuration>"
	}
	if timerDef.TimeDate != nil {
		return "<timeDate>" + *timerDef.TimeDate + "</timeDate>"
	}
	if timerDef.TimeCycle != nil {
		return "<timeCycle>" + *timerDef.TimeCycle + "</timeCycle>"
	}
	return ""
}

func getStringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func PopulateTimerEvents(defs *Definitions) {
	for i := range defs.Processes {
		process := &defs.Processes[i]

		// Clear existing slices
		process.IntermediateTimerEvents = []IntermediateTimerEvent{}
		process.BoundaryTimerEvents = []BoundaryTimerEvent{}

		// Process Intermediate Catch Events (standalone timer events)
		for _, event := range process.IntermediateCatchEvents {
			if event.TimerDefinition != nil && event.TimerDefinition.RawXML != "" {
				process.IntermediateTimerEvents = append(process.IntermediateTimerEvents, IntermediateTimerEvent{
					ID:              event.ID,
					Name:            event.ID,
					TimerDefinition: event.TimerDefinition,
				})
			}
		}

		// Process Boundary Events (timer attached to task)
		for _, event := range process.BoundaryEvents {
			if event.TimerDefinition != nil && event.TimerDefinition.RawXML != "" {
				process.BoundaryTimerEvents = append(process.BoundaryTimerEvents, BoundaryTimerEvent{
					ID:              event.ID,
					AttachedToRef:   event.AttachedToRef,
					CancelActivity:  event.CancelActivity,
					TimerDefinition: event.TimerDefinition,
				})
			}
		}
	}
}
