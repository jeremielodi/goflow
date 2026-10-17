package parser

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Process struct {
	ID                      string                   `xml:"id,attr"`
	IsExecutable            bool                     `xml:"isExecutable,attr"`
	Name                    string                   `xml:"name,attr"`
	StartEvents             []StartEvent             `xml:"startEvent"`
	UserTasks               []UserTask               `xml:"userTask"`
	Tasks                   []Task                   `xml:"task"`
	ServiceTasks            []ServiceTask            `xml:"serviceTask"`
	EndEvents               []EndEvent               `xml:"endEvent"`
	ExclusiveGateways       []ExclusiveGateway       `xml:"exclusiveGateway"`
	ParallelGateways        []ParallelGateway        `xml:"parallelGateway"`
	InclusiveGateways       []InclusiveGateway       `xml:"inclusiveGateway"`
	IntermediateCatchEvents []IntermediateCatchEvent `xml:"intermediateCatchEvent"`
	SequenceFlows           []SequenceFlow           `xml:"sequenceFlow"`
	ScriptTasks             []ScriptTask             `xml:"scriptTask"`
	BusinessRuleTasks       []BusinessRuleTask       `xml:"businessRuleTask"`
	CallActivities          []CallActivity           `xml:"callActivity"`
	EventBasedGateways      []EventBasedGateway      `xml:"eventBasedGateway"`
	IntermediateTimerEvents  []IntermediateTimerEvent
	BoundaryTimerEvents     []BoundaryTimerEvent
	BoundaryErrorEvents     []BoundaryErrorEvent
	BoundaryMessageEvents   []BoundaryMessageEvent
	BoundarySignalEvents    []BoundarySignalEvent
	BoundaryEvents          []BoundaryEvent `xml:"boundaryEvent"`
	SubProcesses            []SubProcess    `xml:"subProcess"`
	ManualTasks             []Task          `xml:"manualTask"`
	SendTasks               []ServiceTask   `xml:"sendTask"`
	ReceiveTasks            []ReceiveTask   `xml:"receiveTask"`
	IntermediateThrowEvents []ThrowEvent    `xml:"intermediateThrowEvent"`
}

// ReceiveTask waits for a message, like an intermediate message catch event.
type ReceiveTask struct {
	ID          string `xml:"id,attr"`
	Name        string `xml:"name,attr"`
	MessageRef  string `xml:"messageRef,attr"`
	MessageName string `xml:"-"` // resolved
}

// ThrowEvent is an intermediate throw event: none (pass-through), signal,
// message or link.
type ThrowEvent struct {
	ID                     string                  `xml:"id,attr"`
	Name                   string                  `xml:"name,attr"`
	SignalEventDefinition  *SignalEventDefinition  `xml:"signalEventDefinition"`
	MessageEventDefinition *MessageEventDefinition `xml:"messageEventDefinition"`
	LinkEventDefinition    *LinkEventDefinition    `xml:"linkEventDefinition"`
	Ext                    *ExtensionElements      `xml:"extensionElements"`
	CamundaTopic           string                  `xml:"http://camunda.org/schema/1.0/bpmn topic,attr"`
	SignalName             string                  `xml:"-"` // resolved
}

// LinkEventDefinition pairs a link throw event with the link catch event
// of the same name in the same scope.
type LinkEventDefinition struct {
	Name string `xml:"name,attr"`
}

// TerminateEventDefinition marks a terminate end event.
type TerminateEventDefinition struct{}

// SubProcess is an embedded BPMN sub-process. Its body is the same set of
// elements a process holds (so sub-processes nest), parsed into the
// embedded Process; ID and Name come from the embedded Process's own
// id/name attribute tags.
type SubProcess struct {
	Process
	Incoming         []string `xml:"incoming"`
	Outgoing         []string `xml:"outgoing"`
	TriggeredByEvent bool     `xml:"triggeredByEvent,attr"`
}

// Scopes returns p followed by the body of every sub-process nested in it,
// depth first, so post-processing that walks a process's elements can
// reach elements inside sub-processes too.
func (p *Process) Scopes() []*Process {
	scopes := []*Process{p}
	for i := range p.SubProcesses {
		scopes = append(scopes, p.SubProcesses[i].Process.Scopes()...)
	}
	return scopes
}

type TimerCycle struct {
	Remaining int           // Number of remaining repetitions (-1 = infinite)
	Interval  time.Duration // Time between repetitions
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

type ErrorDefinition struct {
	ID        string `xml:"id,attr"`
	Name      string `xml:"name,attr"`
	ErrorCode string `xml:"errorCode,attr"`
}

type ErrorEventDefinition struct {
	ID       string `xml:"id,attr"`
	ErrorRef string `xml:"errorRef,attr"`
}

type BoundaryErrorEvent struct {
	ID             string
	AttachedToRef  string
	CancelActivity bool
	ErrorCode      string
}

type MessageDefinition struct {
	ID   string `xml:"id,attr"`
	Name string `xml:"name,attr"`
}

type SignalDefinition struct {
	ID   string `xml:"id,attr"`
	Name string `xml:"name,attr"`
}

type SignalEventDefinition struct {
	ID        string `xml:"id,attr"`
	SignalRef string `xml:"signalRef,attr"`
}

type Definitions struct {
	XMLName   xml.Name            `xml:"definitions"`
	Processes []Process           `xml:"process"`
	Errors    []ErrorDefinition   `xml:"error"`
	Messages  []MessageDefinition `xml:"message"`
	Signals   []SignalDefinition  `xml:"signal"`
}

type Task struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
}

type StartEvent struct {
	ID                     string                  `xml:"id,attr"`
	Name                   string                  `xml:"name,attr"`
	Outgoing               []string                `xml:"outgoing"`
	TimerEventDefinition   *TimerEventDefinition   `xml:"timerEventDefinition"`
	MessageEventDefinition *MessageEventDefinition `xml:"messageEventDefinition"`
	TimerDefinition        *TimerDefinition        `xml:"-"`
	MessageName            string                  `xml:"-"` // resolved

	// Only meaningful on the start event of an event sub-process.
	ErrorEventDefinition  *ErrorEventDefinition  `xml:"errorEventDefinition"`
	SignalEventDefinition *SignalEventDefinition `xml:"signalEventDefinition"`
	IsInterrupting        string                 `xml:"isInterrupting,attr"` // BPMN default: true
	ErrorCode             string                 `xml:"-"`                   // resolved
	SignalName            string                 `xml:"-"`                   // resolved
}

// Interrupting reports the isInterrupting attribute, which defaults to true.
func (s *StartEvent) Interrupting() bool {
	return s.IsInterrupting != "false"
}

type EndEvent struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`

	TerminateEventDefinition *TerminateEventDefinition `xml:"terminateEventDefinition"`
	ErrorEventDefinition     *ErrorEventDefinition     `xml:"errorEventDefinition"`
	SignalEventDefinition    *SignalEventDefinition    `xml:"signalEventDefinition"`
	ErrorCode                string                    `xml:"-"` // resolved
	SignalName               string                    `xml:"-"` // resolved
}

// MultiInstanceLoopCharacteristics represents BPMN multi-instance configuration
type MultiInstanceLoopCharacteristics struct {
	IsSequential        bool   `xml:"isSequential,attr"`
	LoopCardinality     string `xml:"loopCardinality"`      // Expression like "3"
	CompletionCondition string `xml:"completionCondition"`  // Expression like "${nrOfCompletedInstances >= 2}"
	InputDataItem       string `xml:"inputDataItem"`        // Item name for each iteration
	OutputDataItem      string `xml:"outputDataItem"`       // Item name for output collection
	ElementVariable     string `xml:"elementVariable,attr"` // Variable name for current item
}

// Zeebe extension elements (for Camunda 8). Each of these is a direct child
// of <bpmn:extensionElements>, as siblings — not nested inside one another —
// so UserTask/ServiceTask each hold a single ExtensionElements field rather
// than one field per zeebe: element.
type ZeebeTaskDefinition struct {
	Type    string `xml:"type,attr"`
	Retries string `xml:"retries,attr"`
}

type ZeebeAssignmentDefinition struct {
	Assignee        string `xml:"assignee,attr"`
	CandidateGroups string `xml:"candidateGroups,attr"`
	CandidateUsers  string `xml:"candidateUsers,attr"`
}

type ZeebeFormDefinition struct {
	FormId  string `xml:"formId,attr"`
	FormKey string `xml:"formKey,attr"`
}

type ZeebeHeader struct {
	Key   string `xml:"key,attr"`
	Value string `xml:"value,attr"`
}

type ZeebeTaskHeaders struct {
	Headers []ZeebeHeader `xml:"header"`
}

type ZeebeIOEntry struct {
	Source string `xml:"source,attr"`
	Target string `xml:"target,attr"`
}

type ZeebeIoMapping struct {
	Inputs  []ZeebeIOEntry `xml:"input"`
	Outputs []ZeebeIOEntry `xml:"output"`
}

type ZeebeCalledDecision struct {
	DecisionId     string `xml:"decisionId,attr"`
	ResultVariable string `xml:"resultVariable,attr"`
}

// ZeebePriorityDefinition sets a user task's priority (0-100, default 50).
// The value can be a literal integer or a FEEL expression (prefixed "=").
type ZeebePriorityDefinition struct {
	Priority string `xml:"priority,attr"`
}

type ExtensionElements struct {
	ZeebeTaskDefinition       *ZeebeTaskDefinition       `xml:"taskDefinition"`
	ZeebeAssignmentDefinition *ZeebeAssignmentDefinition `xml:"assignmentDefinition"`
	ZeebeFormDefinition       *ZeebeFormDefinition       `xml:"formDefinition"`
	ZeebeTaskHeaders          *ZeebeTaskHeaders          `xml:"taskHeaders"`
	ZeebeIoMapping            *ZeebeIoMapping            `xml:"ioMapping"`
	ZeebeCalledDecision       *ZeebeCalledDecision       `xml:"calledDecision"`
	ZeebePriorityDefinition   *ZeebePriorityDefinition   `xml:"priorityDefinition"`
}

type UserTask struct {
	ID                     string                            `xml:"id,attr"`
	Name                   string                            `xml:"name,attr"`
	Incoming               []string                          `xml:"incoming"`
	Outgoing               []string                          `xml:"outgoing"`
	CamundaAssignee        string                            `xml:"http://camunda.org/schema/1.0/bpmn assignee,attr"`
	CamundaCandidateGroups string                            `xml:"http://camunda.org/schema/1.0/bpmn candidateGroups,attr"`
	CamundaFormKey         string                            `xml:"http://camunda.org/schema/1.0/bpmn formKey,attr"`
	Ext                    *ExtensionElements                `xml:"extensionElements"`
	MultiInstance          *MultiInstanceLoopCharacteristics `xml:"multiInstanceLoopCharacteristics"`
}

type ServiceTask struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`

	Ext *ExtensionElements `xml:"extensionElements"`

	CamundaType  string `xml:"http://camunda.org/schema/1.0/bpmn type,attr"`
	CamundaTopic string `xml:"http://camunda.org/schema/1.0/bpmn topic,attr"`

	MultiInstance *MultiInstanceLoopCharacteristics `xml:"multiInstanceLoopCharacteristics"`
}

type BusinessRuleTask struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`

	Ext *ExtensionElements `xml:"extensionElements"`
}

type ScriptTask struct {
	ID           string   `xml:"id,attr"`
	Name         string   `xml:"name,attr"`
	ScriptFormat string   `xml:"scriptFormat,attr"`
	Script       string   `xml:"script"`
	Incoming     []string `xml:"incoming"`
	Outgoing     []string `xml:"outgoing"`
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

type InclusiveGateway struct {
	ID       string   `xml:"id,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
}

type IntermediateCatchEvent struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`

	MessageEventDefinition *MessageEventDefinition `xml:"messageEventDefinition"`
	SignalEventDefinition  *SignalEventDefinition  `xml:"signalEventDefinition"`
	TimerEventDefinition   *TimerEventDefinition   `xml:"timerEventDefinition"`
	LinkEventDefinition    *LinkEventDefinition    `xml:"linkEventDefinition"`
	TimerDefinition        *TimerDefinition        `xml:"-"`
	MessageName            string                  `xml:"-"` // resolved from Messages map
	SignalName             string                  `xml:"-"` // resolved from Signals map
}

type BoundaryEvent struct {
	ID                     string                  `xml:"id,attr"`
	AttachedToRef          string                  `xml:"attachedToRef,attr"`
	CancelActivity         bool                    `xml:"cancelActivity,attr"`
	Name                   string                  `xml:"name,attr"`
	Outgoing               []string                `xml:"outgoing"`
	TimerEventDefinition   *TimerEventDefinition   `xml:"timerEventDefinition"`
	ErrorEventDefinition   *ErrorEventDefinition   `xml:"errorEventDefinition"`
	MessageEventDefinition *MessageEventDefinition `xml:"messageEventDefinition"`
	SignalEventDefinition  *SignalEventDefinition  `xml:"signalEventDefinition"`
	TimerDefinition        *TimerDefinition        `xml:"-"`
	MessageName            string                  `xml:"-"` // resolved
	SignalName             string                  `xml:"-"` // resolved
}

type BoundaryMessageEvent struct {
	ID             string
	AttachedToRef  string
	CancelActivity bool
	MessageName    string
}

type BoundarySignalEvent struct {
	ID             string
	AttachedToRef  string
	CancelActivity bool
	SignalName     string
}

type CallActivity struct {
	ID            string   `xml:"id,attr"`
	Name          string   `xml:"name,attr"`
	CalledElement string   `xml:"calledElement,attr"`
	Incoming      []string `xml:"incoming"`
	Outgoing      []string `xml:"outgoing"`
}

type EventBasedGateway struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
}

type MessageEventDefinition struct {
	ID         string `xml:"id,attr"`
	MessageRef string `xml:"messageRef,attr"`
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
	for _, process := range allScopes(&defs) {

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
	PopulateErrorBoundaryEvents(&defs)
	PopulateMessageEvents(&defs)
	PopulateSignalEvents(&defs)
	PopulateThrowAndEndEvents(&defs)

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

// ParseTimerCycle parses a BPMN timer cycle expression
// Examples:
//
//	"R/PT1M"        -> infinite, interval 1 minute
//	"R5/PT10S"      -> 5 repetitions, interval 10 seconds
//	"R3/PT1H30M"    -> 3 repetitions, interval 1 hour 30 minutes
func ParseTimerCycle(cycleStr string) (*TimerCycle, error) {
	// Format: R{count}/{duration}
	// count can be omitted for infinite (R/PT1M)

	parts := strings.Split(cycleStr, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid timer cycle format: %s", cycleStr)
	}

	// Parse repetition count
	countPart := strings.TrimPrefix(parts[0], "R")
	var remaining int = -1 // -1 = infinite

	if countPart != "" {
		count, err := strconv.Atoi(countPart)
		if err != nil {
			return nil, fmt.Errorf("invalid cycle count: %s", countPart)
		}
		remaining = count
	}

	// Parse duration
	durationStr := parts[1]
	if !strings.HasPrefix(durationStr, "PT") {
		return nil, fmt.Errorf("invalid duration format: %s", durationStr)
	}

	duration, err := parseDuration(durationStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse duration: %w", err)
	}

	return &TimerCycle{
		Remaining: remaining,
		Interval:  duration,
	}, nil
}

// parseDuration parses ISO 8601 duration like PT1M, PT30S, PT1H30M
func parseDuration(durationStr string) (time.Duration, error) {
	durationStr = strings.TrimPrefix(durationStr, "PT")
	var duration time.Duration

	// Parse hours
	if strings.Contains(durationStr, "H") {
		hoursStr := strings.Split(durationStr, "H")[0]
		if hours, err := strconv.Atoi(hoursStr); err == nil {
			duration += time.Duration(hours) * time.Hour
		}
		durationStr = strings.TrimPrefix(durationStr, hoursStr+"H")
	}

	// Parse minutes
	if strings.Contains(durationStr, "M") {
		minsStr := strings.Split(durationStr, "M")[0]
		if mins, err := strconv.Atoi(minsStr); err == nil {
			duration += time.Duration(mins) * time.Minute
		}
		durationStr = strings.TrimPrefix(durationStr, minsStr+"M")
	}

	// Parse seconds
	if strings.Contains(durationStr, "S") {
		secsStr := strings.Split(durationStr, "S")[0]
		if secs, err := strconv.Atoi(secsStr); err == nil {
			duration += time.Duration(secs) * time.Second
		}
	}

	if duration == 0 {
		return 0, fmt.Errorf("invalid duration: no time component found")
	}

	return duration, nil
}

func PopulateTimerEvents(defs *Definitions) {
	for _, process := range allScopes(defs) {

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

// PopulateErrorBoundaryEvents resolves error codes for boundary error events.
// Must be called after ParseBPMN, passing the Definitions that include bpmn:error elements.
func PopulateErrorBoundaryEvents(defs *Definitions) {
	// Build a map from error ID → error code
	errorCodeByID := make(map[string]string, len(defs.Errors))
	for _, e := range defs.Errors {
		errorCodeByID[e.ID] = e.ErrorCode
	}

	for _, process := range allScopes(defs) {
		process.BoundaryErrorEvents = []BoundaryErrorEvent{}

		for _, event := range process.BoundaryEvents {
			if event.ErrorEventDefinition == nil {
				continue
			}
			errorCode := ""
			if event.ErrorEventDefinition.ErrorRef != "" {
				errorCode = errorCodeByID[event.ErrorEventDefinition.ErrorRef]
			}
			process.BoundaryErrorEvents = append(process.BoundaryErrorEvents, BoundaryErrorEvent{
				ID:             event.ID,
				AttachedToRef:  event.AttachedToRef,
				CancelActivity: event.CancelActivity,
				ErrorCode:      errorCode,
			})
		}
	}
}

// PopulateMessageEvents resolves message names for intermediate message catch events and boundary message events.
func PopulateMessageEvents(defs *Definitions) {
	// Build map: message ID → message name
	msgNameByID := make(map[string]string, len(defs.Messages))
	for _, m := range defs.Messages {
		msgNameByID[m.ID] = m.Name
	}

	for _, process := range allScopes(defs) {
		process.BoundaryMessageEvents = []BoundaryMessageEvent{}

		// Resolve message names for intermediate catch events
		for j := range process.IntermediateCatchEvents {
			ev := &process.IntermediateCatchEvents[j]
			if ev.MessageEventDefinition != nil && ev.MessageEventDefinition.MessageRef != "" {
				ev.MessageName = msgNameByID[ev.MessageEventDefinition.MessageRef]
				if ev.MessageName == "" {
					ev.MessageName = ev.MessageEventDefinition.MessageRef // fallback to ref ID
				}
			}
		}

		// Resolve message names for start events
		for j := range process.StartEvents {
			sv := &process.StartEvents[j]
			if sv.MessageEventDefinition != nil && sv.MessageEventDefinition.MessageRef != "" {
				sv.MessageName = msgNameByID[sv.MessageEventDefinition.MessageRef]
				if sv.MessageName == "" {
					sv.MessageName = sv.MessageEventDefinition.MessageRef
				}
			}
		}

		// Resolve boundary message events
		for _, ev := range process.BoundaryEvents {
			if ev.MessageEventDefinition == nil {
				continue
			}
			msgName := ""
			if ev.MessageEventDefinition.MessageRef != "" {
				msgName = msgNameByID[ev.MessageEventDefinition.MessageRef]
				if msgName == "" {
					msgName = ev.MessageEventDefinition.MessageRef
				}
			}
			process.BoundaryMessageEvents = append(process.BoundaryMessageEvents, BoundaryMessageEvent{
				ID:             ev.ID,
				AttachedToRef:  ev.AttachedToRef,
				CancelActivity: ev.CancelActivity,
				MessageName:    msgName,
			})
		}
	}
}

// PopulateSignalEvents resolves signal names for intermediate signal catch events and boundary signal events.
func PopulateSignalEvents(defs *Definitions) {
	// Build map: signal ID → signal name
	sigNameByID := make(map[string]string, len(defs.Signals))
	for _, s := range defs.Signals {
		sigNameByID[s.ID] = s.Name
	}

	for _, process := range allScopes(defs) {
		process.BoundarySignalEvents = []BoundarySignalEvent{}

		// Resolve signal names for intermediate catch events
		for j := range process.IntermediateCatchEvents {
			ev := &process.IntermediateCatchEvents[j]
			if ev.SignalEventDefinition != nil && ev.SignalEventDefinition.SignalRef != "" {
				ev.SignalName = sigNameByID[ev.SignalEventDefinition.SignalRef]
				if ev.SignalName == "" {
					ev.SignalName = ev.SignalEventDefinition.SignalRef
				}
			}
		}

		// Resolve boundary signal events
		for _, ev := range process.BoundaryEvents {
			if ev.SignalEventDefinition == nil {
				continue
			}
			sigName := ""
			if ev.SignalEventDefinition.SignalRef != "" {
				sigName = sigNameByID[ev.SignalEventDefinition.SignalRef]
				if sigName == "" {
					sigName = ev.SignalEventDefinition.SignalRef
				}
			}
			process.BoundarySignalEvents = append(process.BoundarySignalEvents, BoundarySignalEvent{
				ID:             ev.ID,
				AttachedToRef:  ev.AttachedToRef,
				CancelActivity: ev.CancelActivity,
				SignalName:     sigName,
			})
		}
	}
}

// allScopes returns every process in defs plus the body of every
// sub-process nested in them (see Process.Scopes).
func allScopes(defs *Definitions) []*Process {
	var scopes []*Process
	for i := range defs.Processes {
		scopes = append(scopes, defs.Processes[i].Scopes()...)
	}
	return scopes
}

// PopulateThrowAndEndEvents resolves the message, signal and error names
// referenced by receive tasks, intermediate throw events and end events.
func PopulateThrowAndEndEvents(defs *Definitions) {
	msgNameByID := make(map[string]string, len(defs.Messages))
	for _, m := range defs.Messages {
		msgNameByID[m.ID] = m.Name
	}
	sigNameByID := make(map[string]string, len(defs.Signals))
	for _, s := range defs.Signals {
		sigNameByID[s.ID] = s.Name
	}
	errorCodeByID := make(map[string]string, len(defs.Errors))
	for _, e := range defs.Errors {
		errorCodeByID[e.ID] = e.ErrorCode
	}
	resolve := func(names map[string]string, ref string) string {
		if name := names[ref]; name != "" {
			return name
		}
		return ref // fall back to the ref ID, like the other resolvers
	}

	for _, process := range allScopes(defs) {
		for j := range process.StartEvents {
			sv := &process.StartEvents[j]
			if sv.SignalEventDefinition != nil && sv.SignalEventDefinition.SignalRef != "" {
				sv.SignalName = resolve(sigNameByID, sv.SignalEventDefinition.SignalRef)
			}
			if sv.ErrorEventDefinition != nil && sv.ErrorEventDefinition.ErrorRef != "" {
				sv.ErrorCode = errorCodeByID[sv.ErrorEventDefinition.ErrorRef]
			}
		}
		for j := range process.ReceiveTasks {
			rt := &process.ReceiveTasks[j]
			if rt.MessageRef != "" {
				rt.MessageName = resolve(msgNameByID, rt.MessageRef)
			}
		}
		for j := range process.IntermediateThrowEvents {
			ev := &process.IntermediateThrowEvents[j]
			if ev.SignalEventDefinition != nil && ev.SignalEventDefinition.SignalRef != "" {
				ev.SignalName = resolve(sigNameByID, ev.SignalEventDefinition.SignalRef)
			}
		}
		for j := range process.EndEvents {
			ev := &process.EndEvents[j]
			if ev.SignalEventDefinition != nil && ev.SignalEventDefinition.SignalRef != "" {
				ev.SignalName = resolve(sigNameByID, ev.SignalEventDefinition.SignalRef)
			}
			if ev.ErrorEventDefinition != nil && ev.ErrorEventDefinition.ErrorRef != "" {
				ev.ErrorCode = errorCodeByID[ev.ErrorEventDefinition.ErrorRef]
			}
		}
	}
}
