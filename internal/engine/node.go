package engine

type NodeType string

const (
	StartEventType             NodeType = "startEvent"
	StartTimerEventType        NodeType = "startTimerEvent" // Timer start event
	EndEventType               NodeType = "endEvent"
	TaskType                   NodeType = "task"
	UserTaskType               NodeType = "userTask"
	ScriptTaskType             NodeType = "scriptTask"
	BusinessRuleTaskType       NodeType = "businessRuleTask"
	ServiceTaskType            NodeType = "serviceTask"
	ExclusiveGatewayType       NodeType = "exclusiveGateway"
	ParallelGatewayType        NodeType = "parallelGateway"
	InclusiveGatewayType       NodeType = "inclusiveGateway"
	ErrorBoundaryEventType     NodeType = "errorBoundaryEvent"
	IntermediateCatchEventType NodeType = "intermediateCatchEvent"
	IntermediateTimerEventType NodeType = "intermediateTimerEvent" // Timer catch event
	BoundaryTimerEventType     NodeType = "boundaryTimerEvent"     // Timer on boundary

	// Message events
	IntermediateMessageCatchEventType NodeType = "messageIntermediateCatchEvent"
	MessageStartEventType             NodeType = "messageStartEvent"
	MessageBoundaryEventType          NodeType = "messageBoundaryEvent"

	// Signal events
	IntermediateSignalCatchEventType NodeType = "signalIntermediateCatchEvent"
	IntermediateSignalThrowEventType NodeType = "signalIntermediateThrowEvent"
	SignalBoundaryEventType          NodeType = "signalBoundaryEvent"

	// Event-based gateway
	EventBasedGatewayType NodeType = "eventBasedGateway"

	// Call Activity (sub-process invocation)
	CallActivityType NodeType = "callActivity"

	// Embedded sub-process: its body is flattened into the same graph, each
	// inner node carrying the sub-process's ID in ParentScope.
	SubProcessType NodeType = "subProcess"

	// Event sub-process (triggeredByEvent): started by its start event's
	// trigger while its enclosing scope is active. TriggerType says which
	// field holds the trigger (TimerDefinition, MessageRef, SignalRef or
	// ErrorCode); CancelActivity holds the start event's isInterrupting.
	EventSubProcessType NodeType = "eventSubProcess"

	// Pass-through elements: the token moves straight on.
	ManualTaskType             NodeType = "manualTask"
	IntermediateThrowEventType NodeType = "intermediateThrowEvent" // none or link throw
	LinkCatchEventType         NodeType = "linkIntermediateCatchEvent"

	// End event variants (a plain end event stays EndEventType).
	TerminateEndEventType NodeType = "terminateEndEvent"
	ErrorEndEventType     NodeType = "errorEndEvent"
	SignalEndEventType    NodeType = "signalEndEvent"
)

type Flow struct {
	ID         string
	SourceRef  string
	TargetRef  string
	Condition  string
	IsBoundary bool // true for flows generated from boundary event attachments
}

type Node struct {
	ID                 string
	Name               string
	Type               NodeType
	AssigneeExpr       *string
	CandidateGroupExpr *string
	Incoming           []string
	Outgoing           []Flow
	JobType            *string              // for service tasks
	MultiInstance      *MultiInstanceConfig `json:"multiInstance,omitempty"`
	// Timer support
	TimerDefinition string `json:"timerDefinition,omitempty"` // Raw XML timer definition

	// Boundary event support
	AttachedToRef  string `json:"attachedToRef,omitempty"`  // Which node this boundary attaches to
	CancelActivity bool   `json:"cancelActivity,omitempty"` // true = interrupting, false = non-interrupting
	ErrorCode      string `json:"errorCode,omitempty"`      // BPMN error code for error boundary events

	// Message/Signal event support
	MessageRef string `json:"messageRef,omitempty"` // Message name (resolved from bpmn:message)
	SignalRef  string `json:"signalRef,omitempty"`  // Signal name (resolved from bpmn:signal)

	// Call Activity support
	CalledElement string `json:"calledElement,omitempty"` // Process key of the called process

	// Script task support
	Script       *string `json:"script,omitempty"`
	ScriptFormat *string `json:"scriptFormat,omitempty"`

	// Zeebe extension support (see internal/parser.ExtensionElements)
	FormKey        string            `json:"formKey,omitempty"`
	TaskHeaders    map[string]string `json:"taskHeaders,omitempty"`
	InputMappings  []IOMapping       `json:"inputMappings,omitempty"`
	OutputMappings []IOMapping       `json:"outputMappings,omitempty"`
	PriorityExpr   *string           `json:"priorityExpr,omitempty"` // zeebe:priorityDefinition

	// Business Rule Task support (zeebe:calledDecision)
	DecisionKey    string `json:"decisionKey,omitempty"`
	ResultVariable string `json:"resultVariable,omitempty"`

	// Embedded sub-process support. ParentScope is the ID of the
	// sub-process this node sits in ("" = the process itself);
	// StartNodeID is set on a SubProcessType node to its inner start event.
	ParentScope string `json:"parentScope,omitempty"`
	StartNodeID string `json:"startNodeId,omitempty"`

	// Link events: a link throw event jumps to the link catch event with
	// the same name in the same scope.
	LinkName string `json:"linkName,omitempty"`

	// Event sub-process trigger: "timer", "message", "signal" or "error".
	TriggerType string `json:"triggerType,omitempty"`
}

// Event sub-process trigger types.
const (
	TriggerTimer   = "timer"
	TriggerMessage = "message"
	TriggerSignal  = "signal"
	TriggerError   = "error"
)

// IsEndEvent reports whether n is an end event of any kind.
func (n *Node) IsEndEvent() bool {
	switch n.Type {
	case EndEventType, TerminateEndEventType, ErrorEndEventType, SignalEndEventType:
		return true
	}
	return false
}

// IsProcessStart reports whether n is a start event of the process itself,
// as opposed to the start event of an embedded sub-process.
func (n *Node) IsProcessStart() bool {
	return n.Type == StartEventType && n.ParentScope == ""
}

// IOMapping is a zeebe:ioMapping input or output entry. Source is a FEEL
// expression (conventionally prefixed with "="); Target is the variable name
// the evaluated result is written to.
type IOMapping struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

// MultiInstanceConfig holds multi-instance configuration
type MultiInstanceConfig struct {
	IsSequential        bool   `json:"isSequential"`
	LoopCardinality     string `json:"loopCardinality"`
	CompletionCondition string `json:"completionCondition"`
	InputDataItem       string `json:"inputDataItem"`
	OutputDataItem      string `json:"outputDataItem"`
	ElementVariable     string `json:"elementVariable"`
}
