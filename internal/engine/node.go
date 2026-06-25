package engine

type NodeType string

const (
	StartEventType             NodeType = "startEvent"
	StartTimerEventType        NodeType = "startTimerEvent" // Timer start event
	EndEventType               NodeType = "endEvent"
	TaskType                   NodeType = "task"
	UserTaskType               NodeType = "userTask"
	ScriptTaskType             NodeType = "scriptTask"
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
