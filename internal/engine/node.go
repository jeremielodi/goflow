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
	IntermediateCatchEventType NodeType = "intermediateCatchEvent"
	IntermediateTimerEventType NodeType = "intermediateTimerEvent" // Timer catch event
	BoundaryTimerEventType     NodeType = "boundaryTimerEvent"     // Timer on boundary

)

type Flow struct {
	ID        string
	SourceRef string
	TargetRef string
	Condition string
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
