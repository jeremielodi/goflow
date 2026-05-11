package engine

type NodeType string

const (
	StartEventType             NodeType = "startEvent"
	EndEventType               NodeType = "endEvent"
	TaskType                   NodeType = "task"
	UserTaskType               NodeType = "userTask"
	ServiceTaskType            NodeType = "serviceTask"
	ExclusiveGatewayType       NodeType = "exclusiveGateway"
	ParallelGatewayType        NodeType = "parallelGateway"
	IntermediateCatchEventType NodeType = "intermediateCatchEvent"
)

type Flow struct {
	ID        string
	SourceRef string
	TargetRef string
	Condition string
}

type Node struct {
	ID   string
	Name string
	Type NodeType

	Incoming []string
	Outgoing []Flow
}
