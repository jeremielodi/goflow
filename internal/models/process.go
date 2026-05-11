package parser

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
