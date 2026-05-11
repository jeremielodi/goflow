package parser

import "encoding/xml"

type Definitions struct {
	Process Process `xml:"process"`
}

type Process struct {
	ID                string             `xml:"id,attr"`
	StartEvents       []StartEvent       `xml:"startEvent"`
	EndEvents         []EndEvent         `xml:"endEvent"`
	UserTasks         []UserTask         `xml:"userTask"`
	ServiceTasks      []ServiceTask      `xml:"serviceTask"`
	SequenceFlows     []SequenceFlow     `xml:"sequenceFlow"`
	ExclusiveGateways []ExclusiveGateway `xml:"exclusiveGateway"`
}

type Task struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
}

type StartEvent struct {
	ID       string   `xml:"id,attr"`
	Outgoing []string `xml:"outgoing"`
}

type EndEvent struct {
	ID       string   `xml:"id,attr"`
	Incoming []string `xml:"incoming"`
}

type UserTask struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
}

type ServiceTask struct {
	ID       string   `xml:"id,attr"`
	Name     string   `xml:"name,attr"`
	Incoming []string `xml:"incoming"`
	Outgoing []string `xml:"outgoing"`
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
}

type MessageEventDefinition struct {
	ID string `xml:"id,attr"`
}

type TimerEventDefinition struct {
	ID string `xml:"id,attr"`
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

func ParseBPMN(data []byte) (*Definitions, error) {
	var defs Definitions

	err := xml.Unmarshal(data, &defs)
	if err != nil {
		return nil, err
	}

	return &defs, nil
}
