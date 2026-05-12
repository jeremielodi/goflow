package parser

import "encoding/xml"

type Definitions struct {
	XMLName   xml.Name  `xml:"definitions"`
	Processes []Process `xml:"process"` // change from single Process to slice
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
type CamundaExtensions struct {
	Assignee        string `xml:"assignee,attr"`
	CandidateGroups string `xml:"candidateGroups,attr"`
	TaskDefinition  *struct {
		Topic string `xml:"topic,attr"`
	} `xml:"taskDefinition"`
}
type CamundaTaskDefinition struct {
	Topic string `xml:"topic,attr"` // for external service tasks
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
