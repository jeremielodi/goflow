package parser

import "encoding/xml"

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
