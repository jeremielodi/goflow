package engine

import (
	"fmt"
	"slices"
	"strings"

	"github.com/jeremielodi/goflow/internal/parser"
)

// subProcessInfo describes one embedded sub-process found while flattening.
type subProcessInfo struct {
	ID          string
	Name        string
	ParentScope string // "" when the sub-process sits directly in the process

	// Set for an event sub-process (triggeredByEvent): its start events,
	// which carry the trigger.
	Event       bool
	EventStarts []parser.StartEvent
}

// flattenSubProcesses returns a copy of process whose element lists also
// contain every element of every (nested) embedded sub-process, so the
// existing graph builder registers and wires them like any other element.
// scopeOf maps each inner element's ID to the ID of the sub-process it
// belongs to; elements of the process itself are absent from it.
//
// BPMN requires element IDs to be unique across a whole definitions file,
// so flattening can't create ID collisions.
func flattenSubProcesses(process *parser.Process) (*parser.Process, map[string]string, []subProcessInfo, error) {
	flat := *process
	cloneLists(&flat)
	flat.SubProcesses = nil

	scopeOf := map[string]string{}
	var subs []subProcessInfo

	var walk func(sps []parser.SubProcess, parentScope string) error
	walk = func(sps []parser.SubProcess, parentScope string) error {
		for i := range sps {
			sp := &sps[i]
			info := subProcessInfo{ID: sp.ID, Name: sp.Name, ParentScope: parentScope}
			if sp.TriggeredByEvent {
				info.Event = true
				info.EventStarts = sp.StartEvents
			}
			subs = append(subs, info)
			if parentScope != "" {
				scopeOf[sp.ID] = parentScope
			}
			appendScope(&flat, &sp.Process, sp.ID, scopeOf)
			if err := walk(sp.SubProcesses, sp.ID); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(process.SubProcesses, ""); err != nil {
		return nil, nil, nil, err
	}
	return &flat, scopeOf, subs, nil
}

// cloneLists gives p its own copies of every element slice, so appending
// to them never writes into the parsed process's backing arrays.
func cloneLists(p *parser.Process) {
	p.StartEvents = slices.Clone(p.StartEvents)
	p.UserTasks = slices.Clone(p.UserTasks)
	p.Tasks = slices.Clone(p.Tasks)
	p.ServiceTasks = slices.Clone(p.ServiceTasks)
	p.EndEvents = slices.Clone(p.EndEvents)
	p.ExclusiveGateways = slices.Clone(p.ExclusiveGateways)
	p.ParallelGateways = slices.Clone(p.ParallelGateways)
	p.InclusiveGateways = slices.Clone(p.InclusiveGateways)
	p.IntermediateCatchEvents = slices.Clone(p.IntermediateCatchEvents)
	p.SequenceFlows = slices.Clone(p.SequenceFlows)
	p.ScriptTasks = slices.Clone(p.ScriptTasks)
	p.BusinessRuleTasks = slices.Clone(p.BusinessRuleTasks)
	p.CallActivities = slices.Clone(p.CallActivities)
	p.EventBasedGateways = slices.Clone(p.EventBasedGateways)
	p.IntermediateTimerEvents = slices.Clone(p.IntermediateTimerEvents)
	p.BoundaryTimerEvents = slices.Clone(p.BoundaryTimerEvents)
	p.BoundaryErrorEvents = slices.Clone(p.BoundaryErrorEvents)
	p.BoundaryMessageEvents = slices.Clone(p.BoundaryMessageEvents)
	p.BoundarySignalEvents = slices.Clone(p.BoundarySignalEvents)
	p.BoundaryEvents = slices.Clone(p.BoundaryEvents)
	p.ManualTasks = slices.Clone(p.ManualTasks)
	p.SendTasks = slices.Clone(p.SendTasks)
	p.ReceiveTasks = slices.Clone(p.ReceiveTasks)
	p.IntermediateThrowEvents = slices.Clone(p.IntermediateThrowEvents)
	p.Associations = slices.Clone(p.Associations)
}

// appendScope appends every element of body to flat, recording scope as
// the owning sub-process of each one.
func appendScope(flat, body *parser.Process, scope string, scopeOf map[string]string) {
	mark := func(id string) { scopeOf[id] = scope }

	for _, e := range body.StartEvents {
		mark(e.ID)
	}
	for _, e := range body.UserTasks {
		mark(e.ID)
	}
	for _, e := range body.Tasks {
		mark(e.ID)
	}
	for _, e := range body.ServiceTasks {
		mark(e.ID)
	}
	for _, e := range body.EndEvents {
		mark(e.ID)
	}
	for _, e := range body.ExclusiveGateways {
		mark(e.ID)
	}
	for _, e := range body.ParallelGateways {
		mark(e.ID)
	}
	for _, e := range body.InclusiveGateways {
		mark(e.ID)
	}
	for _, e := range body.IntermediateCatchEvents {
		mark(e.ID)
	}
	for _, e := range body.ScriptTasks {
		mark(e.ID)
	}
	for _, e := range body.BusinessRuleTasks {
		mark(e.ID)
	}
	for _, e := range body.CallActivities {
		mark(e.ID)
	}
	for _, e := range body.EventBasedGateways {
		mark(e.ID)
	}
	for _, e := range body.ManualTasks {
		mark(e.ID)
	}
	for _, e := range body.SendTasks {
		mark(e.ID)
	}
	for _, e := range body.ReceiveTasks {
		mark(e.ID)
	}
	for _, e := range body.IntermediateThrowEvents {
		mark(e.ID)
	}
	for _, e := range body.BoundaryEvents {
		mark(e.ID)
	}

	flat.StartEvents = append(flat.StartEvents, body.StartEvents...)
	flat.UserTasks = append(flat.UserTasks, body.UserTasks...)
	flat.Tasks = append(flat.Tasks, body.Tasks...)
	flat.ServiceTasks = append(flat.ServiceTasks, body.ServiceTasks...)
	flat.EndEvents = append(flat.EndEvents, body.EndEvents...)
	flat.ExclusiveGateways = append(flat.ExclusiveGateways, body.ExclusiveGateways...)
	flat.ParallelGateways = append(flat.ParallelGateways, body.ParallelGateways...)
	flat.InclusiveGateways = append(flat.InclusiveGateways, body.InclusiveGateways...)
	flat.IntermediateCatchEvents = append(flat.IntermediateCatchEvents, body.IntermediateCatchEvents...)
	flat.SequenceFlows = append(flat.SequenceFlows, body.SequenceFlows...)
	flat.ScriptTasks = append(flat.ScriptTasks, body.ScriptTasks...)
	flat.BusinessRuleTasks = append(flat.BusinessRuleTasks, body.BusinessRuleTasks...)
	flat.CallActivities = append(flat.CallActivities, body.CallActivities...)
	flat.EventBasedGateways = append(flat.EventBasedGateways, body.EventBasedGateways...)
	flat.IntermediateTimerEvents = append(flat.IntermediateTimerEvents, body.IntermediateTimerEvents...)
	flat.BoundaryTimerEvents = append(flat.BoundaryTimerEvents, body.BoundaryTimerEvents...)
	flat.BoundaryErrorEvents = append(flat.BoundaryErrorEvents, body.BoundaryErrorEvents...)
	flat.BoundaryMessageEvents = append(flat.BoundaryMessageEvents, body.BoundaryMessageEvents...)
	flat.BoundarySignalEvents = append(flat.BoundarySignalEvents, body.BoundarySignalEvents...)
	flat.BoundaryEvents = append(flat.BoundaryEvents, body.BoundaryEvents...)
	flat.ManualTasks = append(flat.ManualTasks, body.ManualTasks...)
	flat.SendTasks = append(flat.SendTasks, body.SendTasks...)
	flat.ReceiveTasks = append(flat.ReceiveTasks, body.ReceiveTasks...)
	flat.IntermediateThrowEvents = append(flat.IntermediateThrowEvents, body.IntermediateThrowEvents...)
	flat.Associations = append(flat.Associations, body.Associations...)
}

// finishSubProcesses stamps ParentScope on every inner node and links each
// sub-process node to its single inner start event.
func finishSubProcesses(graph *ProcessGraph, scopeOf map[string]string, subs []subProcessInfo) error {
	for id, scope := range scopeOf {
		if n, ok := graph.Nodes[id]; ok {
			n.ParentScope = scope
		}
	}
	for _, sp := range subs {
		node := graph.Nodes[sp.ID]
		if sp.Event {
			if err := finishEventSubProcess(node, sp); err != nil {
				return err
			}
			continue
		}
		for _, n := range graph.Nodes {
			if n.ParentScope != sp.ID {
				continue
			}
			switch n.Type {
			case StartEventType:
				if node.StartNodeID != "" {
					return fmt.Errorf("sub-process %s has more than one start event", sp.ID)
				}
				node.StartNodeID = n.ID
			case StartTimerEventType, MessageStartEventType, SignalStartEventType:
				return fmt.Errorf("sub-process %s: start event %s must be a plain (none) start event", sp.ID, n.ID)
			}
		}
		if node.StartNodeID == "" {
			return fmt.Errorf("sub-process %s has no start event", sp.ID)
		}
	}
	return nil
}

// finishEventSubProcess copies an event sub-process's trigger from its
// single start event onto its node.
func finishEventSubProcess(node *Node, sp subProcessInfo) error {
	if len(sp.EventStarts) != 1 {
		return fmt.Errorf("event sub-process %s must have exactly one start event, found %d", sp.ID, len(sp.EventStarts))
	}
	s := sp.EventStarts[0]
	node.StartNodeID = s.ID
	node.CancelActivity = s.Interrupting()

	switch {
	case s.TimerDefinition != nil && s.TimerDefinition.RawXML != "":
		node.TriggerType = TriggerTimer
		node.TimerDefinition = s.TimerDefinition.RawXML
	case s.MessageEventDefinition != nil:
		node.TriggerType = TriggerMessage
		node.MessageRef = s.MessageName
		node.MessageCorrelationKey = s.MessageCorrelationKey
	case s.SignalEventDefinition != nil:
		node.TriggerType = TriggerSignal
		node.SignalRef = s.SignalName
	case s.ErrorEventDefinition != nil:
		if !node.CancelActivity {
			return fmt.Errorf("event sub-process %s: an error start event is always interrupting", sp.ID)
		}
		node.TriggerType = TriggerError
		node.ErrorCode = s.ErrorCode
	case s.EscalationEventDefinition != nil:
		node.TriggerType = TriggerEscalation
		node.EscalationCode = s.EscalationCode
	case s.ConditionalEventDefinition != nil:
		node.TriggerType = TriggerConditional
		node.Condition = strings.TrimSpace(s.ConditionalEventDefinition.Condition)
	default:
		return fmt.Errorf("event sub-process %s: start event %s needs a timer, message, signal, error, escalation or conditional trigger", sp.ID, s.ID)
	}
	return nil
}
