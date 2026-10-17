package engine

import (
	"fmt"
	"strings"

	"github.com/jeremielodi/goflow/internal/parser"
)

type ProcessGraph struct {
	Nodes map[string]*Node
}

// applyIoMapping copies a parsed zeebe:ioMapping element onto a node's
// InputMappings/OutputMappings, if present.
func applyIoMapping(node *Node, io *parser.ZeebeIoMapping) {
	if io == nil {
		return
	}
	for _, in := range io.Inputs {
		node.InputMappings = append(node.InputMappings, IOMapping{Source: in.Source, Target: in.Target})
	}
	for _, out := range io.Outputs {
		node.OutputMappings = append(node.OutputMappings, IOMapping{Source: out.Source, Target: out.Target})
	}
}

// ============================================================
// Build a graph from a single parser.Process (core builder)
// ============================================================
func BuildGraphFromProcess(process *parser.Process) (*ProcessGraph, error) {
	process, scopeOf, subProcesses, err := flattenSubProcesses(process)
	if err != nil {
		return nil, err
	}
	// A send task runs exactly like a service task (a job for a worker).
	// flattenSubProcesses returned a copy, so this never touches the parsed
	// process.
	process.ServiceTasks = append(process.ServiceTasks, process.SendTasks...)

	graph := &ProcessGraph{Nodes: map[string]*Node{}}
	nodeMap := map[string]*Node{}

	register := func(n *Node) {
		graph.Nodes[n.ID] = n
		nodeMap[n.ID] = n
	}

	// ---- Start Events (including timer start) ----
	for _, s := range process.StartEvents {
		node := &Node{
			ID:   s.ID,
			Type: StartEventType,
		}

		// Check for timer definition
		if s.TimerDefinition != nil && s.TimerDefinition.RawXML != "" {
			node.TimerDefinition = s.TimerDefinition.RawXML
			node.Type = StartTimerEventType
		}

		register(node)
	}

	// ---- Generic Tasks (treat as UserTasks) ----
	for _, t := range process.Tasks {
		node := &Node{
			ID:   t.ID,
			Name: t.Name,
			Type: UserTaskType, // Treat generic tasks as UserTasks
		}
		register(node)
	}

	// ---- User Tasks (assignee & candidate groups) ----
	for _, t := range process.UserTasks {
		node := &Node{
			ID:   t.ID,
			Name: t.Name,
			Type: UserTaskType,
		}
		// Camunda 7 (direct attributes)
		if t.CamundaAssignee != "" {
			node.AssigneeExpr = &t.CamundaAssignee
		}
		if t.CamundaCandidateGroups != "" {
			node.CandidateGroupExpr = &t.CamundaCandidateGroups
		}
		if t.CamundaFormKey != "" {
			node.FormKey = t.CamundaFormKey
		}
		// Zeebe extension elements (override Camunda 7 attributes when present)
		if t.Ext != nil {
			if ad := t.Ext.ZeebeAssignmentDefinition; ad != nil {
				if ad.Assignee != "" {
					node.AssigneeExpr = &ad.Assignee
				}
				if ad.CandidateGroups != "" {
					node.CandidateGroupExpr = &ad.CandidateGroups
				}
			}
			if fd := t.Ext.ZeebeFormDefinition; fd != nil {
				if fd.FormKey != "" {
					node.FormKey = fd.FormKey
				} else if fd.FormId != "" {
					node.FormKey = fd.FormId
				}
			}
			if pd := t.Ext.ZeebePriorityDefinition; pd != nil && pd.Priority != "" {
				node.PriorityExpr = &pd.Priority
			}
			applyIoMapping(node, t.Ext.ZeebeIoMapping)
		}

		// ✅ Add multi-instance support
		if t.MultiInstance != nil {
			node.MultiInstance = &MultiInstanceConfig{
				IsSequential:        t.MultiInstance.IsSequential,
				LoopCardinality:     t.MultiInstance.LoopCardinality,
				CompletionCondition: t.MultiInstance.CompletionCondition,
				InputDataItem:       t.MultiInstance.InputDataItem,
				OutputDataItem:      t.MultiInstance.OutputDataItem,
				ElementVariable:     t.MultiInstance.ElementVariable,
			}
		}

		register(node)
	}

	// ---- Service Tasks (job type) ----
	for _, t := range process.ServiceTasks {
		node := &Node{
			ID:   t.ID,
			Name: t.Name,
			Type: ServiceTaskType,
		}
		// Camunda 7 external topic
		if t.CamundaTopic != "" {
			node.JobType = &t.CamundaTopic
		}
		// Zeebe extension elements
		if t.Ext != nil {
			if td := t.Ext.ZeebeTaskDefinition; td != nil && td.Type != "" {
				node.JobType = &td.Type
			}
			if th := t.Ext.ZeebeTaskHeaders; th != nil && len(th.Headers) > 0 {
				node.TaskHeaders = make(map[string]string, len(th.Headers))
				for _, h := range th.Headers {
					node.TaskHeaders[h.Key] = h.Value
				}
			}
			applyIoMapping(node, t.Ext.ZeebeIoMapping)
		}

		// ✅ Add multi-instance support
		if t.MultiInstance != nil {
			node.MultiInstance = &MultiInstanceConfig{
				IsSequential:        t.MultiInstance.IsSequential,
				LoopCardinality:     t.MultiInstance.LoopCardinality,
				CompletionCondition: t.MultiInstance.CompletionCondition,
				InputDataItem:       t.MultiInstance.InputDataItem,
				OutputDataItem:      t.MultiInstance.OutputDataItem,
				ElementVariable:     t.MultiInstance.ElementVariable,
			}
		}
		register(node)
	}

	for _, t := range process.ScriptTasks {
    node := &Node{
        ID:   t.ID,
        Name: t.Name,
        Type: ScriptTaskType,  // Add this constant
    }
    // Store script content for execution
    if t.Script != "" {
        node.Script = &t.Script
        node.ScriptFormat = &t.ScriptFormat
    }
    register(node)
}

	// ---- Business Rule Tasks (zeebe:calledDecision) ----
	for _, t := range process.BusinessRuleTasks {
		node := &Node{
			ID:   t.ID,
			Name: t.Name,
			Type: BusinessRuleTaskType,
		}
		if t.Ext != nil {
			if cd := t.Ext.ZeebeCalledDecision; cd != nil {
				node.DecisionKey = cd.DecisionId
				node.ResultVariable = cd.ResultVariable
			}
			applyIoMapping(node, t.Ext.ZeebeIoMapping)
		}
		register(node)
	}
	// ---- End Events ----
	for _, e := range process.EndEvents {
		node := &Node{
			ID:   e.ID,
			Name: e.Name,
			Type: EndEventType,
		}
		switch {
		case e.TerminateEventDefinition != nil:
			node.Type = TerminateEndEventType
		case e.ErrorEventDefinition != nil:
			node.Type = ErrorEndEventType
			node.ErrorCode = e.ErrorCode
		case e.SignalEventDefinition != nil:
			node.Type = SignalEndEventType
			node.SignalRef = e.SignalName
		case e.EscalationEventDefinition != nil:
			node.Type = EscalationEndEventType
			node.EscalationCode = e.EscalationCode
		case e.CompensateEventDefinition != nil:
			node.Type = CompensationEndEventType
			node.CompensateActivity = e.CompensateEventDefinition.ActivityRef
		}
		register(node)
	}

	// ---- Compensation Boundary Events (link an activity to its handler) ----
	for _, b := range process.BoundaryEvents {
		if b.CompensateEventDefinition == nil {
			continue
		}
		register(&Node{
			ID:            b.ID,
			Name:          b.Name,
			Type:          CompensationBoundaryEventType,
			AttachedToRef: b.AttachedToRef,
		})
	}

	// ---- Conditional Boundary Events (armed like message boundaries) ----
	for _, b := range process.BoundaryEvents {
		if b.ConditionalEventDefinition == nil {
			continue
		}
		register(&Node{
			ID:             b.ID,
			Name:           b.Name,
			Type:           ConditionalBoundaryEventType,
			AttachedToRef:  b.AttachedToRef,
			CancelActivity: b.Interrupting(),
			Condition:      strings.TrimSpace(b.ConditionalEventDefinition.Condition),
		})
	}

	// ---- Escalation Boundary Events (matched when an escalation is thrown) ----
	for _, b := range process.BoundaryEvents {
		if b.EscalationEventDefinition == nil {
			continue
		}
		register(&Node{
			ID:             b.ID,
			Name:           b.Name,
			Type:           EscalationBoundaryEventType,
			AttachedToRef:  b.AttachedToRef,
			CancelActivity: b.Interrupting(),
			EscalationCode: b.EscalationCode,
		})
	}

	// ---- Manual Tasks (pass-through) ----
	for _, t := range process.ManualTasks {
		register(&Node{
			ID:   t.ID,
			Name: t.Name,
			Type: ManualTaskType,
		})
	}

	// ---- Receive Tasks (wait for a message, like a message catch event) ----
	for _, t := range process.ReceiveTasks {
		register(&Node{
			ID:         t.ID,
			Name:       t.Name,
			Type:       IntermediateMessageCatchEventType,
			MessageRef: t.MessageName,
		})
	}

	// ---- Intermediate Throw Events ----
	for _, ev := range process.IntermediateThrowEvents {
		node := &Node{
			ID:   ev.ID,
			Name: ev.Name,
			Type: IntermediateThrowEventType,
		}
		switch {
		case ev.SignalEventDefinition != nil:
			node.Type = IntermediateSignalThrowEventType
			node.SignalRef = ev.SignalName
		case ev.LinkEventDefinition != nil:
			node.LinkName = ev.LinkEventDefinition.Name
		case ev.EscalationEventDefinition != nil:
			node.Type = IntermediateEscalationThrowEventType
			node.EscalationCode = ev.EscalationCode
		case ev.CompensateEventDefinition != nil:
			node.Type = IntermediateCompensationThrowEventType
			node.CompensateActivity = ev.CompensateEventDefinition.ActivityRef
		case ev.MessageEventDefinition != nil:
			// Camunda 8 style: a message throw event with a job type is
			// worked by a job worker, like a service task. Without one
			// there is nothing to run, so it passes straight through.
			if ev.CamundaTopic != "" {
				node.Type = ServiceTaskType
				node.JobType = &ev.CamundaTopic
			}
			if ev.Ext != nil {
				if td := ev.Ext.ZeebeTaskDefinition; td != nil && td.Type != "" {
					node.Type = ServiceTaskType
					node.JobType = &td.Type
				}
				applyIoMapping(node, ev.Ext.ZeebeIoMapping)
			}
		}
		register(node)
	}

	// ---- Exclusive Gateways ----
	for _, g := range process.ExclusiveGateways {
		register(&Node{
			ID:   g.ID,
			Type: ExclusiveGatewayType,
		})
	}

	// ---- Parallel Gateways ----
	for _, g := range process.ParallelGateways {
		register(&Node{
			ID:   g.ID,
			Type: ParallelGatewayType,
		})
	}

	// ---- Inclusive Gateways ----
	for _, g := range process.InclusiveGateways {
		register(&Node{
			ID:   g.ID,
			Type: InclusiveGatewayType,
		})
	}

	// ---- Intermediate Timer Events ----
	for _, t := range process.IntermediateTimerEvents {
		node := &Node{
			ID:   t.ID,
			Name: t.Name,
			Type: IntermediateTimerEventType,
		}
		if t.TimerDefinition != nil && t.TimerDefinition.RawXML != "" {
			node.TimerDefinition = t.TimerDefinition.RawXML
		}
		register(node)
	}

	// ---- Boundary Timer Events ----
	for _, b := range process.BoundaryTimerEvents {
		node := &Node{
			ID:             b.ID,
			Type:           BoundaryTimerEventType,
			AttachedToRef:  b.AttachedToRef,
			CancelActivity: b.CancelActivity,
		}
		if b.TimerDefinition != nil && b.TimerDefinition.RawXML != "" {
			node.TimerDefinition = b.TimerDefinition.RawXML
		}
		register(node)
	}

	// ---- Boundary Error Events ----
	for _, b := range process.BoundaryErrorEvents {
		register(&Node{
			ID:             b.ID,
			Type:           ErrorBoundaryEventType,
			AttachedToRef:  b.AttachedToRef,
			CancelActivity: b.CancelActivity,
			ErrorCode:      b.ErrorCode,
		})
	}

	// ---- Intermediate Message Catch Events (and signal catch events) ----
	for _, ev := range process.IntermediateCatchEvents {
		if ev.MessageEventDefinition != nil {
			register(&Node{
				ID:         ev.ID,
				Name:       ev.Name,
				Type:       IntermediateMessageCatchEventType,
				MessageRef: ev.MessageName,
			})
		} else if ev.SignalEventDefinition != nil {
			register(&Node{
				ID:        ev.ID,
				Name:      ev.Name,
				Type:      IntermediateSignalCatchEventType,
				SignalRef: ev.SignalName,
			})
		} else if ev.LinkEventDefinition != nil {
			register(&Node{
				ID:       ev.ID,
				Name:     ev.Name,
				Type:     LinkCatchEventType,
				LinkName: ev.LinkEventDefinition.Name,
			})
		} else if ev.ConditionalEventDefinition != nil {
			register(&Node{
				ID:        ev.ID,
				Name:      ev.Name,
				Type:      IntermediateConditionalCatchEventType,
				Condition: strings.TrimSpace(ev.ConditionalEventDefinition.Condition),
			})
		}
		// Note: timer events are handled via process.IntermediateTimerEvents (already registered above)
	}

	// ---- Message Start Events ----
	for j := range process.StartEvents {
		sv := &process.StartEvents[j]
		if sv.MessageEventDefinition != nil {
			// Update the existing node to be a message start event
			if n, ok := nodeMap[sv.ID]; ok {
				n.Type = MessageStartEventType
				n.MessageRef = sv.MessageName
			}
		}
	}

	// ---- Boundary Message Events ----
	for _, b := range process.BoundaryMessageEvents {
		register(&Node{
			ID:             b.ID,
			Type:           MessageBoundaryEventType,
			AttachedToRef:  b.AttachedToRef,
			CancelActivity: b.CancelActivity,
			MessageRef:     b.MessageName,
		})
	}

	// ---- Boundary Signal Events ----
	for _, b := range process.BoundarySignalEvents {
		register(&Node{
			ID:             b.ID,
			Type:           SignalBoundaryEventType,
			AttachedToRef:  b.AttachedToRef,
			CancelActivity: b.CancelActivity,
			SignalRef:      b.SignalName,
		})
	}

	// ---- Event-based Gateways ----
	for _, g := range process.EventBasedGateways {
		register(&Node{
			ID:   g.ID,
			Name: g.Name,
			Type: EventBasedGatewayType,
		})
	}

	// ---- Call Activities ----
	for _, ca := range process.CallActivities {
		register(&Node{
			ID:            ca.ID,
			Name:          ca.Name,
			Type:          CallActivityType,
			CalledElement: ca.CalledElement,
		})
	}

	// ---- Embedded Sub-Processes ----
	for _, sp := range subProcesses {
		nodeType := SubProcessType
		if sp.Event {
			nodeType = EventSubProcessType
		}
		register(&Node{
			ID:   sp.ID,
			Name: sp.Name,
			Type: nodeType,
		})
	}

	// ---- Add incoming/outgoing edges from Sequence Flows ----
	for _, f := range process.SequenceFlows {
		source, ok := nodeMap[f.SourceRef]
		if !ok {
			return nil, fmt.Errorf("source node %s not found for flow %s", f.SourceRef, f.ID)
		}
		target, ok := nodeMap[f.TargetRef]
		if !ok {
			return nil, fmt.Errorf("target node %s not found for flow %s", f.TargetRef, f.ID)
		}
		flow := Flow{
			ID:        f.ID,
			SourceRef: f.SourceRef,
			TargetRef: f.TargetRef,
			Condition: f.Condition,
		}
		source.Outgoing = append(source.Outgoing, flow)
		target.Incoming = append(target.Incoming, f.SourceRef)
	}

	// ---- Add incoming/outgoing edges for Generic Tasks ----
	for _, task := range process.Tasks {
		for _, incoming := range task.Incoming {
			if source, ok := nodeMap[incoming]; ok {
				// Check if flow already exists to avoid duplicates
				found := false
				for _, existingFlow := range source.Outgoing {
					if existingFlow.TargetRef == task.ID {
						found = true
						break
					}
				}
				if !found {
					flow := Flow{
						ID:        incoming + "_to_" + task.ID,
						SourceRef: incoming,
						TargetRef: task.ID,
						Condition: "",
					}
					source.Outgoing = append(source.Outgoing, flow)
				}
			}
			if targetNode, ok := nodeMap[task.ID]; ok {
				// Check if incoming already exists
				found := false
				for _, existingIncoming := range targetNode.Incoming {
					if existingIncoming == incoming {
						found = true
						break
					}
				}
				if !found {
					targetNode.Incoming = append(targetNode.Incoming, incoming)
				}
			}
		}
		for _, outgoing := range task.Outgoing {
			if target, ok := nodeMap[outgoing]; ok {
				// Check if flow already exists to avoid duplicates
				found := false
				for _, existingFlow := range target.Incoming {
					if existingFlow == task.ID {
						found = true
						break
					}
				}
				if !found {
					flow := Flow{
						ID:        task.ID + "_to_" + outgoing,
						SourceRef: task.ID,
						TargetRef: outgoing,
						Condition: "",
					}
					if sourceNode, ok := nodeMap[task.ID]; ok {
						sourceNode.Outgoing = append(sourceNode.Outgoing, flow)
					}
					target.Incoming = append(target.Incoming, task.ID)
				}
			}
		}
	}

	// ---- Add incoming/outgoing edges for User Tasks ----
	for _, task := range process.UserTasks {
		for _, incoming := range task.Incoming {
			if source, ok := nodeMap[incoming]; ok {
				found := false
				for _, existingFlow := range source.Outgoing {
					if existingFlow.TargetRef == task.ID {
						found = true
						break
					}
				}
				if !found {
					flow := Flow{
						ID:        incoming + "_to_" + task.ID,
						SourceRef: incoming,
						TargetRef: task.ID,
						Condition: "",
					}
					source.Outgoing = append(source.Outgoing, flow)
				}
			}
		}
		for _, outgoing := range task.Outgoing {
			if target, ok := nodeMap[outgoing]; ok {
				found := false
				for _, existingFlow := range target.Incoming {
					if existingFlow == task.ID {
						found = true
						break
					}
				}
				if !found {
					flow := Flow{
						ID:        task.ID + "_to_" + outgoing,
						SourceRef: task.ID,
						TargetRef: outgoing,
						Condition: "",
					}
					if sourceNode, ok := nodeMap[task.ID]; ok {
						sourceNode.Outgoing = append(sourceNode.Outgoing, flow)
					}
					target.Incoming = append(target.Incoming, task.ID)
				}
			}
		}
	}

	// ---- Add incoming/outgoing edges for Service Tasks ----
	for _, task := range process.ServiceTasks {
		for _, incoming := range task.Incoming {
			if source, ok := nodeMap[incoming]; ok {
				found := false
				for _, existingFlow := range source.Outgoing {
					if existingFlow.TargetRef == task.ID {
						found = true
						break
					}
				}
				if !found {
					flow := Flow{
						ID:        incoming + "_to_" + task.ID,
						SourceRef: incoming,
						TargetRef: task.ID,
						Condition: "",
					}
					source.Outgoing = append(source.Outgoing, flow)
				}
			}
		}
		for _, outgoing := range task.Outgoing {
			if target, ok := nodeMap[outgoing]; ok {
				found := false
				for _, existingFlow := range target.Incoming {
					if existingFlow == task.ID {
						found = true
						break
					}
				}
				if !found {
					flow := Flow{
						ID:        task.ID + "_to_" + outgoing,
						SourceRef: task.ID,
						TargetRef: outgoing,
						Condition: "",
					}
					if sourceNode, ok := nodeMap[task.ID]; ok {
						sourceNode.Outgoing = append(sourceNode.Outgoing, flow)
					}
					target.Incoming = append(target.Incoming, task.ID)
				}
			}
		}
	}

	// ---- Add outgoing edges from Boundary Events ----
	for _, b := range process.BoundaryTimerEvents {
		// Find the boundary event node
		boundaryNode, ok := nodeMap[b.ID]
		if !ok {
			continue
		}

		// Add edge from attached node to boundary event
		// This allows the timer to be triggered from the attached task
		if b.AttachedToRef != "" {
			// Add incoming reference from attached node
			attachedNode, attachedOk := nodeMap[b.AttachedToRef]
			if attachedOk {
				// Check if flow already exists
				found := false
				for _, existingFlow := range attachedNode.Outgoing {
					if existingFlow.TargetRef == b.ID {
						found = true
						break
					}
				}
				if !found {
					// Create a flow from attached node to boundary event
					flow := Flow{
						ID:         b.ID + "_boundary_flow",
						SourceRef:  b.AttachedToRef,
						TargetRef:  b.ID,
						Condition:  "",
						IsBoundary: true,
					}
					attachedNode.Outgoing = append(attachedNode.Outgoing, flow)
					boundaryNode.Incoming = append(boundaryNode.Incoming, b.AttachedToRef)
				}
			}
		}

		// Add edges from boundary event to its targets from SequenceFlows
		for _, flow := range process.SequenceFlows {
			if flow.SourceRef == b.ID {
				if targetNode, ok := nodeMap[flow.TargetRef]; ok {
					// Check if flow already exists
					found := false
					for _, existingFlow := range boundaryNode.Outgoing {
						if existingFlow.TargetRef == flow.TargetRef {
							found = true
							break
						}
					}
					if !found {
						newFlow := Flow{
							ID:        flow.ID,
							SourceRef: b.ID,
							TargetRef: flow.TargetRef,
							Condition: flow.Condition,
						}
						boundaryNode.Outgoing = append(boundaryNode.Outgoing, newFlow)
						targetNode.Incoming = append(targetNode.Incoming, b.ID)
					}
				}
			}
		}
	}

	// ---- Add outgoing edges from Boundary Error Events ----
	for _, b := range process.BoundaryErrorEvents {
		boundaryNode, ok := nodeMap[b.ID]
		if !ok {
			continue
		}
		// Mark the attached task node so we can look it up quickly
		if b.AttachedToRef != "" {
			boundaryNode.AttachedToRef = b.AttachedToRef
		}
		// Add outgoing edges from boundary event to sequence flow targets
		for _, flow := range process.SequenceFlows {
			if flow.SourceRef != b.ID {
				continue
			}
			targetNode, ok := nodeMap[flow.TargetRef]
			if !ok {
				continue
			}
			found := false
			for _, existing := range boundaryNode.Outgoing {
				if existing.TargetRef == flow.TargetRef {
					found = true
					break
				}
			}
			if !found {
				boundaryNode.Outgoing = append(boundaryNode.Outgoing, Flow{
					ID:        flow.ID,
					SourceRef: b.ID,
					TargetRef: flow.TargetRef,
					Condition: flow.Condition,
				})
				targetNode.Incoming = append(targetNode.Incoming, b.ID)
			}
		}
	}

	if err := finishSubProcesses(graph, scopeOf, subProcesses); err != nil {
		return nil, err
	}
	if err := linkThrowEvents(graph); err != nil {
		return nil, err
	}
	if err := linkCompensationHandlers(graph, process.Associations); err != nil {
		return nil, err
	}

	// Validate parallel gateways
	for _, node := range graph.Nodes {
		if node.Type == ParallelGatewayType {
			if err := validateParallelGateway(node); err != nil {
				return nil, fmt.Errorf("invalid parallel gateway %s: %w", node.ID, err)
			}
		}

		// Validate boundary timer events
		if node.Type == BoundaryTimerEventType {
			if node.AttachedToRef == "" {
				return nil, fmt.Errorf("boundary timer event %s has no attachedToRef", node.ID)
			}
		}
	}

	return graph, nil
}

// validateParallelGateway ensures the gateway has proper fork/join structure
func validateParallelGateway(node *Node) error {
	incomingCount := len(node.Incoming)
	outgoingCount := len(node.Outgoing)

	if incomingCount == 1 && outgoingCount > 1 {
		// This is a fork - valid
		return nil
	}

	if incomingCount > 1 && outgoingCount == 1 {
		// This is a join - valid
		return nil
	}

	if incomingCount == 1 && outgoingCount == 1 {
		return fmt.Errorf("parallel gateway with single incoming and outgoing flow should be exclusive gateway")
	}

	// Can also be both (fork and join) - rare but valid
	if incomingCount > 1 && outgoingCount > 1 {
		return nil // Fork AND join in one gateway
	}

	return fmt.Errorf("invalid parallel gateway configuration: incoming=%d, outgoing=%d", incomingCount, outgoingCount)
}

// ============================================================
// Build a graph for a specific process key from raw BPMN bytes
// ============================================================
func BuildGraphForProcess(bpmnBytes []byte, processKey string) (*ProcessGraph, error) {
	defs, err := parser.ParseBPMN(bpmnBytes) // must return all processes in defs.Processes
	if err != nil {
		return nil, fmt.Errorf("failed to parse BPMN: %w", err)
	}
	var targetProcess *parser.Process
	for i := range defs.Processes {
		if defs.Processes[i].ID == processKey {
			targetProcess = &defs.Processes[i]
			break
		}
	}
	if targetProcess == nil {
		return nil, fmt.Errorf("process with ID %q not found", processKey)
	}
	return BuildGraphFromProcess(targetProcess)
}

// linkThrowEvents gives each link throw event a flow to the link catch event
// with the same name in the same scope — BPMN's "off-page connector".
func linkThrowEvents(graph *ProcessGraph) error {
	type key struct{ scope, name string }
	catches := map[key]*Node{}
	for _, n := range graph.Nodes {
		if n.Type != LinkCatchEventType {
			continue
		}
		k := key{n.ParentScope, n.LinkName}
		if other, dup := catches[k]; dup {
			return fmt.Errorf("link catch events %s and %s share the link name %q in the same scope", other.ID, n.ID, n.LinkName)
		}
		catches[k] = n
	}
	for _, n := range graph.Nodes {
		if n.Type != IntermediateThrowEventType || n.LinkName == "" {
			continue
		}
		target, ok := catches[key{n.ParentScope, n.LinkName}]
		if !ok {
			return fmt.Errorf("link throw event %s: no link catch event named %q in the same scope", n.ID, n.LinkName)
		}
		if len(n.Outgoing) > 0 {
			return fmt.Errorf("link throw event %s must not have outgoing sequence flows", n.ID)
		}
		n.Outgoing = append(n.Outgoing, Flow{ID: n.ID + "_link", SourceRef: n.ID, TargetRef: target.ID})
		target.Incoming = append(target.Incoming, n.ID)
	}
	return nil
}

// linkCompensationHandlers follows each compensation boundary event's
// association to its handler activity: the activity the boundary is
// attached to records its handler (CompensationHandler), and the handler —
// which has no sequence flows of its own — gets a flow to a synthetic
// CompensationHandlerEndType node, so it completes like any activity.
func linkCompensationHandlers(graph *ProcessGraph, associations []parser.Association) error {
	handlerOf := map[string]string{}
	for _, a := range associations {
		handlerOf[a.SourceRef] = a.TargetRef
	}
	for _, b := range graph.Nodes {
		if b.Type != CompensationBoundaryEventType {
			continue
		}
		handlerID, ok := handlerOf[b.ID]
		if !ok {
			return fmt.Errorf("compensation boundary event %s has no associated compensation handler", b.ID)
		}
		handler, ok := graph.Nodes[handlerID]
		if !ok {
			return fmt.Errorf("compensation boundary event %s: handler %s not found", b.ID, handlerID)
		}
		activity, ok := graph.Nodes[b.AttachedToRef]
		if !ok {
			return fmt.Errorf("compensation boundary event %s: activity %s not found", b.ID, b.AttachedToRef)
		}
		if len(handler.Outgoing) > 0 {
			return fmt.Errorf("compensation handler %s must not have outgoing sequence flows", handler.ID)
		}
		activity.CompensationHandler = handler.ID

		end := &Node{ID: handler.ID + "__compensated", Type: CompensationHandlerEndType, ParentScope: handler.ParentScope}
		graph.Nodes[end.ID] = end
		handler.Outgoing = append(handler.Outgoing, Flow{ID: end.ID + "_flow", SourceRef: handler.ID, TargetRef: end.ID})
		end.Incoming = append(end.Incoming, handler.ID)
	}
	return nil
}
