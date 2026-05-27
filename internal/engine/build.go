package engine

import (
	"fmt"

	"github.com/jeremielodi/goflow/internal/parser"
)

type ProcessGraph struct {
	Nodes map[string]*Node
}

// ============================================================
// Build a graph from a single parser.Process (core builder)
// ============================================================
func BuildGraphFromProcess(process *parser.Process) (*ProcessGraph, error) {
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
		// Zeebe (nested extension)
		if t.ZeebeExt != nil && t.ZeebeExt.Assignment != nil && t.ZeebeExt.Assignment.Assignee != "" {
			node.AssigneeExpr = &t.ZeebeExt.Assignment.Assignee
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
		// Zeebe job type
		if t.ZeebeExt != nil && t.ZeebeExt.TaskDefinition != nil && t.ZeebeExt.TaskDefinition.Type != "" {
			node.JobType = &t.ZeebeExt.TaskDefinition.Type
		}
		register(node)
	}

	// ---- End Events ----
	for _, e := range process.EndEvents {
		register(&Node{
			ID:   e.ID,
			Type: EndEventType,
		})
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
						ID:        b.ID + "_boundary_flow",
						SourceRef: b.AttachedToRef,
						TargetRef: b.ID,
						Condition: "",
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
