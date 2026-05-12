package engine

import (
	"fmt"

	"github.com/jeremielodi/goflow/internal/parser"
)

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

	// ---- Start Events ----
	for _, s := range process.StartEvents {
		register(&Node{
			ID:   s.ID,
			Type: StartEventType,
		})
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

	// ---- Sequence Flows ----
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

	return graph, nil
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
