package engine

import (
	"fmt"

	"github.com/jeremielodi/goflow/internal/parser"
)

func BuildGraph(defs *parser.Definitions) (*ProcessGraph, error) {

	graph := &ProcessGraph{
		Nodes: map[string]*Node{},
	}

	// ✅ IMPORTANT: fast lookup map (required for safe linking)
	nodeMap := map[string]*Node{}

	process := defs.Process

	// ----------------------------
	// helper to register nodes safely
	// ----------------------------
	register := func(n *Node) {
		graph.Nodes[n.ID] = n
		nodeMap[n.ID] = n
	}

	// ----------------------------
	// Start Events
	// ----------------------------
	for _, s := range process.StartEvents {
		register(&Node{
			ID:       s.ID,
			Type:     StartEventType,
			Incoming: []string{},
			Outgoing: []Flow{},
		})
	}

	// ----------------------------
	// User Tasks
	// ----------------------------
	for _, t := range process.UserTasks {
		register(&Node{
			ID:       t.ID,
			Name:     t.Name,
			Type:     UserTaskType,
			Incoming: []string{},
			Outgoing: []Flow{},
		})
	}

	// ----------------------------
	// Service Tasks
	// ----------------------------
	for _, t := range process.ServiceTasks {
		register(&Node{
			ID:       t.ID,
			Name:     t.Name,
			Type:     ServiceTaskType,
			Incoming: []string{},
			Outgoing: []Flow{},
		})
	}

	// ----------------------------
	// End Events
	// ----------------------------
	for _, e := range process.EndEvents {
		register(&Node{
			ID:       e.ID,
			Type:     EndEventType,
			Incoming: []string{},
			Outgoing: []Flow{},
		})
	}

	// ----------------------------
	// Exclusive Gateways
	// ----------------------------
	for _, g := range process.ExclusiveGateways {
		register(&Node{
			ID:       g.ID,
			Type:     ExclusiveGatewayType,
			Incoming: []string{},
			Outgoing: []Flow{},
		})
	}

	// ----------------------------
	// Sequence Flows
	// ----------------------------
	for _, f := range process.SequenceFlows {

		sourceNode, ok := nodeMap[f.SourceRef]
		if !ok {
			return nil, fmt.Errorf(
				"source node not found for flow %s (%s)",
				f.ID, f.SourceRef,
			)
		}

		targetNode, ok := nodeMap[f.TargetRef]
		if !ok {
			return nil, fmt.Errorf(
				"target node not found for flow %s (%s)",
				f.ID, f.TargetRef,
			)
		}

		flow := Flow{
			ID:        f.ID,
			SourceRef: f.SourceRef,
			TargetRef: f.TargetRef,
			Condition: f.Condition,
		}

		// forward link
		sourceNode.Outgoing = append(sourceNode.Outgoing, flow)

		// backward link
		targetNode.Incoming = append(targetNode.Incoming, f.SourceRef)
	}

	return graph, nil
}
