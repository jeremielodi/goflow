// Package partition implements Phase 1/2's partitioned, Kafka-command-log
// write path for a narrow, explicitly-flagged subset of process
// definitions — see architecture/event-sourced-kafka-design.md. It has two
// halves that must stay in separate files (not separate packages) but
// never call each other:
//
//   - The Command Gateway (gateway.go, validate.go): called from
//     internal/api / internal/zeebegrpc / internal/worker at the exact
//     points those callers would otherwise call service.StartProcessInstance
//     or service.CompleteJob directly. Produces a command and blocks for the
//     resulting domain event.
//   - The Partition Processor (processor.go): started once from main.go,
//     consumes commands and calls the very same
//     service.StartProcessInstance / runtime.ExecuteExecution /
//     service.CompleteJob functions the legacy synchronous path already
//     calls — reusing their existing, already-tested transactional writes
//     and audit/eventlog side effects verbatim, rather than reimplementing
//     BPMN semantics a second time. This is why this package cannot import
//     internal/service from gateway.go/validate.go: internal/service would
//     then need to import internal/partition for the reverse call, an
//     import cycle. Only processor.go imports internal/service.
package partition

import (
	"fmt"
	"strings"

	"github.com/jeremielodi/goflow/internal/engine"
)

// ValidateShape rejects every BPMN graph except the two shapes the
// partitioned engine supports so far:
//   - Phase 1/3 (linear): one start event -> a chain of one or more
//     external service tasks, each flowing directly into the next ->
//     one end event.
//   - Phase 2 (diamond): one start event -> one exclusive gateway with
//     exactly 2 outgoing flows, each with an explicit non-empty condition,
//     each targeting a distinct service task -> both tasks flow to one
//     shared end event.
//
// Called only when a process definition is flipped to the partitioned
// write path (PATCH .../write-path) — this is an explicit scope fence, not
// a silent gap: parallel/inclusive gateways, timers, multi-instance,
// message/signal correlation, and DMN are simply not implemented by the
// partitioned engine yet (see the design doc's "Explicit non-goals").
func ValidateShape(graph *engine.ProcessGraph) error {
	var startNodes, endNodes, taskNodes, gatewayNodes []*engine.Node
	for _, n := range graph.Nodes {
		switch n.Type {
		case engine.StartEventType:
			startNodes = append(startNodes, n)
		case engine.ServiceTaskType:
			taskNodes = append(taskNodes, n)
		case engine.EndEventType:
			endNodes = append(endNodes, n)
		case engine.ExclusiveGatewayType:
			gatewayNodes = append(gatewayNodes, n)
		default:
			return unsupportedShapeErr(n.Type)
		}
	}
	if len(startNodes) != 1 {
		return unsupportedShapeErr("")
	}
	startNode := startNodes[0]

	switch {
	case len(gatewayNodes) == 0 && len(taskNodes) >= 1:
		if len(endNodes) != 1 {
			return unsupportedShapeErr("")
		}
		return validateLinearShape(startNode, taskNodes, endNodes[0])
	case len(gatewayNodes) == 1 && len(taskNodes) == 2:
		if len(endNodes) != 1 {
			return fmt.Errorf("the diamond shape requires both branches to converge on exactly one shared end event, found %d end events", len(endNodes))
		}
		return validateDiamondShape(startNode, gatewayNodes[0], taskNodes, endNodes[0])
	default:
		return unsupportedShapeErr("")
	}
}

// validateLinearShape walks the chain from startNode's single outgoing
// flow through service tasks (each must have exactly one outgoing flow to
// the next node), until reaching endNode. Confirmed by research before
// generalizing this from Phase 1's single-task case: service.CompleteJob
// and runtime.ExecuteExecution have no assumption about chain length, so
// this validator is the only thing that needs to change to support it.
func validateLinearShape(startNode *engine.Node, taskNodes []*engine.Node, endNode *engine.Node) error {
	taskByID := make(map[string]*engine.Node, len(taskNodes))
	for _, t := range taskNodes {
		taskByID[t.ID] = t
	}

	if len(startNode.Outgoing) != 1 {
		return fmt.Errorf("the linear shape requires exactly one outgoing flow from the start event")
	}

	visited := map[string]bool{}
	current := startNode.Outgoing[0].TargetRef
	for {
		task, ok := taskByID[current]
		if !ok {
			return fmt.Errorf("the linear shape requires a chain of service tasks connecting start to end, found unexpected node %q", current)
		}
		if visited[current] {
			return fmt.Errorf("the linear shape does not support a loop back to task %q", current)
		}
		visited[current] = true
		if len(task.Outgoing) != 1 {
			return fmt.Errorf("the linear shape requires service task %q to have exactly one outgoing flow", current)
		}
		next := task.Outgoing[0].TargetRef
		if next == endNode.ID {
			break
		}
		current = next
	}
	if len(visited) != len(taskNodes) {
		return fmt.Errorf("the linear shape requires every service task to be part of the single start-to-end chain")
	}
	return nil
}

func validateDiamondShape(startNode, gateway *engine.Node, taskNodes []*engine.Node, endNode *engine.Node) error {
	if len(startNode.Outgoing) != 1 || startNode.Outgoing[0].TargetRef != gateway.ID {
		return fmt.Errorf("the diamond shape requires start -> exclusiveGateway directly")
	}
	if len(gateway.Outgoing) != 2 {
		return fmt.Errorf("the diamond shape requires the exclusive gateway to have exactly 2 outgoing flows, got %d", len(gateway.Outgoing))
	}

	taskByID := map[string]*engine.Node{taskNodes[0].ID: taskNodes[0], taskNodes[1].ID: taskNodes[1]}
	targeted := map[string]bool{}
	for _, flow := range gateway.Outgoing {
		// GoFlow's engine.Flow has no explicit default-flow marker — an
		// empty condition is treated by common.ResolveNext as an implicit
		// default, indistinguishable from a forgotten condition. Requiring
		// both flows to be explicit here avoids needing new engine
		// metadata just for this validator.
		if strings.TrimSpace(flow.Condition) == "" {
			return fmt.Errorf("the diamond shape requires both exclusive gateway flows to have an explicit, non-empty condition — flow %s has none", flow.ID)
		}
		if _, ok := taskByID[flow.TargetRef]; !ok {
			return fmt.Errorf("the diamond shape requires both gateway flows to target one of the two service tasks — flow %s targets %s", flow.ID, flow.TargetRef)
		}
		targeted[flow.TargetRef] = true
	}
	if len(targeted) != 2 {
		return fmt.Errorf("the diamond shape requires the gateway's two flows to target two distinct service tasks")
	}

	for _, task := range taskNodes {
		if len(task.Outgoing) != 1 || task.Outgoing[0].TargetRef != endNode.ID {
			return fmt.Errorf("the diamond shape requires branch service task %s to flow directly, and only, to the shared end event", task.ID)
		}
	}
	return nil
}

func unsupportedShapeErr(badType engine.NodeType) error {
	if badType != "" {
		return fmt.Errorf("unsupported node type %q — the partitioned write path only supports startEvent, serviceTask, exclusiveGateway, and endEvent", badType)
	}
	return fmt.Errorf("the partitioned write path only supports two shapes: (1) start -> a chain of one or more service tasks -> end, or (2) start -> exclusiveGateway (exactly 2 outgoing flows, both with explicit conditions) -> {serviceTaskA | serviceTaskB} -> a shared end event")
}
