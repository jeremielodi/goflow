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

// ValidateShape rejects every BPMN graph except the shapes the
// partitioned engine supports so far:
//   - Phase 1/3/4/5 (linear): one start event -> a chain of one or more
//     external service tasks and/or business rule tasks (DMN
//     zeebe:calledDecision), each flowing directly into the next -> one
//     end event. Any service task in the chain may have at most one
//     attached error boundary event (Phase 5), whose own single outgoing
//     flow must target the chain's shared end event directly.
//   - Phase 6 (timer-only linear): one start event -> exactly one
//     intermediate timer event -> one end event, with no service/business-
//     rule tasks at all. Kept separate from the task chain above rather
//     than mixed in — not combined with Phase 3/4's chain generalizations,
//     per this package's discipline of never combining two generalizations
//     in one phase. See internal/service.TimerPartitionRouter for how a
//     due timer on this shape gets routed through the Command Gateway.
//   - Phase 2 (diamond): one start event -> one exclusive gateway with
//     exactly 2 outgoing flows, each with an explicit non-empty condition,
//     each targeting a distinct service task -> both tasks flow to one
//     shared end event. Diamond branches stay service-task-only with no
//     boundary events or timers.
//
// Called only when a process definition is flipped to the partitioned
// write path (PATCH .../write-path) — this is an explicit scope fence, not
// a silent gap: parallel/inclusive gateways, boundary timers, multi-
// instance, and message/signal correlation are simply not implemented by
// the partitioned engine yet (see the design doc's "Explicit non-goals").
func ValidateShape(graph *engine.ProcessGraph) error {
	var startNodes, endNodes, serviceTaskNodes, businessRuleNodes, gatewayNodes, boundaryNodes, timerNodes []*engine.Node
	for _, n := range graph.Nodes {
		switch n.Type {
		case engine.StartEventType:
			startNodes = append(startNodes, n)
		case engine.ServiceTaskType:
			serviceTaskNodes = append(serviceTaskNodes, n)
		case engine.BusinessRuleTaskType:
			businessRuleNodes = append(businessRuleNodes, n)
		case engine.EndEventType:
			endNodes = append(endNodes, n)
		case engine.ExclusiveGatewayType:
			gatewayNodes = append(gatewayNodes, n)
		case engine.ErrorBoundaryEventType:
			boundaryNodes = append(boundaryNodes, n)
		case engine.IntermediateTimerEventType:
			timerNodes = append(timerNodes, n)
		default:
			return unsupportedShapeErr(n.Type)
		}
	}
	if len(startNodes) != 1 {
		return unsupportedShapeErr("")
	}
	startNode := startNodes[0]

	switch {
	case len(gatewayNodes) == 0 && len(timerNodes) == 0 && len(serviceTaskNodes)+len(businessRuleNodes) >= 1:
		if len(endNodes) != 1 {
			return unsupportedShapeErr("")
		}
		chainNodes := make([]*engine.Node, 0, len(serviceTaskNodes)+len(businessRuleNodes))
		chainNodes = append(chainNodes, serviceTaskNodes...)
		chainNodes = append(chainNodes, businessRuleNodes...)
		if err := validateLinearShape(startNode, chainNodes, endNodes[0]); err != nil {
			return err
		}
		return validateBoundaryEvents(boundaryNodes, serviceTaskNodes, endNodes[0])
	case len(gatewayNodes) == 0 && len(timerNodes) == 1 && len(serviceTaskNodes)+len(businessRuleNodes) == 0 && len(boundaryNodes) == 0:
		if len(endNodes) != 1 {
			return unsupportedShapeErr("")
		}
		return validateLinearShape(startNode, timerNodes, endNodes[0])
	case len(gatewayNodes) == 1 && len(serviceTaskNodes) == 2 && len(businessRuleNodes) == 0 && len(boundaryNodes) == 0 && len(timerNodes) == 0:
		if len(endNodes) != 1 {
			return fmt.Errorf("the diamond shape requires both branches to converge on exactly one shared end event, found %d end events", len(endNodes))
		}
		return validateDiamondShape(startNode, gatewayNodes[0], serviceTaskNodes, endNodes[0])
	default:
		return unsupportedShapeErr("")
	}
}

// validateBoundaryEvents checks every error boundary event in the graph
// against the linear chain's shape: attached to one of the chain's
// service tasks (not a business rule task — that executor doesn't raise a
// BPMN-catchable error the way a failed job does), at most one per task,
// a non-empty error code, and a single outgoing flow straight to the
// chain's end event (no sub-chain from the boundary — the smallest
// integration that still lets service.FailJob's boundary-trigger branch,
// already generic per runtime.ExecuteExecution's ErrorBoundaryEventType
// case, be exercised for real).
func validateBoundaryEvents(boundaryNodes, serviceTaskNodes []*engine.Node, endNode *engine.Node) error {
	serviceTaskByID := make(map[string]bool, len(serviceTaskNodes))
	for _, t := range serviceTaskNodes {
		serviceTaskByID[t.ID] = true
	}
	attachedTo := map[string]bool{}
	for _, b := range boundaryNodes {
		if b.AttachedToRef == "" || !serviceTaskByID[b.AttachedToRef] {
			return fmt.Errorf("error boundary event %s must be attached to one of the chain's service tasks", b.ID)
		}
		if attachedTo[b.AttachedToRef] {
			return fmt.Errorf("service task %s has more than one attached error boundary event, only one is supported", b.AttachedToRef)
		}
		attachedTo[b.AttachedToRef] = true
		if strings.TrimSpace(b.ErrorCode) == "" {
			return fmt.Errorf("error boundary event %s requires a non-empty error code", b.ID)
		}
		if len(b.Outgoing) != 1 || b.Outgoing[0].TargetRef != endNode.ID {
			return fmt.Errorf("error boundary event %s must flow directly to the chain's end event", b.ID)
		}
	}
	return nil
}

// validateLinearShape walks the chain from startNode's single outgoing
// flow through chain nodes — service tasks and/or business rule tasks,
// mixed in any order (each must have exactly one outgoing flow to the
// next node) — until reaching endNode. Confirmed by research before
// generalizing this from Phase 1's single-service-task case:
// service.CompleteJob and runtime.ExecuteExecution have no assumption
// about chain length or which of these two node types a given link is, so
// this validator is the only thing that needs to change to support either
// generalization.
func validateLinearShape(startNode *engine.Node, chainNodes []*engine.Node, endNode *engine.Node) error {
	nodeByID := make(map[string]*engine.Node, len(chainNodes))
	for _, n := range chainNodes {
		nodeByID[n.ID] = n
	}

	if len(startNode.Outgoing) != 1 {
		return fmt.Errorf("the linear shape requires exactly one outgoing flow from the start event")
	}

	visited := map[string]bool{}
	current := startNode.Outgoing[0].TargetRef
	for {
		node, ok := nodeByID[current]
		if !ok {
			return fmt.Errorf("the linear shape requires a chain of service/business-rule tasks connecting start to end, found unexpected node %q", current)
		}
		if visited[current] {
			return fmt.Errorf("the linear shape does not support a loop back to task %q", current)
		}
		visited[current] = true
		if len(node.Outgoing) != 1 {
			return fmt.Errorf("the linear shape requires task %q to have exactly one outgoing flow", current)
		}
		next := node.Outgoing[0].TargetRef
		if next == endNode.ID {
			break
		}
		current = next
	}
	if len(visited) != len(chainNodes) {
		return fmt.Errorf("the linear shape requires every task to be part of the single start-to-end chain")
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
		return fmt.Errorf("unsupported node type %q — the partitioned write path only supports startEvent, serviceTask, businessRuleTask, exclusiveGateway, errorBoundaryEvent, intermediateTimerEvent, and endEvent", badType)
	}
	return fmt.Errorf("the partitioned write path only supports three shapes: (1) start -> a chain of one or more service/business-rule tasks (each optionally with one attached error boundary event) -> end, (2) start -> exactly one intermediate timer event -> end (no tasks), or (3) start -> exclusiveGateway (exactly 2 outgoing flows, both with explicit conditions) -> {serviceTaskA | serviceTaskB} -> a shared end event")
}
