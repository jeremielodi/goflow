package partition

import (
	"testing"

	"github.com/jeremielodi/goflow/internal/engine"
)

func newGraph(nodes ...*engine.Node) *engine.ProcessGraph {
	g := &engine.ProcessGraph{Nodes: map[string]*engine.Node{}}
	for _, n := range nodes {
		g.Nodes[n.ID] = n
	}
	return g
}

func flow(id, target, condition string) engine.Flow {
	return engine.Flow{ID: id, TargetRef: target, Condition: condition}
}

func TestValidateShape_Linear_Passes(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task", "")}},
		&engine.Node{ID: "task", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f2", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err != nil {
		t.Fatalf("expected the linear shape (Phase 1's SingleTaskProcess) to pass, got: %v", err)
	}
}

func TestValidateShape_Diamond_Passes(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "taskB", `${branch == "b"}`),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "taskB", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f5", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err != nil {
		t.Fatalf("expected the diamond shape to pass, got: %v", err)
	}
}

func TestValidateShape_Diamond_ThreeOutgoingFlows_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "taskB", `${branch == "b"}`),
			flow("f6", "taskA", `${branch == "c"}`),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "taskB", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f5", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a 3-outgoing-flow gateway to be rejected")
	}
}

func TestValidateShape_Diamond_EmptyCondition_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "taskB", ""),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "taskB", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f5", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a gateway flow with no explicit condition to be rejected")
	}
}

func TestValidateShape_Diamond_BothFlowsSameTask_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "taskA", `${branch == "b"}`),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "taskB", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f5", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected both gateway flows targeting the same task to be rejected")
	}
}

func TestValidateShape_Diamond_DivergingEndEvents_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "taskB", `${branch == "b"}`),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "endA", "")}},
		&engine.Node{ID: "taskB", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f5", "endB", "")}},
		&engine.Node{ID: "endA", Type: engine.EndEventType},
		&engine.Node{ID: "endB", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected two end events (diverging branches) to be rejected — the shared-end connectivity check")
	}
}

func TestValidateShape_Diamond_TaskWithTwoOutgoingFlows_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "taskB", `${branch == "b"}`),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{
			flow("f4", "end", ""),
			flow("f6", "end", ""),
		}},
		&engine.Node{ID: "taskB", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f5", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a branch task with 2 outgoing flows to be rejected")
	}
}

func TestValidateShape_DisallowedNodeType_Rejected(t *testing.T) {
	linear := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task", "")}},
		&engine.Node{ID: "task", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f2", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
		&engine.Node{ID: "pg", Type: engine.ParallelGatewayType},
	)
	if err := ValidateShape(linear); err == nil {
		t.Fatal("expected a parallel gateway anywhere in the graph to be rejected, even alongside an otherwise-valid linear shape")
	}

	diamond := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "taskB", `${branch == "b"}`),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "taskB", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f5", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
		&engine.Node{ID: "timer", Type: engine.IntermediateTimerEventType},
	)
	if err := ValidateShape(diamond); err == nil {
		t.Fatal("expected a timer event anywhere in the graph to be rejected, even alongside an otherwise-valid diamond shape")
	}
}

func TestValidateShape_Chain_ThreeTasks_Passes(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task1", "")}},
		&engine.Node{ID: "task1", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f2", "task2", "")}},
		&engine.Node{ID: "task2", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f3", "task3", "")}},
		&engine.Node{ID: "task3", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err != nil {
		t.Fatalf("expected a 3-task chain to pass, got: %v", err)
	}
}

func TestValidateShape_Chain_MidChainBranch_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task1", "")}},
		&engine.Node{ID: "task1", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{
			flow("f2", "task2", ""),
			flow("f5", "end", ""),
		}},
		&engine.Node{ID: "task2", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f3", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a mid-chain task with 2 outgoing flows to be rejected")
	}
}

func TestValidateShape_Chain_Loop_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task1", "")}},
		&engine.Node{ID: "task1", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f2", "task2", "")}},
		&engine.Node{ID: "task2", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f3", "task1", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a chain looping back to an earlier task to be rejected")
	}
}

func TestValidateShape_Chain_DisconnectedTask_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task1", "")}},
		&engine.Node{ID: "task1", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f2", "end", "")}},
		&engine.Node{ID: "orphan", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f3", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected an extra service task unreachable from start to be rejected")
	}
}

func TestValidateShape_Chain_MixedServiceAndBusinessRuleTasks_Passes(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "evaluate", "")}},
		&engine.Node{ID: "evaluate", Type: engine.BusinessRuleTaskType, Outgoing: []engine.Flow{flow("f2", "apply", "")}},
		&engine.Node{ID: "apply", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f3", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err != nil {
		t.Fatalf("expected a chain mixing a business rule task and a service task to pass, got: %v", err)
	}
}

func TestValidateShape_Diamond_BusinessRuleTaskBranch_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "evaluate", `${branch == "b"}`),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "evaluate", Type: engine.BusinessRuleTaskType, Outgoing: []engine.Flow{flow("f5", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a diamond with a business rule task in place of a branch service task to be rejected — diamond branches stay service-task-only")
	}
}

func TestValidateShape_Chain_ValidBoundaryEvent_Passes(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task", "")}},
		&engine.Node{ID: "task", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f2", "end", "")}},
		&engine.Node{ID: "boundary", Type: engine.ErrorBoundaryEventType, AttachedToRef: "task", ErrorCode: "RISK_ERROR", Outgoing: []engine.Flow{flow("f3", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err != nil {
		t.Fatalf("expected a linear chain with a valid attached boundary event to pass, got: %v", err)
	}
}

func TestValidateShape_BoundaryEvent_OnBusinessRuleTask_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "evaluate", "")}},
		&engine.Node{ID: "evaluate", Type: engine.BusinessRuleTaskType, Outgoing: []engine.Flow{flow("f2", "end", "")}},
		&engine.Node{ID: "boundary", Type: engine.ErrorBoundaryEventType, AttachedToRef: "evaluate", ErrorCode: "RISK_ERROR", Outgoing: []engine.Flow{flow("f3", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a boundary event attached to a business rule task to be rejected")
	}
}

func TestValidateShape_BoundaryEvent_NoErrorCode_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task", "")}},
		&engine.Node{ID: "task", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f2", "end", "")}},
		&engine.Node{ID: "boundary", Type: engine.ErrorBoundaryEventType, AttachedToRef: "task", ErrorCode: "", Outgoing: []engine.Flow{flow("f3", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a boundary event with no error code to be rejected")
	}
}

func TestValidateShape_BoundaryEvent_WrongTarget_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task", "")}},
		&engine.Node{ID: "task", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f2", "end", "")}},
		&engine.Node{ID: "boundary", Type: engine.ErrorBoundaryEventType, AttachedToRef: "task", ErrorCode: "RISK_ERROR", Outgoing: []engine.Flow{flow("f3", "task", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a boundary event whose outgoing flow doesn't target the shared end node to be rejected")
	}
}

func TestValidateShape_BoundaryEvent_DuplicateOnSameTask_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "task", "")}},
		&engine.Node{ID: "task", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f2", "end", "")}},
		&engine.Node{ID: "boundary1", Type: engine.ErrorBoundaryEventType, AttachedToRef: "task", ErrorCode: "RISK_ERROR", Outgoing: []engine.Flow{flow("f3", "end", "")}},
		&engine.Node{ID: "boundary2", Type: engine.ErrorBoundaryEventType, AttachedToRef: "task", ErrorCode: "OTHER_ERROR", Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected two boundary events attached to the same task to be rejected")
	}
}

func TestValidateShape_Diamond_WithBoundaryEvent_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "taskB", `${branch == "b"}`),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "taskB", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f5", "end", "")}},
		&engine.Node{ID: "boundary", Type: engine.ErrorBoundaryEventType, AttachedToRef: "taskA", ErrorCode: "RISK_ERROR", Outgoing: []engine.Flow{flow("f6", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a diamond shape with any boundary event present to be rejected — diamond branches don't support boundary events")
	}
}

func TestValidateShape_TimerOnly_Passes(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "timer", "")}},
		&engine.Node{ID: "timer", Type: engine.IntermediateTimerEventType, Outgoing: []engine.Flow{flow("f2", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err != nil {
		t.Fatalf("expected a start -> timer -> end shape to pass, got: %v", err)
	}
}

func TestValidateShape_TimerMixedWithTask_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "timer", "")}},
		&engine.Node{ID: "timer", Type: engine.IntermediateTimerEventType, Outgoing: []engine.Flow{flow("f2", "task", "")}},
		&engine.Node{ID: "task", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f3", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a timer mixed with a service task to be rejected — timers are only supported in isolation this phase")
	}
}

func TestValidateShape_TwoTimers_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "timer1", "")}},
		&engine.Node{ID: "timer1", Type: engine.IntermediateTimerEventType, Outgoing: []engine.Flow{flow("f2", "timer2", "")}},
		&engine.Node{ID: "timer2", Type: engine.IntermediateTimerEventType, Outgoing: []engine.Flow{flow("f3", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a chain of two timers to be rejected — at most one timer is supported this phase")
	}
}

func TestValidateShape_Diamond_WithTimer_Rejected(t *testing.T) {
	graph := newGraph(
		&engine.Node{ID: "start", Type: engine.StartEventType, Outgoing: []engine.Flow{flow("f1", "gw", "")}},
		&engine.Node{ID: "gw", Type: engine.ExclusiveGatewayType, Outgoing: []engine.Flow{
			flow("f2", "taskA", `${branch == "a"}`),
			flow("f3", "taskB", `${branch == "b"}`),
		}},
		&engine.Node{ID: "taskA", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f4", "end", "")}},
		&engine.Node{ID: "taskB", Type: engine.ServiceTaskType, Outgoing: []engine.Flow{flow("f5", "end", "")}},
		&engine.Node{ID: "timer", Type: engine.IntermediateTimerEventType, Outgoing: []engine.Flow{flow("f6", "end", "")}},
		&engine.Node{ID: "end", Type: engine.EndEventType},
	)
	if err := ValidateShape(graph); err == nil {
		t.Fatal("expected a diamond shape with a timer node present to be rejected")
	}
}
