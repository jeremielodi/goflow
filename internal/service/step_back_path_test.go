package service

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/models"
)

func stepBackGraph() *engine.ProcessGraph {
	nodes := map[string]*engine.Node{
		"start":  {ID: "start", Type: engine.StartEventType},
		"a":      {ID: "a", Type: engine.UserTaskType},
		"gw":     {ID: "gw", Type: engine.ExclusiveGatewayType},
		"script": {ID: "script", Type: engine.ScriptTaskType},
		"b":      {ID: "b", Type: engine.ServiceTaskType},
		"c":      {ID: "c", Type: engine.UserTaskType},
		"fork":   {ID: "fork", Type: engine.ParallelGatewayType},
		"sub":    {ID: "sub", Type: engine.SubProcessType},
		"inner":  {ID: "inner", Type: engine.UserTaskType, ParentScope: "sub"},
		"mi":     {ID: "mi", Type: engine.UserTaskType, MultiInstance: &engine.MultiInstanceConfig{}},
	}
	return &engine.ProcessGraph{Nodes: nodes}
}

type hop struct {
	exec     uuid.UUID
	from, to string
	stepBack bool
}

func hopLogs(t *testing.T, hops ...hop) []models.AuditLog {
	now := time.Now()
	logs := make([]models.AuditLog, 0, len(hops))
	for i, h := range hops {
		data := map[string]interface{}{"executionId": h.exec.String(), "fromNode": h.from, "toNode": h.to}
		if h.stepBack {
			data["stepBack"] = true
		}
		logs = append(logs, models.AuditLog{
			Action:    models.ActionExecutionMoved,
			CreatedAt: now.Add(time.Duration(i) * time.Millisecond),
			NewData:   marshalOrPanic(t, data),
		})
	}
	return logs
}

func TestStepBackPath_KeepsOnlyWaitStates(t *testing.T) {
	e := uuid.New()
	logs := hopLogs(t,
		hop{e, "start", "a", false},
		hop{e, "a", "gw", false},
		hop{e, "gw", "script", false},
		hop{e, "script", "b", false},
		hop{e, "b", "c", false},
	)
	got := StepBackPath(logs, stepBackGraph(), e, "")
	if want := []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("path = %v, want %v", got, want)
	}
}

func TestStepBackPath_IgnoresOtherExecutions(t *testing.T) {
	e, other := uuid.New(), uuid.New()
	logs := hopLogs(t,
		hop{e, "start", "a", false},
		hop{other, "x", "b", false},
		hop{e, "a", "c", false},
	)
	got := StepBackPath(logs, stepBackGraph(), e, "")
	if want := []string{"a", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("path = %v, want %v", got, want)
	}
}

// Pressing "previous" twice from c must reach a, not bounce back to c.
func TestStepBackPath_EarlierStepBackPops(t *testing.T) {
	e := uuid.New()
	logs := hopLogs(t,
		hop{e, "start", "a", false},
		hop{e, "a", "b", false},
		hop{e, "b", "c", false},
		hop{e, "c", "b", true},
	)
	got := StepBackPath(logs, stepBackGraph(), e, "")
	if want := []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("path = %v, want %v", got, want)
	}
}

func TestStepBackPath_ParallelGatewayIsABarrier(t *testing.T) {
	e := uuid.New()
	logs := hopLogs(t,
		hop{e, "start", "a", false},
		hop{e, "a", "fork", false},
		hop{e, "fork", "b", false},
		hop{e, "b", "c", false},
	)
	got := StepBackPath(logs, stepBackGraph(), e, "")
	if want := []string{"b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("path = %v, want %v", got, want)
	}
}

// A parallel branch is created right at its first task, with no entry for
// arriving there: its first move starts from it.
func TestStepBackPath_BranchCreatedAtItsFirstTask(t *testing.T) {
	e := uuid.New()
	logs := hopLogs(t,
		hop{e, "b", "gw", false},
		hop{e, "gw", "c", false},
	)
	got := StepBackPath(logs, stepBackGraph(), e, "")
	if want := []string{"b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("path = %v, want %v", got, want)
	}
}

func TestStepBackPath_StaysInScopeAndSkipsMultiInstance(t *testing.T) {
	e := uuid.New()
	logs := hopLogs(t,
		hop{e, "start", "a", false},
		hop{e, "a", "inner", false},
		hop{e, "inner", "mi", false},
		hop{e, "mi", "sub", false},
		hop{e, "sub", "c", false},
	)
	got := StepBackPath(logs, stepBackGraph(), e, "")
	if want := []string{"a", "sub", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("path = %v, want %v", got, want)
	}
}

func TestStepBackPath_CollapsesRepeatedElement(t *testing.T) {
	e := uuid.New()
	logs := hopLogs(t,
		hop{e, "start", "a", false},
		hop{e, "a", "a", false},
		hop{e, "a", "b", false},
	)
	got := StepBackPath(logs, stepBackGraph(), e, "")
	if want := []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("path = %v, want %v", got, want)
	}
}
