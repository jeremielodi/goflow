package service

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
)

func marshalOrPanic(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestBuildTokenHistory_ExecutionMoved(t *testing.T) {
	now := time.Now()
	logs := []models.AuditLog{
		{
			Action:    models.ActionExecutionMoved,
			CreatedAt: now,
			NewData:   marshalOrPanic(t, map[string]interface{}{"fromNode": "taskA", "toNode": "taskB"}),
		},
	}

	steps := BuildTokenHistory(logs, nil)

	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	if steps[0].ElementId != "taskB" {
		t.Errorf("expected elementId 'taskB', got %q", steps[0].ElementId)
	}
	if steps[0].Detail != "taskA → taskB" {
		t.Errorf("expected detail 'taskA → taskB', got %q", steps[0].Detail)
	}
}

func TestBuildTokenHistory_TaskClaimedResolvesElementFromTask(t *testing.T) {
	taskID := uuid.New()
	logs := []models.AuditLog{
		{
			Action:    models.ActionTaskClaimed,
			CreatedAt: time.Now(),
			TaskID:    &taskID,
			NewData:   marshalOrPanic(t, map[string]interface{}{"claimedBy": "alice"}),
		},
	}
	tasks := []models.Task{
		{ID: taskID, TaskDefinitionKey: "approveTask", TaskName: "Approve"},
	}

	steps := BuildTokenHistory(logs, tasks)

	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	if steps[0].ElementId != "approveTask" {
		t.Errorf("expected elementId 'approveTask', got %q", steps[0].ElementId)
	}
	if steps[0].ElementName != "Approve" {
		t.Errorf("expected elementName 'Approve', got %q", steps[0].ElementName)
	}
	if steps[0].Detail != "Claimed by alice" {
		t.Errorf("expected detail 'Claimed by alice', got %q", steps[0].Detail)
	}
}

// Task-related audit entries never carried an executionId in their own
// newData payload; BuildTokenHistory must resolve it from the task's own
// ExecutionID field so the frontend can group replay steps by token
// (multi-token / parallel-branch replay).
func TestBuildTokenHistory_TaskStepResolvesExecutionIdFromTask(t *testing.T) {
	taskID := uuid.New()
	execID := uuid.New()
	logs := []models.AuditLog{
		{
			Action:    models.ActionTaskCreated,
			CreatedAt: time.Now(),
			TaskID:    &taskID,
			NewData:   marshalOrPanic(t, map[string]interface{}{"taskName": "Approve"}),
		},
	}
	tasks := []models.Task{
		{ID: taskID, ExecutionID: &execID, TaskDefinitionKey: "approveTask", TaskName: "Approve"},
	}

	steps := BuildTokenHistory(logs, tasks)

	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	if steps[0].ExecutionId == nil || *steps[0].ExecutionId != execID {
		t.Errorf("expected executionId %s, got %v", execID, steps[0].ExecutionId)
	}
}

func TestBuildTokenHistory_ProcessStartedHasNoElementId(t *testing.T) {
	logs := []models.AuditLog{
		{
			Action:    models.ActionProcessStarted,
			CreatedAt: time.Now(),
			NewData:   marshalOrPanic(t, map[string]interface{}{"variables": map[string]interface{}{}}),
		},
	}

	steps := BuildTokenHistory(logs, nil)

	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	if steps[0].ElementId != "" {
		t.Errorf("expected empty elementId for PROCESS_STARTED, got %q", steps[0].ElementId)
	}
	if steps[0].Detail != "Process instance started" {
		t.Errorf("unexpected detail: %q", steps[0].Detail)
	}
}

// A BPMN loop that revisits the same element must produce two distinct
// steps, not one collapsed entry — this is exactly what the raw SQL
// DISTINCT-based approach would have gotten wrong.
func TestBuildTokenHistory_LoopProducesTwoDistinctSteps(t *testing.T) {
	t1 := time.Now()
	t2 := t1.Add(time.Second)
	logs := []models.AuditLog{
		{
			Action:    models.ActionExecutionMoved,
			CreatedAt: t1,
			NewData:   marshalOrPanic(t, map[string]interface{}{"fromNode": "reviewTask", "toNode": "gatewayX"}),
		},
		{
			Action:    models.ActionExecutionMoved,
			CreatedAt: t2,
			NewData:   marshalOrPanic(t, map[string]interface{}{"fromNode": "gatewayX", "toNode": "reviewTask"}),
		},
	}

	steps := BuildTokenHistory(logs, nil)

	if len(steps) != 2 {
		t.Fatalf("expected 2 distinct steps for a loop, got %d", len(steps))
	}
	if steps[0].ElementId != "gatewayX" || steps[1].ElementId != "reviewTask" {
		t.Errorf("unexpected element sequence: %q, %q", steps[0].ElementId, steps[1].ElementId)
	}
}
