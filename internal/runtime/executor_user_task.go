package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

func (r *Runtime) executeUserTask(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph, variables map[string]interface{}) error {
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ProcessInstanceID, exec.ID, "active", node.ID); err != nil {
		return fmt.Errorf("failed to update execution: %w", err)
	}

	if err := r.armBoundaryEventsTx(tx, engineRepo, graph, exec, node); err != nil {
		return err
	}
	// Input mappings are the activity's local variables (also kept as the
	// task's form data below).
	if inputs := common.EvaluateIOMappings(node.InputMappings, variables); len(inputs) > 0 {
		if err := repository.SetLocalVariablesTx(tx, exec.ProcessInstanceID, exec.ID, inputs); err != nil {
			return fmt.Errorf("user task %s: input mappings: %w", node.ID, err)
		}
		// The assignment expressions below see them too (the execution
		// stops at this task, so nothing else reads this map).
		for k, v := range inputs {
			variables[k] = v
		}
	}

	var assignee *string
	if node.AssigneeExpr != nil {
		if resolved := resolveExpression(*node.AssigneeExpr, variables); resolved != "" {
			assignee = &resolved
		}
	}

	var candidateGroup *string
	if node.CandidateGroupExpr != nil {
		if resolved := resolveExpression(*node.CandidateGroupExpr, variables); resolved != "" {
			candidateGroup = &resolved
		}
	}

	priority := 50
	if node.PriorityExpr != nil {
		if resolved, err := common.EvaluateValue(*node.PriorityExpr, variables); err == nil && resolved != nil {
			switch v := resolved.(type) {
			case int:
				priority = v
			case int64:
				priority = int(v)
			case float64:
				priority = int(v)
			case string:
				if n, err := strconv.Atoi(v); err == nil {
					priority = n
				}
			}
		}
	}

	taskID := uuid.New()
	taskName := node.Name
	taskDefinitionId := node.ID
	if err := engineRepo.CreateUserTaskTx(tx, &repository.UserTask{
		ID:                taskID,
		ProcessInstanceID: exec.ProcessInstanceID,
		ExecutionID:       exec.ID,
		TaskDefinitionKey: node.ID,
		TaskName:          &taskName,
		Assignee:          assignee,
		CandidateGroup:    candidateGroup,
		Priority:          priority,
	}); err != nil {
		return fmt.Errorf("failed to create user task: %w", err)
	}

	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ProcessInstanceID, exec.ID, "waiting"); err != nil {
		return fmt.Errorf("failed to update execution status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// zeebe:ioMapping inputs are local to the activity (not the process scope),
	// so they're stored as the task's form data rather than merged into
	// process variables.
	if inputVars := common.EvaluateIOMappings(node.InputMappings, variables); len(inputVars) > 0 {
		taskRepo := repository.NewTaskRepository(r.DB, r.dispatcher)
		if _, err := taskRepo.UpdateTaskFormData(exec.ProcessInstanceID, taskID, inputVars); err != nil {
			log.Printf("Warning: failed to store input-mapped variables on task %s: %v", taskID, err)
		}
	}

	r.auditService.LogTaskCreated(taskID, exec.ProcessInstanceID, node.Name, assignee, candidateGroup)

	if p := eventlog.Global(); p != nil {
		var assigneeStr string
		if assignee != nil {
			assigneeStr = *assignee
		}
		p.TaskCreated(exec.ProcessInstanceID.String(), taskID.String(), exec.ID.String(), taskDefinitionId, assigneeStr)
	}

	if r.dispatcher != nil {
		processKey := ""
		if procDef, err := engineRepo.GetProcessDefinitionByInstanceID(exec.ProcessInstanceID); err == nil && procDef != nil {
			processKey = procDef.ProcessKey
		}
		event := &events.TaskEvent{
			ID:                uuid.New().String(),
			EventType:         events.TaskCreated,
			TaskID:            taskID.String(),
			ProcessInstanceID: exec.ProcessInstanceID.String(),
			ExecutionID:       exec.ID.String(),
			TaskName:          taskName,
			TaskDefinitionKey: taskDefinitionId,
			Assignee:          assignee,
			CandidateGroup:    candidateGroup,
			NewStatus:         "created",
			Timestamp:         time.Now(),
			ProcessKey:        processKey,
		}
		go r.dispatcher.Dispatch(context.Background(), event)
	}

	return nil
}

// createTimerTx creates a timer job on exec from a raw BPMN timer
// definition; when it fires, exec moves to targetNodeID. Returns when it
// first falls due.
func (r *Runtime) createTimerTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, exec *repository.Execution, timerDefinition, targetNodeID, eventType string) (time.Time, error) {
	t, err := r.resolveTimer(tx, timerDefinition, exec.ProcessInstanceID, exec.ID)
	if err != nil {
		return time.Time{}, fmt.Errorf("timer on %s: %w", targetNodeID, err)
	}
	dueAt, ok := t.First(time.Now())
	if !ok {
		// A cycle with no repetitions left (R0, or past its end): never fires.
		return time.Time{}, nil
	}

	// timer_kind/timer_value let the scheduler compute a cycle's next
	// occurrences (service.TimerScheduler.scheduleNextCycleIfNeeded);
	// timer_value is the FEEL-resolved value, so a cycle keeps the value it
	// had when armed.
	payload := map[string]interface{}{
		"target_node_id": targetNodeID,
		"event_type":     eventType,
		"is_cycle":       t.IsCycle(),
		"total_cycles":   t.Repetitions,
		"current_cycle":  0,
		"timer_kind":     string(t.Kind),
		"timer_value":    t.Raw,
	}
	payloadBytes, _ := json.Marshal(payload)

	timerID := uuid.New()
	if err := engineRepo.CreateTimerWithCycleTx(tx, timerID, exec.ProcessInstanceID, &exec.ID, dueAt.In(time.Local), eventType, payloadBytes, 0, t.Repetitions, ""); err != nil {
		return time.Time{}, err
	}

	r.auditService.LogTimerCreated(exec.ProcessInstanceID, &exec.ID, dueAt, eventType)
	return dueAt, nil
}
