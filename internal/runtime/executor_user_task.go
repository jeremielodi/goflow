package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

func (r *Runtime) executeUserTask(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph, variables map[string]interface{}) error {
	if node.MultiInstance != nil {
		return r.handleMultiInstance(ctx, exec, node, variables)
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ID, "active", node.ID); err != nil {
		return fmt.Errorf("failed to update execution: %w", err)
	}

	// Schedule boundary timer if one is attached to this task
	for _, n := range graph.Nodes {
		if n.Type == engine.BoundaryTimerEventType && n.AttachedToRef == node.ID {
			if err := r.scheduleBoundaryTimerTx(tx, engineRepo, exec, n); err != nil {
				return err
			}
			break
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
	}); err != nil {
		return fmt.Errorf("failed to create user task: %w", err)
	}

	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting"); err != nil {
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
		if _, err := taskRepo.UpdateTaskFormData(taskID, inputVars); err != nil {
			log.Printf("Warning: failed to store input-mapped variables on task %s: %v", taskID, err)
		}
	}

	r.auditService.LogTaskCreated(taskID, exec.ProcessInstanceID, node.Name, assignee, candidateGroup)

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

func (r *Runtime) scheduleBoundaryTimerTx(tx *sqlx.Tx, engineRepo *repository.EngineRepository, exec *repository.Execution, attachedTimer *engine.Node) error {
	if attachedTimer.TimerDefinition == "" {
		return nil
	}

	var totalCycles int = 1
	var repeatInterval string
	var timerDuration time.Duration

	if strings.Contains(attachedTimer.TimerDefinition, "timeCycle") {
		cycleStr := extractCycleString(attachedTimer.TimerDefinition)
		cycle, err := ParseTimerCycle(cycleStr)
		if err == nil {
			totalCycles = cycle.Remaining
			repeatInterval = formatDuration(cycle.Interval)
			timerDuration = cycle.Interval
		} else {
			log.Printf("⚠️ Failed to parse timer cycle: %v, using fallback", err)
			timerDuration = parseTimerDuration(attachedTimer.TimerDefinition)
		}
	} else {
		timerDuration = parseTimerDuration(attachedTimer.TimerDefinition)
	}

	dueAt := time.Now().Add(timerDuration)

	var nextNodeID string
	if len(attachedTimer.Outgoing) > 0 {
		nextNodeID = attachedTimer.Outgoing[0].TargetRef
	}

	payload := map[string]interface{}{
		"target_node_id":    nextNodeID,
		"event_type":        "boundary",
		"is_cycle":          totalCycles != 1 || repeatInterval != "",
		"total_cycles":      totalCycles,
		"repeat_interval":   repeatInterval,
		"current_cycle":     0,
		"original_duration": timerDuration.String(),
	}
	payloadBytes, _ := json.Marshal(payload)

	timerID := uuid.New()
	if err := engineRepo.CreateTimerWithCycleTx(tx, timerID, exec.ProcessInstanceID, &exec.ID, dueAt, "boundary", payloadBytes, 0, totalCycles, repeatInterval); err != nil {
		return fmt.Errorf("failed to create boundary timer: %w", err)
	}

	log.Printf("⏰ [CREATE TIMER] Task: %s, Duration: %v, DueAt: %s", attachedTimer.AttachedToRef, timerDuration, dueAt.Format("15:04:05.000"))
	r.auditService.LogTimerCreated(exec.ProcessInstanceID, &exec.ID, dueAt, "boundary")
	return nil
}
