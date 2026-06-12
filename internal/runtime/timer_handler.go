package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type TimerHandler struct {
	db         *sqlx.DB
	engineRepo *repository.EngineRepository
}

func NewTimerHandler(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *TimerHandler {
	return &TimerHandler{
		db:         db,
		engineRepo: repository.NewEngineRepository(db, dispatcher),
	}
}

// HandleIntermediateTimerEvent handles an intermediate timer catch event
func (t *TimerHandler) HandleIntermediateTimerEvent(
	ctx context.Context,
	execID uuid.UUID,
	timerNode *engine.Node,
	variables map[string]interface{},
) error {
	// Parse timer definition from node
	timerDef, err := engine.ParseTimerDefinition(timerNode.TimerDefinition)
	if err != nil {
		return fmt.Errorf("failed to parse timer definition: %w", err)
	}

	// Calculate when the timer should fire
	dueAt, err := engine.CalculateDueTime(timerDef, time.Now())
	if err != nil {
		return fmt.Errorf("failed to calculate due time: %w", err)
	}

	// Get current execution
	exec, err := t.engineRepo.GetActiveExecution(ctx, execID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}

	// Prepare timer payload with next node info
	timerPayload := map[string]interface{}{
		"nextNodeId":  t.getNextNodeAfterTimer(timerNode),
		"timerNodeId": timerNode.ID,
		"dueAt":       dueAt,
	}
	payloadJSON, _ := json.Marshal(timerPayload)

	tx, err := t.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create timer job
	timerJob := &models.TimerJob{
		ID:                uuid.New(),
		ProcessInstanceID: exec.ProcessInstanceID,
		ExecutionID:       &exec.ID,
		EventType:         string(models.TimerIntermediate),
		DueAt:             dueAt,
		Payload:           payloadJSON,
	}

	err = t.engineRepo.CreateTimerJobTx(tx, timerJob)
	if err != nil {
		return fmt.Errorf("failed to create timer job: %w", err)
	}

	// Update execution to waiting state
	err = t.engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting")
	if err != nil {
		return fmt.Errorf("failed to update execution status: %w", err)
	}

	log.Printf("Timer created for execution %s, due at %s", execID, dueAt)

	return tx.Commit()
}

// HandleBoundaryTimerEvent handles a boundary timer event attached to an activity
func (t *TimerHandler) HandleBoundaryTimerEvent(
	ctx context.Context,
	attachedToExecID uuid.UUID,
	timerNode *engine.Node,
	variables map[string]interface{},
) error {
	// Parse timer definition
	timerDef, err := engine.ParseTimerDefinition(timerNode.TimerDefinition)
	if err != nil {
		return fmt.Errorf("failed to parse timer definition: %w", err)
	}

	// Calculate due time
	dueAt, err := engine.CalculateDueTime(timerDef, time.Now())
	if err != nil {
		return fmt.Errorf("failed to calculate due time: %w", err)
	}

	// Get the execution this timer is attached to
	exec, err := t.engineRepo.GetActiveExecution(ctx, attachedToExecID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}

	// Prepare timer payload
	timerPayload := map[string]interface{}{
		"attachedToNodeId": timerNode.AttachedToRef,
		"boundaryEventId":  timerNode.ID,
		"isInterrupting":   timerNode.CancelActivity,
	}
	payloadJSON, _ := json.Marshal(timerPayload)

	tx, err := t.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Create boundary timer job
	timerJob := &models.TimerJob{
		ID:                uuid.New(),
		ProcessInstanceID: exec.ProcessInstanceID,
		ExecutionID:       &exec.ID,
		EventType:         string(models.TimerBoundary),
		DueAt:             dueAt,
		Payload:           payloadJSON,
	}

	err = t.engineRepo.CreateTimerJobTx(tx, timerJob)
	if err != nil {
		return fmt.Errorf("failed to create boundary timer: %w", err)
	}

	return tx.Commit()
}

// HandleTimerStartEvent handles a timer start event for a process definition
func (t *TimerHandler) HandleTimerStartEvent(
	ctx context.Context,
	processDefinitionID uuid.UUID,
	timerNode *engine.Node,
) error {
	// Parse timer definition for repeating schedule
	timerDef, err := engine.ParseTimerDefinition(timerNode.TimerDefinition)
	if err != nil {
		return fmt.Errorf("failed to parse timer definition: %w", err)
	}

	// For start events, we typically use timeCycle or timeDate
	if timerDef.TimeCycle != "" || timerDef.Cycle != "" {
		// Schedule recurring process starts
		return t.scheduleRecurringStart(ctx, processDefinitionID, timerNode, timerDef)
	}

	if timerDef.TimeDate != "" {
		// Schedule one-time start
		startTime, err := engine.CalculateDueTime(timerDef, time.Now())
		if err != nil {
			return err
		}

		// Create timer job for the start
		timerPayload := map[string]interface{}{
			"processDefinitionId": processDefinitionID,
			"startEventId":        timerNode.ID,
		}
		payloadJSON, _ := json.Marshal(timerPayload)

		timerJob := &models.TimerJob{
			ID:                uuid.New(),
			ProcessInstanceID: uuid.Nil, // No process instance yet
			ExecutionID:       nil,
			EventType:         string(models.TimerStart),
			DueAt:             startTime,
			Payload:           payloadJSON,
		}

		return t.engineRepo.CreateTimerJob(timerJob)
	}

	return nil
}

// scheduleRecurringStart schedules recurring process starts
func (t *TimerHandler) scheduleRecurringStart(
	ctx context.Context,
	processDefinitionID uuid.UUID,
	timerNode *engine.Node,
	timerDef *models.TimerDefinition,
) error {
	// Calculate next run time
	nextRun, err := engine.CalculateDueTime(timerDef, time.Now())
	if err != nil {
		return err
	}

	timerPayload := map[string]interface{}{
		"processDefinitionId": processDefinitionID,
		"startEventId":        timerNode.ID,
		"cycle":               timerDef.Cycle,
	}
	payloadJSON, _ := json.Marshal(timerPayload)

	timerJob := &models.TimerJob{
		ID:                uuid.New(),
		ProcessInstanceID: uuid.Nil,
		ExecutionID:       nil,
		EventType:         string(models.TimerStart),
		DueAt:             nextRun,
		Payload:           payloadJSON,
	}

	return t.engineRepo.CreateTimerJob(timerJob)
}

func (t *TimerHandler) getNextNodeAfterTimer(timerNode *engine.Node) string {
	if len(timerNode.Outgoing) > 0 {
		return timerNode.Outgoing[0].TargetRef
	}
	return ""
}
