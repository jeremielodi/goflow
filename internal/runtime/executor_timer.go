package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// executeBoundaryTimerNode is called when a timer fires and execution reaches the BoundaryTimerEvent node.
// It simply advances to the node's first outgoing flow. Returns the next node ID.
func (r *Runtime) executeBoundaryTimerNode(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) (string, error) {
	if len(node.Outgoing) == 0 {
		return "", fmt.Errorf("boundary timer event %s has no outgoing flow", node.ID)
	}
	nextNodeID := node.Outgoing[0].TargetRef

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := engineRepo.UpdateExecutionNodeTx(tx, exec.ID, nextNodeID); err != nil {
		return "", fmt.Errorf("failed to update execution node: %w", err)
	}
	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "active"); err != nil {
		return "", fmt.Errorf("failed to update execution status: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(exec.ID, exec.ProcessInstanceID, node.ID, nextNodeID, nil))

	return nextNodeID, nil
}

// executeIntermediateTimer handles an intermediate catch event (timer). Creates a timer job
// and parks the execution in "waiting" state until the timer fires.
func (r *Runtime) executeIntermediateTimer(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node) error {
	if node.TimerDefinition == "" {
		return nil
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var totalCycles int = 1
	var repeatInterval string
	var timerDuration time.Duration

	if strings.Contains(node.TimerDefinition, "timeCycle") {
		cycleStr := extractCycleString(node.TimerDefinition)
		cycle, err := ParseTimerCycle(cycleStr)
		if err == nil {
			totalCycles = cycle.Remaining
			repeatInterval = formatDuration(cycle.Interval)
			timerDuration = cycle.Interval
		} else {
			log.Printf("⚠️ Failed to parse timer cycle: %v, using fallback", err)
			timerDuration = parseTimerDuration(node.TimerDefinition)
		}
	} else {
		timerDuration = parseTimerDuration(node.TimerDefinition)
	}

	dueAt := time.Now().Add(timerDuration)

	var nextNodeID string
	if len(node.Outgoing) > 0 {
		nextNodeID = node.Outgoing[0].TargetRef
	}

	payload := map[string]interface{}{
		"target_node_id":    nextNodeID,
		"event_type":        "intermediate",
		"is_cycle":          totalCycles != 1 || repeatInterval != "",
		"total_cycles":      totalCycles,
		"repeat_interval":   repeatInterval,
		"current_cycle":     0,
		"original_duration": timerDuration.String(),
	}
	payloadBytes, _ := json.Marshal(payload)

	timerID := uuid.New()
	if err := engineRepo.CreateTimerWithCycleTx(tx, timerID, exec.ProcessInstanceID, &exec.ID, dueAt, "intermediate", payloadBytes, 0, totalCycles, repeatInterval); err != nil {
		return fmt.Errorf("failed to create timer: %w", err)
	}

	if err := engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting"); err != nil {
		return fmt.Errorf("failed to update execution status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit intermediate timer transaction: %w", err)
	}

	service.LogAuditErr("timer created", r.auditService.LogTimerCreated(exec.ProcessInstanceID, &exec.ID, dueAt, "intermediate"))
	service.LogAuditErr("execution waited", r.auditService.LogExecutionWaited(exec.ID, exec.ProcessInstanceID, node.ID, "timer"))
	return nil
}
