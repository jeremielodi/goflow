// service/timer_scheduler.go
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type TimerScheduler struct {
	db         *sqlx.DB
	engineRepo *repository.EngineRepository
	stopChan   chan bool
	resumer    func(ctx context.Context, execID uuid.UUID) error
}

func NewTimerScheduler(db *sqlx.DB, resumer func(ctx context.Context, execID uuid.UUID) error, dispatcher *events.TaskEventDispatcher) *TimerScheduler {
	return &TimerScheduler{
		db:         db,
		engineRepo: repository.NewEngineRepository(db, dispatcher),
		stopChan:   make(chan bool),
		resumer:    resumer,
	}
}

// Start begins the timer scheduler poller
func (s *TimerScheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second) // Check every 2 seconds
	defer ticker.Stop()

	log.Println("Timer scheduler started")

	for {
		select {
		case <-ticker.C:
			s.processDueTimers(ctx)
		case <-s.stopChan:
			log.Println("Timer scheduler stopped")
			return
		case <-ctx.Done():
			log.Println("Timer scheduler context cancelled")
			return
		}
	}
}

// Stop stops the timer scheduler
func (s *TimerScheduler) Stop() {
	s.stopChan <- true
}

// processDueTimers atomically claims and fires all due timers.
func (s *TimerScheduler) processDueTimers(ctx context.Context) {
	timers, err := s.engineRepo.ClaimDueTimers(ctx, 20)
	if err != nil {
		log.Printf("Error claiming due timers: %v", err)
		return
	}

	for _, timer := range timers {
		log.Printf("🔥 Processing timer %s (due_at: %s)", timer.ID, timer.DueAt)
		s.triggerTimer(ctx, timer)
	}
}

// triggerTimer triggers a single timer
func (s *TimerScheduler) triggerTimer(ctx context.Context, timer models.TimerJob) {
	log.Printf("Triggering timer %s for process instance %s", timer.ID, timer.ProcessInstanceID)

	// Parse payload
	var payload map[string]interface{}
	if len(timer.Payload) > 0 {
		json.Unmarshal(timer.Payload, &payload)
	}

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction for timer %s: %v", timer.ID, err)
		return
	}
	defer tx.Rollback()

	var execID uuid.UUID
	if timer.ExecutionID != nil {
		execID = *timer.ExecutionID
		err = s.resumeExecution(tx, execID, timer)
		if err != nil {
			log.Printf("Failed to resume execution for timer %s: %v", timer.ID, err)
			return
		}
	}

	// Handle timer cycles
	isCycle, _ := payload["is_cycle"].(bool)
	if isCycle {
		totalCycles, _ := payload["total_cycles"].(float64)
		repeatInterval, _ := payload["repeat_interval"].(string)
		currentCycle, _ := payload["current_cycle"].(float64)

		nextCycle := int(currentCycle) + 1
		totalCyclesInt := int(totalCycles)

		// Convert PT10S to 10s for time.ParseDuration
		intervalStr := repeatInterval
		if strings.HasPrefix(intervalStr, "PT") {
			intervalStr = strings.TrimPrefix(intervalStr, "PT")
			intervalStr = strings.ToLower(intervalStr)
			// Replace H with h, M with m, S with s
			intervalStr = strings.ReplaceAll(intervalStr, "H", "h")
			intervalStr = strings.ReplaceAll(intervalStr, "M", "m")
			intervalStr = strings.ReplaceAll(intervalStr, "S", "s")
		}

		interval, err := time.ParseDuration(intervalStr)
		if err != nil {
			log.Printf("Failed to parse repeat interval: %v, using default 10s", err)
			interval = 10 * time.Second
		}

		// Check if we have more cycles (totalCycles = -1 means infinite)
		if totalCyclesInt == -1 || nextCycle < totalCyclesInt {
			nextDueAt := time.Now().UTC().Add(interval)

			payload["current_cycle"] = nextCycle
			newPayload, _ := json.Marshal(payload)

			nextTimerID := uuid.New()
			err = s.engineRepo.CreateTimerWithDurationSeconds(tx, nextTimerID, timer.ProcessInstanceID, timer.ExecutionID, int(interval.Seconds()), timer.EventType, newPayload, nextCycle, totalCyclesInt, repeatInterval)
			if err != nil {
				log.Printf("Failed to schedule next cycle: %v", err)
			} else {
				log.Printf("🔄 Scheduled cycle %d/%d for timer %s at %s",
					nextCycle, totalCyclesInt, timer.ID, nextDueAt)
			}
		} else {
			log.Printf("✅ Timer cycle completed for timer %s (final cycle)", timer.ID)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction for timer %s: %v", timer.ID, err)
		return
	}

	// Continue execution after transaction is committed
	if execID != uuid.Nil && s.resumer != nil {
		time.Sleep(100 * time.Millisecond)
		log.Printf("🔄 Continuing execution %s after timer", execID)
		if err := s.resumer(ctx, execID); err != nil {
			log.Printf("❌ Failed to continue execution after timer: %v", err)
		}
	}
}

// resumeExecution resumes an execution that was waiting for a timer
func (s *TimerScheduler) resumeExecution(tx *sqlx.Tx, execID uuid.UUID, timer models.TimerJob) error {
	// Parse payload to get the target node
	var payload map[string]interface{}
	if len(timer.Payload) > 0 {
		if err := json.Unmarshal(timer.Payload, &payload); err != nil {
			return fmt.Errorf("failed to parse timer payload: %w", err)
		}
	}

	targetNodeID, ok := payload["target_node_id"].(string)
	if !ok {
		return fmt.Errorf("target_node_id not found in timer payload")
	}

	// Cancel the user task
	_, err := tx.Exec(`
		UPDATE public.tasks 
		SET status = 'cancelled', updated_at = NOW()
		WHERE execution_id = $1 AND status = 'created'
	`, execID)
	if err != nil {
		return fmt.Errorf("failed to cancel user task: %w", err)
	}

	// Update execution to move to the target node
	err = s.engineRepo.UpdateExecutionStatusAndNodeTx(tx, execID, "active", targetNodeID)
	if err != nil {
		return fmt.Errorf("failed to update execution: %w", err)
	}

	log.Printf("Execution %s resumed after timer, moving to node %s (user task cancelled)", execID, targetNodeID)

	return nil
}
