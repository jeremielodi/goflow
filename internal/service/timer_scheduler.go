// service/timer_scheduler.go
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type ExecutionResumer func(ctx context.Context, execID uuid.UUID) error

type TimerScheduler struct {
	db         *sqlx.DB
	engineRepo *repository.EngineRepository
	stopChan   chan bool
	resumer    ExecutionResumer
}

func NewTimerScheduler(db *sqlx.DB, resumer ExecutionResumer) *TimerScheduler {
	return &TimerScheduler{
		db:         db,
		engineRepo: repository.NewEngineRepository(db),
		stopChan:   make(chan bool),
		resumer:    resumer,
	}
}

// Start begins the timer scheduler poller
func (s *TimerScheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
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

// processDueTimers processes all timers that are due
func (s *TimerScheduler) processDueTimers(ctx context.Context) {
	timers, err := s.engineRepo.GetDueTimers(ctx, time.Now())
	if err != nil {
		log.Printf("Error fetching due timers: %v", err)
		return
	}

	for _, timer := range timers {
		s.triggerTimer(ctx, timer)
	}
}

// triggerTimer triggers a single timer
func (s *TimerScheduler) triggerTimer(ctx context.Context, timer models.TimerJob) {

	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction for timer %s: %v", timer.ID, err)
		return
	}
	defer tx.Rollback()

	// Mark timer as triggered
	err = s.engineRepo.MarkTimerTriggeredTx(tx, timer.ID)
	if err != nil {
		log.Printf("Failed to mark timer %s as triggered: %v", timer.ID, err)
		return
	}

	var execID uuid.UUID
	if timer.ExecutionID != nil {
		execID = *timer.ExecutionID
		// Resume the waiting execution
		err = s.resumeExecution(tx, execID, timer)
		if err != nil {
			log.Printf("Failed to resume execution for timer %s: %v", timer.ID, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction for timer %s: %v", timer.ID, err)
		return
	}

	// ✅ Continue execution after transaction is committed
	if execID != uuid.Nil && s.resumer != nil {
		// Small delay for transaction to fully commit
		time.Sleep(100 * time.Millisecond)
		if err := s.resumer(ctx, execID); err != nil {
			log.Printf("❌ Failed to continue execution after timer: %v", err)
		}
	}
}

// resumeExecution resumes an execution that was waiting for a timer
func (s *TimerScheduler) resumeExecution(tx *sqlx.Tx, execID uuid.UUID, timer models.TimerJob) error {
	// Parse timer payload to get the next node
	var timerPayload struct {
		NextNodeID string `json:"nextNodeId"`
	}
	if err := json.Unmarshal(timer.Payload, &timerPayload); err != nil {
		return fmt.Errorf("failed to parse timer payload: %w", err)
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

	// Update execution to move to the next node
	err = s.engineRepo.UpdateExecutionStatusAndNodeTx(tx, execID, "active", timerPayload.NextNodeID)
	if err != nil {
		return fmt.Errorf("failed to update execution: %w", err)
	}

	// log.Printf("Execution %s resumed after timer, moving to node %s (user task cancelled)", execID, timerPayload.NextNodeID)

	return nil
}
