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

type TimerScheduler struct {
	db         *sqlx.DB
	engineRepo *repository.EngineRepository
	stopChan   chan bool
}

func NewTimerScheduler(db *sqlx.DB) *TimerScheduler {
	return &TimerScheduler{
		db:         db,
		engineRepo: repository.NewEngineRepository(db),
		stopChan:   make(chan bool),
	}
}

// Start begins the timer scheduler poller
func (s *TimerScheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second) // Check every second
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
	timers, err := s.engineRepo.GetDueTimers(ctx)
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
	log.Printf("Triggering timer %s for process instance %s", timer.ID, timer.ProcessInstanceID)

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

	// Handle the timer based on event type
	if timer.ExecutionID != nil {
		// Resume the waiting execution
		err = s.resumeExecution(tx, *timer.ExecutionID, timer)
		if err != nil {
			log.Printf("Failed to resume execution for timer %s: %v", timer.ID, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction for timer %s: %v", timer.ID, err)
	}
}

// resumeExecution resumes an execution that was waiting for a timer
func (s *TimerScheduler) resumeExecution(tx *sqlx.Tx, execID uuid.UUID, timer models.TimerJob) error {
	// Get the execution
	_, err := s.engineRepo.GetActiveExecutionWithTx(tx, execID)
	if err != nil {
		return fmt.Errorf("failed to get execution: %w", err)
	}

	// Parse timer payload to get the next node
	var timerPayload struct {
		NextNodeID string `json:"nextNodeId"`
	}
	if err := json.Unmarshal(timer.Payload, &timerPayload); err != nil {
		return fmt.Errorf("failed to parse timer payload: %w", err)
	}

	// Update execution to move past the timer
	err = s.engineRepo.UpdateExecutionStatusAndNodeTx(tx, execID, "active", timerPayload.NextNodeID)
	if err != nil {
		return fmt.Errorf("failed to update execution: %w", err)
	}

	log.Printf("Execution %s resumed after timer, moving to node %s", execID, timerPayload.NextNodeID)

	return nil
}
