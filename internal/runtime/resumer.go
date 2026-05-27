// internal/runtime/resumer.go
package runtime

import (
	"context"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// ResumeExecution resumes an execution from a given ID
func ResumeExecution(db *sqlx.DB, execID uuid.UUID) error {
	engineRepo := repository.NewEngineRepository(db)

	// Get execution
	exec, err := engineRepo.GetExecutionByID(execID)
	if err != nil {
		return err
	}

	// Get process graph
	graph, err := engineRepo.GetProcessGraphByInstanceID(exec.ProcessInstanceID)
	if err != nil {
		return err
	}

	// Create runtime and execute
	rt := NewRuntime(graph, db)
	return rt.ExecuteExecution(context.Background(), execID)
}

// CreateResumer returns a function that can resume executions
func CreateResumer(db *sqlx.DB) func(ctx context.Context, execID uuid.UUID) error {
	return func(ctx context.Context, execID uuid.UUID) error {
		return ResumeExecution(db, execID)
	}
}
