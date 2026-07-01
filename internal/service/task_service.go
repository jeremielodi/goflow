// service/task_service.go
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

type TaskService struct {
	db             *sqlx.DB
	taskRepo       *repository.TaskRepository
	procRepo       *repository.ProcessRepository
	instRepo       *repository.ProcessInstanceRepository
	resumeExecutor ResumeExecutionFunc // Callback for resuming execution
	dispatcher     *events.TaskEventDispatcher
}

// ResumeExecutionFunc is a function type for resuming execution
type ResumeExecutionFunc func(ctx context.Context, graph *engine.ProcessGraph, execID uuid.UUID) error

// NewTaskService creates a new TaskService
func NewTaskService(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *TaskService {
	return &TaskService{
		db:         db,
		taskRepo:   repository.NewTaskRepository(db, dispatcher),
		procRepo:   repository.NewProcessRepository(db),
		instRepo:   repository.NewProcessInstanceRepository(db),
		dispatcher: dispatcher,
	}
}

// SetResumeExecutor sets the callback function for resuming execution
func (s *TaskService) SetResumeExecutor(fn ResumeExecutionFunc) {
	s.resumeExecutor = fn
}

// CompleteTask handles the full task completion flow with a transaction.
func (s *TaskService) CompleteTask(ctx context.Context, taskID uuid.UUID, userVars map[string]interface{}) error {

	// 1. Load the task
	task, err := s.taskRepo.FindTaskByID(taskID)
	if err != nil {
		return fmt.Errorf("task not found: %w", err)
	}
	if task.Status == "completed" {
		return fmt.Errorf("task already completed")
	}

	// Store task before completion for event
	taskBefore := task

	// 2. Load process definition and graph
	defID, err := s.instRepo.GetProcessDefinitionID(task.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("process instance not found: %w", err)
	}
	def, err := s.procRepo.FindProcessDefinitionByID(defID)
	if err != nil {
		return fmt.Errorf("process definition not found: %w", err)
	}
	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return fmt.Errorf("failed to unmarshal graph: %w", err)
	}

	// 3. Get current variables & merge user variables
	currentVars, err := s.instRepo.GetVariables(task.ProcessInstanceID)
	if err != nil {
		return err
	}
	for k, v := range userVars {
		currentVars[k] = v
	}

	// 4. Find the user task node and resolve next element
	userNode, ok := graph.Nodes[task.TaskDefinitionKey]
	if !ok || userNode.Type != engine.UserTaskType {
		return fmt.Errorf("user task node not found")
	}

	// Use common.ResolveNext to find the next node
	nextID, err := common.ResolveNext(userNode, currentVars)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	// node has no outgoing flows
	if nextID == "" {
		return s.completeProcessAndTask(task, currentVars, taskBefore)
	}
	// 5. Start transaction (move token, complete task, update variables)
	tx := database.NewTransaction(s.db)

	// a. Move execution token
	tx.AddUpdateQuery("public.executions", []database.QueryParameter{
		{Key: "current_element_id", Value: nextID},
		{Key: "updated_at", Value: time.Now()},
	}, []database.QueryParameter{
		{Key: "id", Value: *task.ExecutionID},
	})

	// b. Mark task as completed
	tx.AddUpdateQuery("public.tasks", []database.QueryParameter{
		{Key: "status", Value: "completed"},
		{Key: "completed_at", Value: time.Now()},
	}, []database.QueryParameter{
		{Key: "id", Value: task.ID},
	})

	// c. Merge variables (upsert)
	varJSON, _ := json.Marshal(currentVars)
	tx.AddDeleteQuery("public.variables", []database.QueryParameter{
		{Key: "process_instance_id", Value: task.ProcessInstanceID},
	})
	tx.AddInsertQuery("public.variables", []database.QueryParameter{
		{Key: "id", Value: uuid.New().String()},
		{Key: "process_instance_id", Value: task.ProcessInstanceID},
		{Key: "data", Value: varJSON},
		{Key: "updated_at", Value: time.Now()},
	})

	// d. Reactivate execution (if status was 'waiting')
	tx.AddUpdateQuery("public.executions", []database.QueryParameter{
		{Key: "status", Value: "active"},
		{Key: "updated_at", Value: time.Now()},
	}, []database.QueryParameter{
		{Key: "id", Value: task.ExecutionID},
	})

	eventPayload := map[string]interface{}{
		"task_id":      task.ID,
		"task_key":     task.TaskDefinitionKey,
		"assignee":     task.Assignee,
		"variables":    currentVars,
		"next_element": nextID,
		"completed_at": time.Now(),
	}

	eventPayloadJson, _ := json.Marshal(eventPayload)

	tx.AddInsertQuery("public.process_outbox", []database.QueryParameter{
		{Key: "process_instance_id", Value: task.ProcessInstanceID},
		{Key: "event_type", Value: "TaskComplete"},
		{Key: "payload", Value: eventPayloadJson},
	})

	// Execute all
	_, success, err := tx.Execute()
	if err != nil || !success {
		return fmt.Errorf("transaction failed: %w", err)
	}

	// Dispatch TASK_COMPLETED event
	if s.dispatcher != nil {
		assignee := ""
		if taskBefore.Assignee != nil {
			assignee = *taskBefore.Assignee
		}

		processKey := ""
		if procDef, err := s.procRepo.FindProcessDefinitionByID(defID); err == nil {
			processKey = procDef.ProcessKey
		}

		event := &events.TaskEvent{
			ID:                uuid.New().String(),
			EventType:         events.TaskCompleted,
			TaskID:            taskID.String(),
			ProcessInstanceID: task.ProcessInstanceID.String(),
			ExecutionID:       task.ExecutionID.String(),
			TaskName:          task.TaskName,
			TaskDefinitionKey: task.TaskDefinitionKey,
			Assignee:          &assignee,
			OldStatus:         &taskBefore.Status,
			NewStatus:         "completed",
			Timestamp:         time.Now(),
			ProcessKey:        processKey,
			Variables: map[string]interface{}{
				"next_element": nextID,
				"user_vars":    userVars,
			},
		}

		go s.dispatcher.Dispatch(ctx, event)
	}

	// 6. Resume execution using the callback function
	if s.resumeExecutor != nil {
		return s.resumeExecutor(ctx, &graph, *task.ExecutionID)
	}

	return fmt.Errorf("resume executor not set")
}

// Helper to finish process when no next node exists
func (s *TaskService) completeProcessAndTask(task models.Task, currentVars map[string]interface{}, taskBefore models.Task) error {
	tx := database.NewTransaction(s.db)

	// Mark task as completed
	tx.AddUpdateQuery("public.tasks", []database.QueryParameter{
		{Key: "status", Value: "completed"},
		{Key: "completed_at", Value: time.Now()},
	}, []database.QueryParameter{
		{Key: "id", Value: task.ID},
	})

	// Save final variables
	varJSON, _ := json.Marshal(currentVars)
	tx.AddDeleteQuery("public.variables", []database.QueryParameter{
		{Key: "process_instance_id", Value: task.ProcessInstanceID},
	})
	tx.AddInsertQuery("public.variables", []database.QueryParameter{
		{Key: "id", Value: uuid.New().String()},
		{Key: "process_instance_id", Value: task.ProcessInstanceID},
		{Key: "data", Value: varJSON},
		{Key: "updated_at", Value: time.Now()},
	})

	// End process instance
	tx.AddUpdateQuery("public.process_instances", []database.QueryParameter{
		{Key: "status", Value: "completed"},
		{Key: "ended_at", Value: time.Now()},
	}, []database.QueryParameter{
		{Key: "id", Value: task.ProcessInstanceID},
	})

	// Mark execution as completed
	tx.AddUpdateQuery("public.executions", []database.QueryParameter{
		{Key: "status", Value: "completed"},
		{Key: "is_active", Value: false},
		{Key: "updated_at", Value: time.Now()},
	}, []database.QueryParameter{
		{Key: "id", Value: *task.ExecutionID},
	})

	eventPayload := map[string]interface{}{
		"task_id":      task.ID,
		"task_key":     task.TaskDefinitionKey,
		"assignee":     task.Assignee,
		"variables":    currentVars,
		"completed_at": time.Now(),
	}

	eventPayloadJson, _ := json.Marshal(eventPayload)

	tx.AddInsertQuery("public.process_outbox", []database.QueryParameter{
		{Key: "process_instance_id", Value: task.ProcessInstanceID.String()},
		{Key: "event_type", Value: "TaskComplete"},
		{Key: "payload", Value: eventPayloadJson},
	})

	_, success, err := tx.Execute()
	if err != nil || !success {
		return fmt.Errorf("transaction failed while ending process: %w", err)
	}

	// Dispatch TASK_COMPLETED event for final task
	if s.dispatcher != nil {
		assignee := ""
		if taskBefore.Assignee != nil {
			assignee = *taskBefore.Assignee
		}

		processKey := ""
		if defID, err := s.instRepo.GetProcessDefinitionID(task.ProcessInstanceID); err == nil {
			if procDef, err := s.procRepo.FindProcessDefinitionByID(defID); err == nil {
				processKey = procDef.ProcessKey
			}
		}

		event := &events.TaskEvent{
			ID:                uuid.New().String(),
			EventType:         events.TaskCompleted,
			TaskID:            task.ID.String(),
			ProcessInstanceID: task.ProcessInstanceID.String(),
			ExecutionID:       task.ExecutionID.String(),
			TaskName:          task.TaskName,
			Assignee:          &assignee,
			OldStatus:         &taskBefore.Status,
			NewStatus:         "completed",
			Timestamp:         time.Now(),
			ProcessKey:        processKey,
			Variables: map[string]interface{}{
				"process_ended": true,
				"final_vars":    currentVars,
			},
		}

		go s.dispatcher.Dispatch(context.Background(), event)
	}

	return nil
}
