// service/task_service.go
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type TaskService struct {
	db             *sqlx.DB
	taskRepo       *repository.TaskRepository
	procRepo       *repository.ProcessRepository
	instRepo       *repository.ProcessInstanceRepository
	resumeExecutor ResumeExecutionFunc // Callback for resuming execution
	dispatcher     *events.TaskEventDispatcher
	auditService   *AuditService
}

// ResumeExecutionFunc is a function type for resuming execution
type ResumeExecutionFunc func(ctx context.Context, graph *engine.ProcessGraph, execID uuid.UUID) error

// NewTaskService creates a new TaskService
func NewTaskService(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *TaskService {
	return &TaskService{
		db:           db,
		taskRepo:     repository.NewTaskRepository(db, dispatcher),
		procRepo:     repository.NewProcessRepository(db),
		instRepo:     repository.NewProcessInstanceRepository(db),
		dispatcher:   dispatcher,
		auditService: NewAuditService(db),
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
	if task.Status == "cancelled" {
		// e.g. interrupted by a boundary event: its token has already moved on.
		return fmt.Errorf("task was cancelled")
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

	// 3. Find the user task node
	userNode, ok := graph.Nodes[task.TaskDefinitionKey]
	if !ok || userNode.Type != engine.UserTaskType {
		return fmt.Errorf("user task node not found")
	}

	// 4. Complete the task, set its variables (Camunda 8 scopes and output
	// mappings, see ApplyActivityOutputTx) and move the token, atomically.
	sqlTx, err := s.db.Beginx()
	if err != nil {
		return err
	}
	defer sqlTx.Rollback()

	res, err := sqlTx.Exec(`
		UPDATE public.tasks SET status = 'completed', completed_at = NOW()
		WHERE process_instance_id = $1 AND id = $2 AND status NOT IN ('completed', 'cancelled')
	`, task.ProcessInstanceID, task.ID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("task already completed or cancelled")
	}
	if err := ApplyActivityOutputTx(sqlTx, task.ProcessInstanceID, *task.ExecutionID, userNode, userVars); err != nil {
		return fmt.Errorf("failed to update variables: %w", err)
	}

	// Without an outgoing flow the token ends in the task's scope
	// (engine.ImplicitEnd); the runtime takes it from there.
	nextID, err := common.ResolveNext(userNode, nil)
	if err != nil {
		return err
	}
	if _, err := sqlTx.Exec(`
		UPDATE public.executions SET current_element_id = $3, status = 'active', updated_at = NOW()
		WHERE process_instance_id = $1 AND id = $2
	`, task.ProcessInstanceID, *task.ExecutionID, nextID); err != nil {
		return err
	}

	// The task can now be compensated.
	if userNode.CompensationHandler != "" {
		if err := repository.NewEngineRepository(s.db, nil).RecordCompensableTx(sqlTx, task.ProcessInstanceID, *task.ExecutionID, userNode.ID, userNode.CompensationHandler); err != nil {
			return fmt.Errorf("failed to record compensation: %w", err)
		}
	}

	currentVars, err := repository.VariablesForExecution(sqlTx, task.ProcessInstanceID, uuid.Nil)
	if err != nil {
		return err
	}
	eventPayloadJson, _ := json.Marshal(map[string]interface{}{
		"task_id":      task.ID,
		"task_key":     task.TaskDefinitionKey,
		"assignee":     task.Assignee,
		"variables":    currentVars,
		"next_element": nextID,
		"completed_at": time.Now(),
	})
	if _, err := sqlTx.Exec(`
		INSERT INTO public.process_outbox (process_instance_id, event_type, payload) VALUES ($1, 'TaskComplete', $2)
	`, task.ProcessInstanceID, eventPayloadJson); err != nil {
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}
	s.disarmBoundaryEvents(task.ProcessInstanceID, *task.ExecutionID)
	LogAuditErr("task completed", s.auditService.LogTaskCompleted(task.ID, task.ProcessInstanceID, nil, userVars))
	if len(userVars) > 0 {
		LogAuditErr("variables merged", s.auditService.LogVariablesMerged(task.ProcessInstanceID, userVars, "taskComplete"))
	}
	LogAuditErr("execution moved", s.auditService.LogExecutionMoved(*task.ExecutionID, task.ProcessInstanceID, task.TaskDefinitionKey, nextID, currentVars))
	if p := eventlog.Global(); p != nil {
		p.TaskCompleted(task.ProcessInstanceID.String(), task.ID.String())
		p.ExecutionMoved(task.ProcessInstanceID.String(), task.ExecutionID.String(), task.TaskDefinitionKey, nextID)
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

// disarmBoundaryEvents removes the boundary event triggers of the user task
// its execution just left. Best-effort: a trigger that survives a failure
// here is discarded when it fires, since its task is no longer active.
func (s *TaskService) disarmBoundaryEvents(instanceID, execID uuid.UUID) {
	tx, err := s.db.Beginx()
	if err != nil {
		log.Printf("disarming boundary events of execution %s: %v", execID, err)
		return
	}
	defer tx.Rollback()
	if err := repository.NewEngineRepository(s.db, nil).DisarmBoundaryTriggersTx(tx, instanceID, execID); err != nil {
		log.Printf("disarming boundary events of execution %s: %v", execID, err)
		return
	}
	if err := tx.Commit(); err != nil {
		log.Printf("disarming boundary events of execution %s: %v", execID, err)
	}
}
