package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

type TaskService struct {
	db       *sqlx.DB
	taskRepo *repository.TaskRepository
	procRepo *repository.ProcessRepository
	instRepo *repository.ProcessInstanceRepository
}

func NewTaskService(db *sqlx.DB) *TaskService {
	return &TaskService{
		db:       db,
		taskRepo: repository.NewTaskRepository(db),
		procRepo: repository.NewProcessRepository(db),
		instRepo: repository.NewProcessInstanceRepository(db),
	}
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

	// 2. Load process definition and graph
	defID, err := s.instRepo.GetProcessDefinitionID(task.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("process instance not found: %w", err)
	}
	print("Porcess definition id ", defID.String())
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
	nextID, err := s.resolveNext(&graph, userNode, currentVars)
	if err != nil {
		return fmt.Errorf("failed to resolve next node: %w", err)
	}

	print("Next node", nextID)

	// 5. Start transaction using your database.NewTransaction
	tx := database.NewTransaction(s.db)

	// a. Move execution token
	tx.AddUpdateQuery("public.executions", []database.QueryParameter{
		{Key: "current_element_id", Value: nextID},
		{Key: "updated_at", Value: time.Now()},
	}, []database.QueryParameter{
		{Key: "id", Value: task.ExecutionID},
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
		{Key: "id", Value: *task.ExecutionID},
	})

	// Execute all
	_, success, err := tx.Execute()
	if err != nil || !success {
		return fmt.Errorf("transaction failed: %w", err)
	}

	// 6. Resume runtime
	rt := &runtime.Runtime{
		Graph: &graph,
		DB:    s.db,
	}
	return rt.ExecuteExecution(ctx, *task.ExecutionID)
}

// Helper to resolve next element (copied from runtime logic)
func (s *TaskService) resolveNext(graph *engine.ProcessGraph, node *engine.Node, vars map[string]interface{}) (string, error) {
	if len(node.Outgoing) == 0 {
		return "", fmt.Errorf("no outgoing flows")
	}
	if len(node.Outgoing) == 1 {
		flow := node.Outgoing[0]
		if flow.Condition != "" {
			ok, err := runtime.EvaluateCondition(flow.Condition, vars)
			if err != nil {
				return "", err
			}
			if !ok {
				return "", fmt.Errorf("condition false on single outgoing flow")
			}
		}
		return flow.TargetRef, nil
	}
	// exclusive gateway
	var selected *engine.Flow
	for _, flow := range node.Outgoing {
		ok, err := runtime.EvaluateCondition(flow.Condition, vars)
		if err != nil {
			return "", err
		}
		if ok {
			if selected != nil {
				return "", fmt.Errorf("multiple conditions true")
			}
			selected = &flow
		}
	}
	if selected == nil {
		return "", fmt.Errorf("no matching condition")
	}
	return selected.TargetRef, nil
}
