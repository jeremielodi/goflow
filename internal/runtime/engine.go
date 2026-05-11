package runtime

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/jeremielodi/goflow/internal/engine"

	"context"
	"database/sql"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// ------------------------------------------------------------
// PROCESS EXECUTION RESULT
// ------------------------------------------------------------

// ExecutionResult represents the runtime state after execution step.
type ExecutionResult struct {
	ProcessEnded bool

	// Current waiting task (if any)
	WaitingTaskID   string
	WaitingTaskName string

	// Last executed BPMN element
	CurrentElementID string

	// Next element after current execution
	NextElementID string
}

// ------------------------------------------------------------
// CONDITION EVALUATION
// ------------------------------------------------------------

// EvaluateCondition translates BPMN FEEL-like syntax into CEL
// and evaluates it against runtime variables.
func EvaluateCondition(
	expr string,
	variables map[string]interface{},
) (bool, error) {

	// Empty condition = always true
	if strings.TrimSpace(expr) == "" {
		return true, nil
	}

	// --------------------------------------------------------
	// Normalize BPMN syntax
	// --------------------------------------------------------

	expr = strings.TrimSpace(expr)

	// remove leading "="
	expr = strings.TrimPrefix(expr, "=")

	// remove surrounding quotes
	if len(expr) > 1 {
		if (expr[0] == '"' && expr[len(expr)-1] == '"') ||
			(expr[0] == '\'' && expr[len(expr)-1] == '\'') {
			expr = expr[1 : len(expr)-1]
		}
	}

	// Convert FEEL-like syntax to CEL
	expr = convertFEELToCEL(expr)

	// --------------------------------------------------------
	// Build CEL environment dynamically
	// --------------------------------------------------------

	envOptions := []cel.EnvOption{}

	for variableName := range variables {
		envOptions = append(
			envOptions,
			cel.Variable(variableName, cel.DynType),
		)
	}

	env, err := cel.NewEnv(envOptions...)
	if err != nil {
		return false, fmt.Errorf(
			"failed to create CEL environment: %w",
			err,
		)
	}

	// --------------------------------------------------------
	// Compile expression
	// --------------------------------------------------------

	ast, issues := env.Compile(expr)

	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf(
			"condition compilation error: %w",
			issues.Err(),
		)
	}

	// --------------------------------------------------------
	// Create executable program
	// --------------------------------------------------------

	prg, err := env.Program(ast)
	if err != nil {
		return false, fmt.Errorf(
			"failed to create CEL program: %w",
			err,
		)
	}

	// --------------------------------------------------------
	// Execute condition
	// --------------------------------------------------------

	out, _, err := prg.Eval(variables)
	if err != nil {
		return false, fmt.Errorf(
			"condition evaluation error: %w",
			err,
		)
	}

	// --------------------------------------------------------
	// Convert result
	// --------------------------------------------------------

	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf(
			"condition did not return boolean",
		)
	}

	return result, nil
}

// convertFEELToCEL converts simple BPMN FEEL syntax into CEL syntax.
func convertFEELToCEL(expr string) string {

	replacer := strings.NewReplacer(
		" and ", " && ",
		" or ", " || ",
		" = ", " == ",
		" not ", " !",
		" true", " true",
		" false", " false",
	)

	return replacer.Replace(expr)
}

// ------------------------------------------------------------
// EXECUTION
// ------------------------------------------------------------

type Runtime struct {
	Graph *engine.ProcessGraph
	DB    *sqlx.DB
}

// ------------------------------------------------------------
// EvaluateCondition (unchanged from your version, but moved to a helper file)
// ------------------------------------------------------------

// ------------------------------------------------------------
// ExecuteExecution starts or resumes from a given execution ID
// ------------------------------------------------------------
func (r *Runtime) ExecuteExecution(ctx context.Context, execID uuid.UUID) error {
	// 1. Load execution, process instance, variables, and graph
	var exec struct {
		ID                uuid.UUID `db:"id"`
		ProcessInstanceID uuid.UUID `db:"process_instance_id"`
		CurrentElementID  string    `db:"current_element_id"`
		Status            string    `db:"status"`
		IsActive          bool      `db:"is_active"`
	}
	err := r.DB.GetContext(ctx, &exec, `
        SELECT id, process_instance_id, current_element_id, status, is_active
        FROM public.executions
        WHERE id = $1 AND is_active = true
    `, execID)
	if err != nil {
		return fmt.Errorf("failed to load execution: %w", err)
	}

	// 2. Load process variables
	var varsData []byte
	err = r.DB.GetContext(ctx, &varsData, `
        SELECT data FROM public.variables
        WHERE process_instance_id = $1
    `, exec.ProcessInstanceID)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to load variables: %w", err)
	}
	variables := make(map[string]interface{})
	if len(varsData) > 0 {
		json.Unmarshal(varsData, &variables)
	}

	// 3. Run the token through the graph until a user task or end
	currentID := exec.CurrentElementID
	for {
		node, ok := r.Graph.Nodes[currentID]
		if !ok {
			return fmt.Errorf("node does not exist: %s", currentID)
		}

		switch node.Type {
		case engine.StartEventType, engine.ExclusiveGatewayType:
			next, err := r.resolveNext(ctx, node, variables, exec.ProcessInstanceID)
			if err != nil {
				return fmt.Errorf("Failed to resolve next %s", err)
			}
			// Move execution token
			err = r.updateExecution(ctx, exec.ID, next, variables)
			if err != nil {
				return fmt.Errorf("Failed to update execution %s, next is %s", err, next)
			}
			currentID = next
			return nil

		case engine.ServiceTaskType:
			// For now, execute synchronously (later create a job)
			fmt.Printf("Executing service task: %s\n", node.Name)
			// Optionally call an external worker hook
			next, err := r.resolveNext(ctx, node, variables, exec.ProcessInstanceID)
			if err != nil {
				return err
			}
			err = r.updateExecution(ctx, exec.ID, next, variables)
			if err != nil {
				return err
			}
			currentID = next
			return nil

		case engine.UserTaskType:
			// Create a task in the tasks table
			taskID, err := r.createUserTask(ctx, exec.ProcessInstanceID, exec.ID, node, variables)
			if err != nil {
				return err
			}
			fmt.Printf("UserTask '%s' created, task id = %s\n", node.Name, taskID)
			// Execution stops here – waiting for task completion
			return nil

		case engine.EndEventType:
			// Process instance completed
			_, err = r.DB.ExecContext(ctx, `
                UPDATE public.process_instances
                SET status = 'completed', ended_at = $1
                WHERE id = $2
            `, time.Now(), exec.ProcessInstanceID)
			if err != nil {
				return err
			}
			// Mark execution as completed
			_, err = r.DB.ExecContext(ctx, `
                UPDATE public.executions
                SET status = 'completed', is_active = false, updated_at = $1
                WHERE id = $2
            `, time.Now(), exec.ID)
			fmt.Println("Process completed.")
			return nil

		default:
			return fmt.Errorf("unknown node type: %s", node.Type)
		}
	}
}

// ------------------------------------------------------------
// Helper: resolveNext (similar to your resolveNext but persists condition evaluation)
// ------------------------------------------------------------
func (r *Runtime) resolveNext(ctx context.Context, node *engine.Node, variables map[string]interface{}, instanceID uuid.UUID) (string, error) {
	if len(node.Outgoing) == 0 {
		return "", fmt.Errorf("node %s has no outgoing flows", node.ID)
	}
	if len(node.Outgoing) == 1 {
		flow := node.Outgoing[0]
		if flow.Condition != "" {
			ok, err := EvaluateCondition(flow.Condition, variables)
			if err != nil {
				return "", fmt.Errorf("condition evaluation failed: %w", err)
			}
			if !ok {
				return "", fmt.Errorf("condition false on single outgoing flow")
			}
		}
		return flow.TargetRef, nil
	}
	// Exclusive gateway
	var selected *engine.Flow
	for _, flow := range node.Outgoing {
		ok, err := EvaluateCondition(flow.Condition, variables)
		if err != nil {
			return "", err
		}
		if ok {
			if selected != nil {
				return "", fmt.Errorf("multiple conditions true on exclusive gateway")
			}
			selected = &flow
		}
	}
	if selected == nil {
		return "", fmt.Errorf("no matching condition on exclusive gateway")
	}
	return selected.TargetRef, nil
}

// ------------------------------------------------------------
// updateExecution moves token and optionally updates variables
// ------------------------------------------------------------
func (r *Runtime) updateExecution(ctx context.Context, execID uuid.UUID, newElementID string, newVars map[string]interface{}) error {
	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Update execution current element
	_, err = tx.ExecContext(ctx, `
        UPDATE public.executions
        SET current_element_id = $1, updated_at = $2
        WHERE id = $3
    `, newElementID, time.Now(), execID)
	if err != nil {
		return fmt.Errorf("1. update failed %s", err.Error())
	}

	// If variables changed, merge them
	if len(newVars) > 0 {
		// First load current variables for this execution's instance
		var instanceID uuid.UUID
		err = tx.GetContext(ctx, &instanceID, `
            SELECT process_instance_id FROM public.executions WHERE id = $1
        `, execID)
		if err != nil {
			return fmt.Errorf("2. update failed %s", err.Error())
		}
		var existingData []byte
		err = tx.GetContext(ctx, &existingData, `
            SELECT data FROM public.variables WHERE process_instance_id = $1
        `, instanceID)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("2. update failed %s", err.Error())
		}
		merged := make(map[string]interface{})
		if len(existingData) > 0 {
			json.Unmarshal(existingData, &merged)
		}
		for k, v := range newVars {
			merged[k] = v
		}
		newJSON, _ := json.Marshal(merged)
		_, err = tx.ExecContext(ctx, `
            INSERT INTO public.variables (id, process_instance_id, data, updated_at)
            VALUES ($1, $2, $3, $4)
        `, uuid.New(), instanceID, newJSON, time.Now())
		if err != nil {
			return fmt.Errorf("5. update failed %s", err.Error())
		}
	}

	return tx.Commit()
}

// ------------------------------------------------------------
// createUserTask inserts a task and returns its ID
// ------------------------------------------------------------
func (r *Runtime) createUserTask(ctx context.Context, instanceID, execID uuid.UUID, node *engine.Node, variables map[string]interface{}) (uuid.UUID, error) {
	taskID := uuid.New()
	_, err := r.DB.ExecContext(ctx, `
        INSERT INTO public.tasks (
            id, process_instance_id, execution_id, task_definition_key, task_name,
            status, created_at, form_data
        ) VALUES ($1, $2, $3, $4, $5, 'created', $6, $7)
    `, taskID, instanceID, execID, node.ID, node.Name, time.Now(), variables)
	if err != nil {
		return uuid.Nil, err
	}
	// Also mark execution as waiting? Optionally, set execution status to 'waiting'
	_, err = r.DB.ExecContext(ctx, `
        UPDATE public.executions
        SET status = 'waiting', updated_at = $1
        WHERE id = $2
    `, time.Now(), execID)
	return taskID, err
}
