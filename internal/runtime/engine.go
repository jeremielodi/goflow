package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
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

	// Strip Camunda 7 ${...} wrapper
	// ${amount > 1000} → amount > 1000
	if strings.HasPrefix(expr, "${") && strings.HasSuffix(expr, "}") {
		expr = expr[2 : len(expr)-1]
		expr = strings.TrimSpace(expr)
	}

	// Strip FEEL = prefix
	// =amount > 1000 → amount > 1000
	if strings.HasPrefix(expr, "=") {
		expr = strings.TrimPrefix(expr, "=")
		expr = strings.TrimSpace(expr)
	}

	// Strip surrounding quotes
	// "amount > 1000" → amount > 1000
	if len(expr) > 1 {
		if (expr[0] == '"' && expr[len(expr)-1] == '"') ||
			(expr[0] == '\'' && expr[len(expr)-1] == '\'') {
			expr = expr[1 : len(expr)-1]
		}
	}

	// --------------------------------------------------------
	// Convert FEEL-like syntax to CEL
	// --------------------------------------------------------

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
			"condition compilation error for '%s': %w",
			expr,
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
			"condition '%s' did not return boolean, got %T: %v",
			expr,
			out.Value(),
			out.Value(),
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

func NewRuntime(Graph *engine.ProcessGraph, DB *sqlx.DB) *Runtime {
	return &Runtime{
		Graph: Graph,
		DB:    DB,
	}
}

// ------------------------------------------------------------
// ExecuteExecution starts or resumes from a given execution ID
// ------------------------------------------------------------
func (r *Runtime) ExecuteExecution(ctx context.Context, execID uuid.UUID) error {
	engineRepo := repository.NewEngineRepository(r.DB)

	// Get active execution
	exec, err := engineRepo.GetActiveExecution(ctx, execID)
	if err != nil {
		return fmt.Errorf("failed to load execution: %w", err)
	}
	if exec == nil {
		return fmt.Errorf("execution not found or not active")
	}

	// Get variables
	variables, err := engineRepo.GetProcessVariables(exec.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("failed to load variables: %w", err)
	}

	// Get process graph
	graph, err := engineRepo.GetProcessGraphByInstanceID(exec.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("failed to load process graph: %w", err)
	}

	currentID := exec.CurrentElementID
	for {
		node, err := engineRepo.GetNodeByID(graph, currentID)
		if err != nil {
			return err
		}

		switch node.Type {
		case engine.StartEventType, engine.ExclusiveGatewayType:
			next, err := r.ResolveNext(ctx, node, variables)
			if err != nil {
				return err
			}

			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}

			err = engineRepo.UpdateExecutionNodeTx(tx, exec.ID, next)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to update execution node: %w", err)
			}

			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			currentID = next

		case engine.ServiceTaskType:
			// Begin transaction
			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback()

			// Move token to current service task node
			err = engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ID, "active", node.ID)
			if err != nil {
				return fmt.Errorf("failed to update execution: %w", err)
			}

			// Validate job type
			if node.JobType == nil {
				return fmt.Errorf("service task %s has no job type", node.ID)
			}

			// Prepare payload from variables
			payload, err := json.Marshal(variables)
			if err != nil {
				return fmt.Errorf("failed to marshal variables: %w", err)
			}

			// Create job
			jobID := uuid.New()
			err = engineRepo.CreateJobTx(tx, jobID, exec.ProcessInstanceID, exec.ID, *node.JobType, payload)
			if err != nil {
				return fmt.Errorf("failed to create job: %w", err)
			}

			// Update execution status to waiting
			err = engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting")
			if err != nil {
				return fmt.Errorf("failed to update execution status: %w", err)
			}

			// Commit transaction
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			// Log job creation
			fmt.Printf("Job %s created for service task %s (type: %s)\n", jobID, node.ID, *node.JobType)

			return nil

		case engine.UserTaskType:
			// Begin transaction
			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback()

			// Move token to current user task node
			err = engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ID, "active", node.ID)
			if err != nil {
				return fmt.Errorf("failed to update execution: %w", err)
			}

			// Resolve assignee and candidate group
			var assignee *string
			if node.AssigneeExpr != nil {
				resolved := resolveExpression(*node.AssigneeExpr, variables)
				if resolved != "" {
					assignee = &resolved
				}
			}

			var candidateGroup *string
			if node.CandidateGroupExpr != nil {
				resolved := resolveExpression(*node.CandidateGroupExpr, variables)
				if resolved != "" {
					candidateGroup = &resolved
				}
			}

			// Create user task
			taskID := uuid.New()
			taskName := node.Name
			err = engineRepo.CreateUserTaskTx(tx, &repository.UserTask{
				ID:                taskID,
				ProcessInstanceID: exec.ProcessInstanceID,
				ExecutionID:       exec.ID,
				TaskDefinitionKey: node.ID,
				TaskName:          &taskName,
				Assignee:          assignee,
				CandidateGroup:    candidateGroup,
			})
			if err != nil {
				return fmt.Errorf("failed to create user task: %w", err)
			}

			// Update execution status to waiting
			err = engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting")
			if err != nil {
				return fmt.Errorf("failed to update execution status: %w", err)
			}

			// Commit transaction
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			return nil

		case engine.EndEventType:
			// Begin transaction
			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback()

			// Complete process instance
			err = engineRepo.CompleteProcessInstanceTx(tx, exec.ProcessInstanceID)
			if err != nil {
				return fmt.Errorf("failed to complete process instance: %w", err)
			}

			// Complete execution
			err = engineRepo.CompleteExecutionTx(tx, exec.ID)
			if err != nil {
				return fmt.Errorf("failed to complete execution: %w", err)
			}

			// Commit transaction
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			return nil

		default:
			return fmt.Errorf("unknown node type: %s", node.Type)
		}
	}
}

// ------------------------------------------------------------
// Helper: ResolveNext (evaluates conditions to find next node)
// ------------------------------------------------------------
func (r *Runtime) ResolveNext(ctx context.Context, node *engine.Node, variables map[string]interface{}) (string, error) {
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
// Helper: resolveExpression resolves expression strings like ${variableName}
// ------------------------------------------------------------
func resolveExpression(expr string, vars map[string]interface{}) string {
	expr = strings.TrimSpace(expr)
	// If it looks like a variable reference: ${...}
	if strings.HasPrefix(expr, "${") && strings.HasSuffix(expr, "}") {
		key := expr[2 : len(expr)-1] // extract variable name
		if val, ok := vars[key]; ok {
			return fmt.Sprintf("%v", val)
		}
		// fallback: return empty if variable not found
		return ""
	}
	// Otherwise treat as literal string
	return expr
}
