package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"

	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

var (
	// Compilation cache to avoid re-compiling same expressions
	compilationCache     = make(map[string]*cachedProgram)
	compilationCacheLock sync.RWMutex

	// Common CEL environment with built-in functions
	commonEnvOptions = []cel.EnvOption{
		cel.Declarations(
			decls.NewFunction("contains",
				decls.NewOverload("contains_string_string",
					[]*exprpb.Type{decls.String, decls.String},
					decls.Bool)),
			decls.NewFunction("startsWith",
				decls.NewOverload("startsWith_string_string",
					[]*exprpb.Type{decls.String, decls.String},
					decls.Bool)),
			decls.NewFunction("endsWith",
				decls.NewOverload("endsWith_string_string",
					[]*exprpb.Type{decls.String, decls.String},
					decls.Bool)),
			decls.NewFunction("matches",
				decls.NewOverload("matches_string_string",
					[]*exprpb.Type{decls.String, decls.String},
					decls.Bool)),
			decls.NewFunction("isEmpty",
				decls.NewOverload("isEmpty_string",
					[]*exprpb.Type{decls.String},
					decls.Bool)),
			decls.NewFunction("isNotEmpty",
				decls.NewOverload("isNotEmpty_string",
					[]*exprpb.Type{decls.String},
					decls.Bool)),
		),
		cel.HomogeneousAggregateLiterals(),
		cel.EagerlyValidateDeclarations(true),
	}
)

type cachedProgram struct {
	program cel.Program
	env     *cel.Env
	expr    string
}

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
	normalized, err := normalizeExpression(expr)
	if err != nil {
		return false, err
	}

	// Generate cache key based on normalized expression and variable names
	cacheKey := generateCacheKey(normalized, variables)

	// Try to get from cache
	cached := getCachedProgram(cacheKey)
	if cached != nil {
		return executeProgram(cached.program, variables, normalized)
	}

	// --------------------------------------------------------
	// Build CEL environment with variable names
	// --------------------------------------------------------
	envOptions := make([]cel.EnvOption, 0, len(commonEnvOptions)+len(variables))
	envOptions = append(envOptions, commonEnvOptions...)

	for variableName := range variables {
		envOptions = append(envOptions, cel.Variable(variableName, cel.DynType))
	}

	env, err := cel.NewEnv(envOptions...)
	if err != nil {
		return false, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	// --------------------------------------------------------
	// Compile expression
	// --------------------------------------------------------
	ast, issues := env.Compile(normalized)
	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf("condition compilation error for '%s': %w", normalized, issues.Err())
	}

	// --------------------------------------------------------
	// Create executable program
	// --------------------------------------------------------
	prg, err := env.Program(ast)
	if err != nil {
		return false, fmt.Errorf("failed to create CEL program: %w", err)
	}

	// Cache the program
	setCachedProgram(cacheKey, &cachedProgram{
		program: prg,
		env:     env,
		expr:    normalized,
	})

	return executeProgram(prg, variables, normalized)
}

// normalizeExpression normalizes BPMN/FEEL expression to CEL syntax
func normalizeExpression(expr string) (string, error) {
	expr = strings.TrimSpace(expr)

	// Strip Camunda 7 ${...} wrapper
	if strings.HasPrefix(expr, "${") && strings.HasSuffix(expr, "}") {
		expr = expr[2 : len(expr)-1]
		expr = strings.TrimSpace(expr)
	}

	// Strip FEEL = prefix
	if strings.HasPrefix(expr, "=") {
		expr = strings.TrimPrefix(expr, "=")
		expr = strings.TrimSpace(expr)
	}

	// Strip surrounding quotes
	if (strings.HasPrefix(expr, "\"") && strings.HasSuffix(expr, "\"")) ||
		(strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'")) {
		expr = expr[1 : len(expr)-1]
	}

	return convertFEELToCEL(expr), nil
}

// convertFEELToCEL converts BPMN FEEL syntax to CEL syntax
func convertFEELToCEL(expr string) string {
	// Order matters! Do longer/regex replacements first

	// Convert comparison operators
	replacements := []struct {
		feel    string
		cel     string
		regex   bool
		pattern string
	}{
		// Logical operators
		{feel: " and ", cel: " && ", regex: false},
		{feel: " AND ", cel: " && ", regex: false},
		{feel: " or ", cel: " || ", regex: false},
		{feel: " OR ", cel: " || ", regex: false},

		// Equality operators
		{feel: " = ", cel: " == ", regex: false},
		{feel: " != ", cel: " != ", regex: false},
		{feel: " <> ", cel: " != ", regex: false},

		// NOT operator
		{feel: " not ", cel: " !", regex: false},
		{feel: " NOT ", cel: " !", regex: false},
		{feel: "not(", cel: "!(", regex: false},
		{feel: "NOT(", cel: "!(", regex: false},

		// Null checks
		{feel: " is null", cel: " == null", regex: false},
		{feel: " IS NULL", cel: " == null", regex: false},
		{feel: " is not null", cel: " != null", regex: false},
		{feel: " IS NOT NULL", cel: " != null", regex: false},

		// String functions
		{regex: true, pattern: `contains\(\s*([^,]+)\s*,\s*([^)]+)\s*\)`, cel: `${1}.contains(${2})`},
		{regex: true, pattern: `startsWith\(\s*([^,]+)\s*,\s*([^)]+)\s*\)`, cel: `${1}.startsWith(${2})`},
		{regex: true, pattern: `endsWith\(\s*([^,]+)\s*,\s*([^)]+)\s*\)`, cel: `${1}.endsWith(${2})`},
		{regex: true, pattern: `matches\(\s*([^,]+)\s*,\s*([^)]+)\s*\)`, cel: `${1}.matches(${2})`},
		{regex: true, pattern: `isEmpty\(\s*([^)]+)\s*\)`, cel: `${1} == "" || ${1} == null`},
		{regex: true, pattern: `isNotEmpty\(\s*([^)]+)\s*\)`, cel: `${1} != "" && ${1} != null`},

		// String literals (single quote to double quote)
		{regex: true, pattern: `'([^']*)'`, cel: `"${1}"`},
	}

	for _, r := range replacements {
		if r.regex {
			re := regexp.MustCompile(r.pattern)
			expr = re.ReplaceAllString(expr, r.cel)
		} else {
			expr = strings.ReplaceAll(expr, r.feel, r.cel)
		}
	}

	// Clean up multiple spaces
	expr = regexp.MustCompile(`\s+`).ReplaceAllString(expr, " ")

	return expr
}

// executeProgram executes a CEL program with given variables
func executeProgram(prg cel.Program, variables map[string]interface{}, originalExpr string) (bool, error) {
	out, _, err := prg.Eval(variables)
	if err != nil {
		return false, fmt.Errorf("condition evaluation error for '%s': %w", originalExpr, err)
	}

	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("condition '%s' did not return boolean, got %T: %v",
			originalExpr, out.Value(), out.Value())
	}

	return result, nil
}

// generateCacheKey creates a unique key for caching compiled expressions
func generateCacheKey(expr string, variables map[string]interface{}) string {
	hash := sha256.New()
	hash.Write([]byte(expr))

	// Include variable names (but not values) in cache key
	for varName := range variables {
		hash.Write([]byte(varName))
	}

	return hex.EncodeToString(hash.Sum(nil))
}

// getCachedProgram retrieves a cached program
func getCachedProgram(key string) *cachedProgram {
	compilationCacheLock.RLock()
	defer compilationCacheLock.RUnlock()
	return compilationCache[key]
}

// setCachedProgram stores a program in cache
func setCachedProgram(key string, prog *cachedProgram) {
	compilationCacheLock.Lock()
	defer compilationCacheLock.Unlock()

	// Limit cache size to prevent memory bloat
	if len(compilationCache) > 1000 {
		// Simple cleanup: remove oldest entries
		for k := range compilationCache {
			delete(compilationCache, k)
			break
		}
	}
	compilationCache[key] = prog
}

// EvaluateConditionWithDefault evaluates with a default value on error
func EvaluateConditionWithDefault(expr string, variables map[string]interface{}, defaultValue bool) bool {
	result, err := EvaluateCondition(expr, variables)
	if err != nil {
		return defaultValue
	}
	return result
}

// BatchEvaluateConditions evaluates multiple conditions at once
func BatchEvaluateConditions(
	conditions map[string]string,
	variables map[string]interface{},
) (map[string]bool, error) {
	results := make(map[string]bool, len(conditions))

	for id, expr := range conditions {
		result, err := EvaluateCondition(expr, variables)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate condition %s: %w", id, err)
		}
		results[id] = result
	}

	return results, nil
}

// ClearConditionCache clears the compilation cache (useful for testing)
func ClearConditionCache() {
	compilationCacheLock.Lock()
	defer compilationCacheLock.Unlock()
	compilationCache = make(map[string]*cachedProgram)
}

// ------------------------------------------------------------
// TIMER HELPERS
// ------------------------------------------------------------

// parseTimerDuration parses BPMN timer duration like PT30S, PT1M, PT1H
func parseTimerDuration(timerDef string) time.Duration {
	// Extract from XML: <timeDuration>PT30S</timeDuration>
	re := regexp.MustCompile(`<timeDuration>(.*?)</timeDuration>`)
	matches := re.FindStringSubmatch(timerDef)
	if len(matches) < 2 {
		return 30 * time.Second // default 30 seconds
	}

	durationStr := matches[1]

	// Parse PT format
	if strings.HasPrefix(durationStr, "PT") {
		durationStr = strings.TrimPrefix(durationStr, "PT")

		var duration time.Duration

		// Parse seconds
		if strings.Contains(durationStr, "S") {
			secsStr := strings.Split(durationStr, "S")[0]
			if secs, err := strconv.Atoi(secsStr); err == nil {
				duration += time.Duration(secs) * time.Second
			}
			durationStr = strings.TrimPrefix(durationStr, secsStr+"S")
		}

		// Parse minutes
		if strings.Contains(durationStr, "M") {
			minsStr := strings.Split(durationStr, "M")[0]
			if mins, err := strconv.Atoi(minsStr); err == nil {
				duration += time.Duration(mins) * time.Minute
			}
			durationStr = strings.TrimPrefix(durationStr, minsStr+"M")
		}

		// Parse hours
		if strings.Contains(durationStr, "H") {
			hoursStr := strings.Split(durationStr, "H")[0]
			if hours, err := strconv.Atoi(hoursStr); err == nil {
				duration += time.Duration(hours) * time.Hour
			}
		}

		if duration > 0 {
			return duration
		}
	}

	return 30 * time.Second
}

// ------------------------------------------------------------
// EXECUTION
// ------------------------------------------------------------

type Runtime struct {
	Graph        *engine.ProcessGraph
	DB           *sqlx.DB
	auditService *service.AuditService
	dispatcher   *events.TaskEventDispatcher
}

func NewRuntime(Graph *engine.ProcessGraph, DB *sqlx.DB, dispatcher *events.TaskEventDispatcher) *Runtime {
	return &Runtime{
		Graph:        Graph,
		DB:           DB,
		auditService: service.NewAuditService(DB),
		dispatcher:   dispatcher,
	}
}

// ------------------------------------------------------------
// ExecuteExecution starts or resumes from a given execution ID
// ------------------------------------------------------------
func (r *Runtime) ExecuteExecution(ctx context.Context, execID uuid.UUID) error {
	engineRepo := repository.NewEngineRepository(r.DB, r.dispatcher)

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

		// log.Printf("🔍 ExecuteExecution loop: currentID=%s, node.Type=%s, node.MultiInstance=%v",
		// 	currentID, node.Type, node.MultiInstance != nil)

		switch node.Type {
		case engine.StartEventType, engine.ExclusiveGatewayType:
			next, err := common.ResolveNext(node, variables)
			if err != nil {
				return err
			}

			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}

			if err = engineRepo.UpdateExecutionNodeTx(tx, exec.ID, next); err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to update execution node: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			currentID = next
			r.auditService.LogExecutionMoved(exec.ID, exec.ProcessInstanceID, currentID, next, variables)

		case engine.ServiceTaskType:
			if node.MultiInstance != nil && exec.ParentExecutionID != nil {
				log.Printf("📦 Multi-instance child %s processing service task %s", exec.ID, node.ID)
			}
			return r.executeServiceTask(ctx, engineRepo, exec, node, variables)

		case engine.UserTaskType:
			return r.executeUserTask(ctx, engineRepo, exec, node, graph, variables)

		case engine.ScriptTaskType:
			next, err := r.executeScriptTask(ctx, engineRepo, exec, node, variables)
			if err != nil {
				return err
			}
			currentID = next
			continue

		case engine.BoundaryTimerEventType:
			next, err := r.executeBoundaryTimerNode(ctx, engineRepo, exec, node)
			if err != nil {
				return err
			}
			currentID = next
			continue

		case engine.ErrorBoundaryEventType:
			next, err := r.executeErrorBoundaryNode(ctx, engineRepo, exec, node)
			if err != nil {
				return err
			}
			currentID = next
			continue

		case engine.IntermediateCatchEventType, engine.IntermediateTimerEventType:
			return r.executeIntermediateTimer(ctx, engineRepo, exec, node)

		case engine.ParallelGatewayType:
			// Create parallel gateway executor
			parallelExecutor := NewParallelGatewayExecutor(r.DB, r.dispatcher)

			// Check if this is a fork or join
			if r.isForkGateway(node, exec) {
				// Fork: Create child executions
				err := parallelExecutor.ExecuteFork(ctx, exec.ID, node, variables)
				if err != nil {
					return fmt.Errorf("failed to execute parallel fork: %w", err)
				}
				// Log gateway fork
				r.auditService.LogGatewayForked(exec.ProcessInstanceID, node.ID, len(node.Outgoing))

				// Execute each child execution so branches advance to their first tasks
				engineRepo := repository.NewEngineRepository(r.DB, r.dispatcher)
				children, err := engineRepo.GetChildExecutions(exec.ID)
				if err != nil {
					return fmt.Errorf("failed to get child executions after fork: %w", err)
				}
				for _, child := range children {
					if err := r.ExecuteExecution(ctx, child.ID); err != nil {
						return fmt.Errorf("failed to execute child execution %s: %w", child.ID, err)
					}
				}
				return nil // Parent waits, children continue
			} else {
				// For join, we need to get the expected count from the gateway state
				tx, err := r.DB.BeginTxx(ctx, nil)
				if err != nil {
					return fmt.Errorf("failed to begin transaction: %w", err)
				}

				// Get the gateway state to know expected count
				var gatewayState struct {
					ExpectedIncoming int `db:"expected_incoming"`
					ReceivedIncoming int `db:"received_incoming"`
				}

				err = tx.Get(&gatewayState, `
			SELECT expected_incoming, received_incoming
			FROM public.gateway_state
			WHERE process_instance_id = $1 AND gateway_id = $2 AND status = 'waiting'
		`, exec.ProcessInstanceID, node.ID)

				expectedCount := 0
				receivedCount := 0

				if err == nil {
					expectedCount = gatewayState.ExpectedIncoming
					receivedCount = gatewayState.ReceivedIncoming
				}
				tx.Rollback()

				// Execute join
				err = parallelExecutor.ExecuteJoin(ctx, exec.ID, node)
				if err != nil {
					return fmt.Errorf("failed to execute parallel join: %w", err)
				}

				// Get updated counts after join
				var finalState struct {
					ReceivedIncoming int `db:"received_incoming"`
					ExpectedIncoming int `db:"expected_incoming"`
				}
				err = r.DB.Get(&finalState, `
			SELECT received_incoming, expected_incoming
			FROM public.gateway_state
			WHERE process_instance_id = $1 AND gateway_id = $2
			ORDER BY created_at DESC
			LIMIT 1
		`, exec.ProcessInstanceID, node.ID)

				if err == nil {
					receivedCount = finalState.ReceivedIncoming
					expectedCount = finalState.ExpectedIncoming
				}

				// Log gateway join
				r.auditService.LogGatewayJoined(exec.ProcessInstanceID, node.ID, expectedCount, receivedCount)

				// If all branches have joined, resume the parent execution
				if err == nil && finalState.ReceivedIncoming >= finalState.ExpectedIncoming && exec.ParentExecutionID != nil {
					joinEngineRepo := repository.NewEngineRepository(r.DB, r.dispatcher)
					parentExec, getErr := joinEngineRepo.GetActiveExecution(ctx, *exec.ParentExecutionID)
					if getErr == nil && parentExec != nil {
						if resumeErr := r.ExecuteExecution(ctx, parentExec.ID); resumeErr != nil {
							return fmt.Errorf("failed to resume parent after join: %w", resumeErr)
						}
					}
				}
				return nil
			}
		case engine.InclusiveGatewayType:
			inclusiveExecutor := NewInclusiveGatewayExecutor(r.DB, r.dispatcher)

			if r.isForkGateway(node, exec) {
				if err := inclusiveExecutor.ExecuteFork(ctx, exec.ID, node, variables); err != nil {
					return fmt.Errorf("inclusive gateway fork failed: %w", err)
				}
				r.auditService.LogGatewayForked(exec.ProcessInstanceID, node.ID, len(node.Outgoing))

				children, err := engineRepo.GetChildExecutions(exec.ID)
				if err != nil {
					return fmt.Errorf("failed to get child executions after inclusive fork: %w", err)
				}
				for _, child := range children {
					if err := r.ExecuteExecution(ctx, child.ID); err != nil {
						return fmt.Errorf("failed to execute inclusive branch %s: %w", child.ID, err)
					}
				}
				return nil
			}

			if err := inclusiveExecutor.ExecuteJoin(ctx, exec.ID, node); err != nil {
				return fmt.Errorf("inclusive gateway join failed: %w", err)
			}

			var finalState struct {
				ReceivedIncoming int `db:"received_incoming"`
				ExpectedIncoming int `db:"expected_incoming"`
			}
			r.DB.Get(&finalState, `
				SELECT received_incoming, expected_incoming
				FROM public.gateway_state
				WHERE process_instance_id = $1 AND gateway_id = $2
				ORDER BY created_at DESC LIMIT 1
			`, exec.ProcessInstanceID, node.ID)

			r.auditService.LogGatewayJoined(exec.ProcessInstanceID, node.ID, finalState.ExpectedIncoming, finalState.ReceivedIncoming)

			if finalState.ReceivedIncoming >= finalState.ExpectedIncoming && exec.ParentExecutionID != nil {
				parentExec, getErr := engineRepo.GetActiveExecution(ctx, *exec.ParentExecutionID)
				if getErr == nil && parentExec != nil {
					if resumeErr := r.ExecuteExecution(ctx, parentExec.ID); resumeErr != nil {
						return fmt.Errorf("failed to resume parent after inclusive join: %w", resumeErr)
					}
				}
			}
			return nil

		case engine.EndEventType:
			return r.executeEndEvent(ctx, engineRepo, exec)

		case engine.IntermediateMessageCatchEventType:
			return r.executeMessageCatchEvent(ctx, engineRepo, exec, node)

		case engine.MessageBoundaryEventType:
			return r.executeMessageBoundaryEvent(ctx, engineRepo, exec, node)

		case engine.IntermediateSignalCatchEventType:
			return r.executeSignalCatchEvent(ctx, engineRepo, exec, node)

		case engine.SignalBoundaryEventType:
			return r.executeSignalBoundaryEvent(ctx, engineRepo, exec, node)

		case engine.MessageStartEventType:
			// Message start event: advance past it immediately (started via message endpoint)
			next, err := common.ResolveNext(node, variables)
			if err != nil {
				return err
			}
			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return err
			}
			if err = engineRepo.UpdateExecutionNodeTx(tx, exec.ID, next); err != nil {
				tx.Rollback()
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			currentID = next
			continue

		case engine.EventBasedGatewayType:
			return r.executeEventBasedGateway(ctx, engineRepo, exec, node, graph)

		case engine.CallActivityType:
			return r.executeCallActivity(ctx, engineRepo, exec, node, variables)

		default:
			return fmt.Errorf("unknown node type: %s", node.Type)
		}
	}
}

// isForkGateway determines if a parallel gateway is a fork or join
func (r *Runtime) isForkGateway(node *engine.Node, exec *repository.Execution) bool {
	// A gateway is a fork if:
	// 1. It has multiple outgoing flows (splitting)
	// 2. It has no parent execution (root level) OR
	//    It's the first time we're visiting this gateway

	// Check number of outgoing flows
	if len(node.Outgoing) > 1 {
		// No parent execution means this is a root fork
		if exec.ParentExecutionID == nil {
			return true
		}
		engineRepo := repository.NewEngineRepository(r.DB, r.dispatcher)
		// Check if this gateway already has a waiting state (means we're joining)
		var count = engineRepo.CountGatewayWaitingState(exec.ParentExecutionID, node.ID)
		// If no waiting gateway state, this is a fork
		return count == 0
	}

	// Single outgoing flow - this is likely a join
	return false
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

// internal/runtime/runtime.go

// internal/runtime/runtime.go

func (r *Runtime) handleMultiInstance(ctx context.Context, exec *repository.Execution, node *engine.Node, variables map[string]interface{}) error {
	engineRepo := repository.NewEngineRepository(r.DB, r.dispatcher)
	miHandler := NewMultiInstanceHandler(r.DB)

	// Check if multi-instance already exists
	var miExec models.MultiInstanceExecution
	query := `SELECT * FROM public.multi_instance_executions WHERE execution_id = $1 AND status = 'active'`
	err := r.DB.Get(&miExec, query, exec.ID)

	if err != nil {
		// First time - create multi-instance
		totalCount := 1 // Default
		if node.MultiInstance != nil && node.MultiInstance.LoopCardinality != "" {
			totalCount, err = miHandler.ParseLoopCardinality(node.MultiInstance.LoopCardinality, variables)
			if err != nil {
				log.Printf("Warning: failed to parse loop cardinality: %v", err)
				totalCount = 1
			}
		}

		// Get input collection if specified
		var inputItems []interface{}
		if node.MultiInstance != nil && node.MultiInstance.InputDataItem != "" {
			inputItems, err = miHandler.GetInputCollection(node.MultiInstance.InputDataItem, variables)
			if err == nil && len(inputItems) > 0 {
				totalCount = len(inputItems)
			}
		}

		isSequential := false
		if node.MultiInstance != nil {
			isSequential = node.MultiInstance.IsSequential
		}

		tx, err := r.DB.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// Create multi-instance record
		miExecID := uuid.New()
		_, err = tx.Exec(`
            INSERT INTO public.multi_instance_executions 
            (id, process_instance_id, execution_id, activity_id, is_sequential, total_count, 
             completed_count, status, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, 0, 'active', NOW(), NOW())
        `, miExecID, exec.ProcessInstanceID, exec.ID, node.ID, isSequential, totalCount)
		if err != nil {
			return err
		}

		// Set parent execution to waiting
		err = engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting")
		if err != nil {
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		// Get the created miExec record
		err = r.DB.Get(&miExec, query, exec.ID)
		if err != nil {
			return err
		}

		if isSequential {
			// Sequential: create only first child
			return r.createSequentialChild(ctx, exec.ID, &miExec, node, node.ID, 0, variables, inputItems)
		} else {
			// Parallel: create all children at once
			return r.createParallelChildren(ctx, exec.ID, &miExec, node, node.ID, totalCount, variables, inputItems)
		}
	}
	return r.checkMultiInstanceCompletion(ctx, exec, node, &miExec, variables)
}

// internal/runtime/runtime.go
// In runtime.go - update createSequentialChild
func (r *Runtime) createSequentialChild(ctx context.Context, parentExecID uuid.UUID, miExec *models.MultiInstanceExecution, node *engine.Node, taskNodeID string, loopIndex int, variables map[string]interface{}, inputItems []interface{}) error {
	if loopIndex >= miExec.TotalCount {
		log.Printf("✅ Sequential: All children created, completing multi-instance")
		return r.completeMultiInstance(ctx, parentExecID, miExec, node, variables)
	}

	log.Printf("📝 createSequentialChild: parentExecID=%s, loopIndex=%d, totalCount=%d", parentExecID, loopIndex, miExec.TotalCount)

	// Get element value for this iteration
	var elementValue interface{}
	if len(inputItems) > loopIndex && inputItems[loopIndex] != nil {
		elementValue = inputItems[loopIndex]
	} else {
		elementValue = fmt.Sprintf("%d", loopIndex+1)
	}

	log.Printf("📦 Creating sequential child %d/%d with value: %v", loopIndex+1, miExec.TotalCount, elementValue)

	tx, err := r.DB.Beginx()
	if err != nil {
		log.Printf("❌ Failed to begin transaction: %v", err)
		return err
	}
	defer tx.Rollback()

	// Update loop counter FIRST
	_, err = tx.Exec(`
        UPDATE public.multi_instance_executions 
        SET loop_counter = $1, updated_at = NOW() 
        WHERE id = $2
    `, loopIndex+1, miExec.ID)
	if err != nil {
		log.Printf("❌ Failed to update loop counter: %v", err)
		return err
	}

	// Create child execution
	childExecID := uuid.New()
	_, err = tx.Exec(`
        INSERT INTO public.executions 
        (id, process_instance_id, current_element_id, status, is_active, parent_execution_id, created_at, updated_at)
        VALUES ($1, $2, $3, 'active', true, $4, NOW(), NOW())
    `, childExecID, miExec.ProcessInstanceID, taskNodeID, parentExecID)
	if err != nil {
		log.Printf("❌ Failed to create child execution: %v", err)
		return err
	}

	// Record child relationship
	_, err = tx.Exec(`
        INSERT INTO public.multi_instance_children 
        (parent_execution_id, child_execution_id, loop_index, element_value, status, created_at)
        VALUES ($1, $2, $3, $4, 'active', NOW())
    `, parentExecID, childExecID, loopIndex, fmt.Sprintf("%v", elementValue))
	if err != nil {
		log.Printf("❌ Failed to record child relationship: %v", err)
		return err
	}

	miHandler := NewMultiInstanceHandler(r.DB)

	// Set element variable if specified
	if node.MultiInstance != nil && node.MultiInstance.ElementVariable != "" {
		err = miHandler.SetProcessVariable(tx, miExec.ProcessInstanceID, node.MultiInstance.ElementVariable, elementValue)
		if err != nil {
			log.Printf("⚠️ Warning: failed to set element variable: %v", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Printf("❌ Failed to commit transaction: %v", err)
		return err
	}

	log.Printf("✅ Successfully created child %d/%d with ID: %s", loopIndex+1, miExec.TotalCount, childExecID)

	// Execute child
	rt := NewRuntime(r.Graph, r.DB, r.dispatcher)
	go func() {
		if err := rt.ExecuteExecution(context.Background(), childExecID); err != nil {
			log.Printf("❌ Sequential child %d failed: %v", loopIndex+1, err)
		}
	}()

	return nil
}

// func (r *Runtime) completeMultiInstance(ctx context.Context, parentExecID uuid.UUID, miExec *models.MultiInstanceExecution, node *engine.Node, variables map[string]interface{}) error {
// 	engineRepo := repository.NewEngineRepository(r.DB)

// 	log.Printf("🏁 Completing multi-instance task %s", node.ID)

// 	tx, err := r.DB.Beginx()
// 	if err != nil {
// 		return err
// 	}
// 	defer tx.Rollback()

// 	// Mark multi-instance as completed
// 	_, err = tx.Exec(`
//         UPDATE public.multi_instance_executions
//         SET status = 'completed', updated_at = NOW()
//         WHERE id = $1
//     `, miExec.ID)
// 	if err != nil {
// 		return err
// 	}

// 	// Get next node after the multi-instance task
// 	var nextNodeID string
// 	if len(node.Outgoing) > 0 {
// 		nextNodeID = node.Outgoing[0].TargetRef
// 	}

// 	log.Printf("Parent moving from %s to %s", node.ID, nextNodeID)

// 	// Update parent execution to move to next node
// 	err = engineRepo.UpdateExecutionStatusAndNodeTx(tx, parentExecID, "active", nextNodeID)
// 	if err != nil {
// 		return err
// 	}

// 	if err := tx.Commit(); err != nil {
// 		return err
// 	}

// 	// Resume parent execution
// 	rt := NewRuntime(r.Graph, r.DB)
// 	return rt.ExecuteExecution(ctx, parentExecID)
// }

