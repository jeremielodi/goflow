package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
}

func NewRuntime(Graph *engine.ProcessGraph, DB *sqlx.DB) *Runtime {
	return &Runtime{
		Graph:        Graph,
		DB:           DB,
		auditService: service.NewAuditService(DB),
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
			next, err := common.ResolveNext(node, variables)
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

			r.auditService.LogExecutionMoved(exec.ID, exec.ProcessInstanceID, currentID, next, variables)

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

			r.auditService.LogJobCreated(exec.ProcessInstanceID, jobID, *node.JobType)
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

			// ✅ CHECK FOR BOUNDARY TIMER EVENT
			var attachedTimer *engine.Node
			for _, n := range r.Graph.Nodes {
				if n.Type == engine.BoundaryTimerEventType && n.AttachedToRef == node.ID {
					attachedTimer = n
					break
				}
			}

			// If there's a boundary timer, schedule it
			if attachedTimer != nil && attachedTimer.TimerDefinition != "" {
				timerDuration := parseTimerDuration(attachedTimer.TimerDefinition)
				dueAt := time.Now().Add(timerDuration)

				log.Printf("⏰ [CREATE TIMER] Task: %s, Duration: %v, DueAt: %s",
					node.ID, timerDuration, dueAt.Format("15:04:05.000"))

				// Get the next node after the boundary event
				var nextNodeID string
				if len(attachedTimer.Outgoing) > 0 {
					nextNodeID = attachedTimer.Outgoing[0].TargetRef
				}

				timerID := uuid.New()
				err = engineRepo.CreateTimerTx(tx, timerID, exec.ProcessInstanceID, &exec.ID, nextNodeID, dueAt, "boundary")
				if err != nil {
					return fmt.Errorf("failed to create timer: %w", err)
				}

				r.auditService.LogTimerCreated(exec.ProcessInstanceID, &exec.ID, dueAt, "boundary")
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
			r.auditService.LogTaskCreated(taskID, exec.ProcessInstanceID, node.Name, assignee, candidateGroup)
			return nil

		case engine.BoundaryTimerEventType:
			// This node is reached when a timer fires
			// Move to the target node (Task_Escalate)
			if len(node.Outgoing) == 0 {
				return fmt.Errorf("boundary timer event %s has no outgoing flow", node.ID)
			}
			nextNodeID := node.Outgoing[0].TargetRef

			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback()

			// Update execution to next node
			err = engineRepo.UpdateExecutionNodeTx(tx, exec.ID, nextNodeID)
			if err != nil {
				return fmt.Errorf("failed to update execution node: %w", err)
			}

			// Update execution status to active
			err = engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "active")
			if err != nil {
				return fmt.Errorf("failed to update execution status: %w", err)
			}

			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			currentID = nextNodeID
			continue

			// For intermediate timer events (standalone)
		case engine.IntermediateCatchEventType:
			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback()

			if node.TimerDefinition != "" {
				timerDuration := parseTimerDuration(node.TimerDefinition)
				dueAt := time.Now().Add(timerDuration)

				var nextNodeID string
				if len(node.Outgoing) > 0 {
					nextNodeID = node.Outgoing[0].TargetRef
				}

				timerID := uuid.New()
				err = engineRepo.CreateTimerTx(tx, timerID, exec.ProcessInstanceID, &exec.ID, nextNodeID, dueAt, "intermediate")
				if err != nil {
					return fmt.Errorf("failed to create timer: %w", err)
				}

				// Move execution to waiting state
				err = engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting")
				if err != nil {
					return fmt.Errorf("failed to update execution status: %w", err)
				}

				r.auditService.LogTimerCreated(exec.ProcessInstanceID, &exec.ID, dueAt, "intermediate")
				return nil
			}
		case engine.ParallelGatewayType:
			// Create parallel gateway executor
			parallelExecutor := NewParallelGatewayExecutor(r.DB)

			// Check if this is a fork or join
			if r.isForkGateway(node, exec) {
				// Fork: Create child executions
				err := parallelExecutor.ExecuteFork(ctx, exec.ID, node, variables)
				if err != nil {
					return fmt.Errorf("failed to execute parallel fork: %w", err)
				}
				// Log gateway fork
				r.auditService.LogGatewayForked(exec.ProcessInstanceID, node.ID, len(node.Outgoing))
				return nil // Parent waits, children continue
			} else {
				// For join, we need to get the expected count from the gateway state
				tx, err := r.DB.BeginTxx(ctx, nil)
				if err != nil {
					return fmt.Errorf("failed to begin transaction: %w", err)
				}

				// Get the gateway state to know expected count
				var gatewayState struct {
					ExpectedIncoming int
					ReceivedIncoming int
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
					ReceivedIncoming int
				}
				err = r.DB.Get(&finalState, `
			SELECT received_incoming
			FROM public.gateway_state
			WHERE process_instance_id = $1 AND gateway_id = $2
			ORDER BY created_at DESC
			LIMIT 1
		`, exec.ProcessInstanceID, node.ID)

				if err == nil {
					receivedCount = finalState.ReceivedIncoming
				}

				// Log gateway join
				r.auditService.LogGatewayJoined(exec.ProcessInstanceID, node.ID, expectedCount, receivedCount)
				return nil
			}
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
		engineRepo := repository.NewEngineRepository(r.DB)
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

// ------------------------------------------------------------
// Timer Worker - Call this from main.go to start timer processing
// ------------------------------------------------------------

// StartTimerWorker starts a background goroutine that processes due timers
func StartTimerWorker(db *sqlx.DB, stopCh <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			processDueTimers(db)
		}
	}
}

func processDueTimers(db *sqlx.DB) {
	ctx := context.Background()
	engineRepo := repository.NewEngineRepository(db)

	// Get timers that need to fire
	timers, err := engineRepo.GetDueTimers(ctx, time.Now())
	if err != nil {
		return
	}

	for _, timer := range timers {
		// Parse payload to get target node ID
		var payload map[string]interface{}
		if len(timer.Payload) > 0 {
			if err := json.Unmarshal(timer.Payload, &payload); err != nil {
				continue
			}
		}

		targetNodeID, ok := payload["target_node_id"].(string)
		if !ok {
			continue
		}

		// Mark timer as fired
		err = engineRepo.MarkTimerFired(ctx, timer.ID)
		if err != nil {
			continue
		}

		// Get the process graph
		graph, err := engineRepo.GetProcessGraphByInstanceID(timer.ProcessInstanceID)
		if err != nil {
			continue
		}

		// Get the execution
		if timer.ExecutionID == nil {
			continue
		}
		exec, err := engineRepo.GetExecutionByID(*timer.ExecutionID)
		if err != nil {
			continue
		}

		// Start transaction
		tx, err := db.Beginx()
		if err != nil {
			continue
		}

		// Cancel the user task (mark as canceled due to timer)
		err = engineRepo.CancelUserTaskByExecutionTx(tx, exec.ID)
		if err != nil {
			tx.Rollback()
			continue
		}

		// Move execution to the target node (boundary event)
		err = engineRepo.UpdateExecutionNodeTx(tx, exec.ID, targetNodeID)
		if err != nil {
			tx.Rollback()
			continue
		}

		// Update execution status to active
		err = engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "active")
		if err != nil {
			tx.Rollback()
			continue
		}

		if err := tx.Commit(); err != nil {
			continue
		}

		// Resume execution
		rt := NewRuntime(graph, db)
		go rt.ExecuteExecution(ctx, exec.ID)
	}
}
