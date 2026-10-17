package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/eventlog"
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
	variables, err := repository.VariablesForExecution(r.DB, exec.ProcessInstanceID, repository.VariableScopeOf(r.DB, exec.ProcessInstanceID, exec.ID))
	if err != nil {
		return fmt.Errorf("failed to load variables: %w", err)
	}

	// Get process graph
	graph, err := engineRepo.GetProcessGraphByInstanceID(exec.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("failed to load process graph: %w", err)
	}

	inner, err := lookupInnerInstance(r.DB, exec.ProcessInstanceID, exec.ID)
	if err != nil {
		return err
	}

	currentID := exec.CurrentElementID
	for {
		// An inner instance of a multi-instance activity that has left the
		// activity — whatever kind it is — has finished its iteration.
		if inner != nil && currentID != inner.ActivityID {
			return r.completeInnerInstance(ctx, engineRepo, exec, inner, graph)
		}
		// Left an activity without an outgoing flow: the token ends in the
		// activity's scope, as at an end event there.
		if strings.HasPrefix(currentID, engine.ImplicitEndPrefix) {
			activity, ok := graph.Nodes[strings.TrimPrefix(currentID, engine.ImplicitEndPrefix)]
			if !ok {
				return fmt.Errorf("implicit end of unknown element %s", currentID)
			}
			exec.CurrentElementID = currentID
			end := &engine.Node{ID: currentID, Type: engine.EndEventType, ParentScope: activity.ParentScope}
			return r.executeEnd(ctx, engineRepo, exec, end, graph, variables)
		}

		node, err := engineRepo.GetNodeByID(graph, currentID)
		if err != nil {
			return err
		}

		// Reaching a multi-instance activity: this execution becomes its
		// body and runs the inner instances.
		if node.MultiInstance != nil && (inner == nil || inner.ActivityID != node.ID) {
			return r.startMultiInstanceBody(ctx, engineRepo, exec, node, graph, variables)
		}

		switch node.Type {
		case engine.StartEventType, engine.StartTimerEventType, engine.ExclusiveGatewayType:
			if node.Type != engine.ExclusiveGatewayType && node.ParentScope == "" {
				if err := r.armProcessEventSubProcesses(ctx, engineRepo, graph, exec); err != nil {
					return err
				}
			}
			next, err := common.ResolveNext(node, variables)
			if err != nil {
				return err
			}

			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}

			if err = engineRepo.UpdateExecutionNodeTx(tx, exec.ProcessInstanceID, exec.ID, next); err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to update execution node: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			fromNode := currentID
			currentID = next
			service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(exec.ID, exec.ProcessInstanceID, fromNode, next, variables))
			if p := eventlog.Global(); p != nil {
				p.ExecutionMoved(exec.ProcessInstanceID.String(), exec.ID.String(), fromNode, next)
			}

		case engine.ServiceTaskType:
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

		case engine.BusinessRuleTaskType:
			next, err := r.executeBusinessRuleTask(ctx, engineRepo, exec, node, variables)
			if err != nil {
				return err
			}
			currentID = next
			continue

		case engine.IntermediateCompensationThrowEventType:
			carryOn, err := r.throwCompensation(ctx, engineRepo, exec, node, graph)
			if err != nil || !carryOn {
				return err
			}
			next, err := r.advance(ctx, engineRepo, exec, node, variables)
			if err != nil {
				return err
			}
			currentID = next

		case engine.CompensationEndEventType:
			carryOn, err := r.throwCompensation(ctx, engineRepo, exec, node, graph)
			if err != nil || !carryOn {
				return err
			}
			return r.executeEnd(ctx, engineRepo, exec, node, graph, variables)

		case engine.CompensationHandlerEndType:
			return r.executeCompensationHandlerEnd(ctx, engineRepo, exec, node)

		case engine.IntermediateConditionalCatchEventType:
			passed, err := r.executeConditionalCatch(ctx, engineRepo, exec, node, variables)
			if err != nil || !passed {
				return err
			}
			next, err := r.advance(ctx, engineRepo, exec, node, variables)
			if err != nil {
				return err
			}
			currentID = next

		case engine.BoundaryTimerEventType, engine.MessageBoundaryEventType, engine.SignalBoundaryEventType, engine.ConditionalBoundaryEventType:
			// A boundary trigger whose event came, or a token already
			// leaving through the boundary.
			if _, isTrigger := repository.ParseBoundaryTriggerPathID(exec.PathID); isTrigger && currentID == exec.CurrentElementID {
				return r.fireBoundary(ctx, engineRepo, exec, node, graph)
			}
			if node.Type == engine.BoundaryTimerEventType {
				next, err := r.executeBoundaryTimerNode(ctx, engineRepo, exec, node)
				if err != nil {
					return err
				}
				currentID = next
				continue
			}
			next, err := r.advance(ctx, engineRepo, exec, node, variables)
			if err != nil {
				return err
			}
			currentID = next

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
				if p := eventlog.Global(); p != nil {
					p.GatewayForked(exec.ProcessInstanceID.String(), node.ID, int32(len(node.Outgoing)))
				}

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
				if p := eventlog.Global(); p != nil {
					p.GatewayJoined(exec.ProcessInstanceID.String(), node.ID, int32(expectedCount), int32(receivedCount))
				}

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
				if p := eventlog.Global(); p != nil {
					p.GatewayForked(exec.ProcessInstanceID.String(), node.ID, int32(len(node.Outgoing)))
				}

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
			if p := eventlog.Global(); p != nil {
				p.GatewayJoined(exec.ProcessInstanceID.String(), node.ID, int32(finalState.ExpectedIncoming), int32(finalState.ReceivedIncoming))
			}

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
			return r.executeEnd(ctx, engineRepo, exec, node, graph, variables)

		case engine.SignalEndEventType:
			r.broadcastSignal(node)
			return r.executeEnd(ctx, engineRepo, exec, node, graph, variables)

		case engine.TerminateEndEventType:
			return r.executeTerminateEnd(ctx, engineRepo, exec, node, graph, variables)

		case engine.ErrorEndEventType:
			return r.executeErrorEnd(ctx, engineRepo, exec, node, graph)

		case engine.SubProcessType:
			return r.executeSubProcess(ctx, engineRepo, exec, node, graph, variables)

		case engine.EventSubProcessType:
			return r.executeEventSubProcess(ctx, engineRepo, exec, node)

		case engine.IntermediateEscalationThrowEventType:
			carryOn, err := r.throwEscalation(ctx, engineRepo, exec, node, graph)
			if err != nil || !carryOn {
				return err
			}
			next, err := r.advance(ctx, engineRepo, exec, node, variables)
			if err != nil {
				return err
			}
			currentID = next

		case engine.EscalationEndEventType:
			carryOn, err := r.throwEscalation(ctx, engineRepo, exec, node, graph)
			if err != nil || !carryOn {
				return err
			}
			return r.executeEnd(ctx, engineRepo, exec, node, graph, variables)

		case engine.ManualTaskType, engine.IntermediateThrowEventType, engine.LinkCatchEventType, engine.EscalationBoundaryEventType:
			next, err := r.advance(ctx, engineRepo, exec, node, variables)
			if err != nil {
				return err
			}
			currentID = next

		case engine.IntermediateSignalThrowEventType:
			r.broadcastSignal(node)
			next, err := r.advance(ctx, engineRepo, exec, node, variables)
			if err != nil {
				return err
			}
			currentID = next

		case engine.IntermediateMessageCatchEventType:
			return r.executeMessageCatchEvent(ctx, engineRepo, exec, node)

		case engine.IntermediateSignalCatchEventType:
			return r.executeSignalCatchEvent(ctx, engineRepo, exec, node)

		case engine.MessageStartEventType, engine.SignalStartEventType:
			// Message start event: advance past it immediately (started via message endpoint)
			if node.ParentScope == "" {
				if err := r.armProcessEventSubProcesses(ctx, engineRepo, graph, exec); err != nil {
					return err
				}
			}
			next, err := common.ResolveNext(node, variables)
			if err != nil {
				return err
			}
			tx, err := r.DB.BeginTxx(ctx, nil)
			if err != nil {
				return err
			}
			if err = engineRepo.UpdateExecutionNodeTx(tx, exec.ProcessInstanceID, exec.ID, next); err != nil {
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
	value, err := common.EvaluateValue(expr, vars)
	if err != nil || value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}
