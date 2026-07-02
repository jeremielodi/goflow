// internal/common/condition.go
package common

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/jeremielodi/goflow/internal/engine"
)

var (
	// Compilation cache to avoid re-compiling same expressions
	compilationCache     = make(map[string]*cachedProgram)
	compilationCacheLock sync.RWMutex

	// Simple CEL environment - NO custom functions
	commonEnvOptions = []cel.EnvOption{
		cel.HomogeneousAggregateLiterals(),
		cel.EagerlyValidateDeclarations(true),
	}
)

type cachedProgram struct {
	program cel.Program
	env     *cel.Env
	expr    string
}

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

	normalized, prg, err := compileExpression(expr, variables)
	if err != nil {
		return false, err
	}

	return executeProgram(prg, variables, normalized)
}

// EvaluateValue evaluates a FEEL/CEL expression (or Camunda 7 ${...} wrapper)
// and returns its raw value, rather than requiring a boolean result. Used for
// assignee/candidateGroups expressions and zeebe:ioMapping source expressions.
//
// A value is only treated as an expression when it looks like one (FEEL "="
// prefix or Camunda 7 "${...}" wrapper) — otherwise it's returned unchanged,
// since attributes like candidateGroups="sales,marketing" are plain literals,
// not variable references. A blank expression returns nil.
func EvaluateValue(expr string, variables map[string]interface{}) (interface{}, error) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil, nil
	}
	isExpression := strings.HasPrefix(trimmed, "=") ||
		(strings.HasPrefix(trimmed, "${") && strings.HasSuffix(trimmed, "}"))
	if !isExpression {
		return trimmed, nil
	}

	normalized, prg, err := compileExpression(trimmed, variables)
	if err != nil {
		return nil, err
	}

	out, _, err := prg.Eval(variables)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation error for '%s': %w", normalized, err)
	}
	return out.Value(), nil
}

// compileExpression normalizes expr to CEL and returns a compiled (and
// cached) program ready to Eval against variables.
func compileExpression(expr string, variables map[string]interface{}) (string, cel.Program, error) {
	normalized, err := normalizeExpression(expr)
	if err != nil {
		return normalized, nil, err
	}

	cacheKey := generateCacheKey(normalized, variables)
	if cached := getCachedProgram(cacheKey); cached != nil {
		return normalized, cached.program, nil
	}

	envOptions := make([]cel.EnvOption, 0, len(commonEnvOptions)+len(variables))
	envOptions = append(envOptions, commonEnvOptions...)
	for variableName := range variables {
		envOptions = append(envOptions, cel.Variable(variableName, cel.DynType))
	}

	env, err := cel.NewEnv(envOptions...)
	if err != nil {
		return normalized, nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	ast, issues := env.Compile(normalized)
	if issues != nil && issues.Err() != nil {
		return normalized, nil, fmt.Errorf("expression compilation error for '%s': %w", normalized, issues.Err())
	}

	prg, err := env.Program(ast)
	if err != nil {
		return normalized, nil, fmt.Errorf("failed to create CEL program: %w", err)
	}

	setCachedProgram(cacheKey, &cachedProgram{program: prg, env: env, expr: normalized})
	return normalized, prg, nil
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
	// Convert single quotes to double quotes for strings
	expr = regexp.MustCompile(`'([^']*)'`).ReplaceAllString(expr, `"$1"`)

	// Logical operators
	expr = strings.ReplaceAll(expr, " and ", " && ")
	expr = strings.ReplaceAll(expr, " AND ", " && ")
	expr = strings.ReplaceAll(expr, " or ", " || ")
	expr = strings.ReplaceAll(expr, " OR ", " || ")

	// Equality operators
	expr = strings.ReplaceAll(expr, " = ", " == ")
	expr = strings.ReplaceAll(expr, " != ", " != ")
	expr = strings.ReplaceAll(expr, " <> ", " != ")

	// NOT operator
	expr = strings.ReplaceAll(expr, " not ", " !")
	expr = strings.ReplaceAll(expr, " NOT ", " !")
	expr = strings.ReplaceAll(expr, "not(", "!(")
	expr = strings.ReplaceAll(expr, "NOT(", "!(")

	// Null checks
	expr = strings.ReplaceAll(expr, " is null", " == null")
	expr = strings.ReplaceAll(expr, " IS NULL", " == null")
	expr = strings.ReplaceAll(expr, " is not null", " != null")
	expr = strings.ReplaceAll(expr, " IS NOT NULL", " != null")

	// Clean up multiple spaces
	expr = regexp.MustCompile(`\s+`).ReplaceAllString(expr, " ")

	return strings.TrimSpace(expr)
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

	if len(compilationCache) > 1000 {
		for k := range compilationCache {
			delete(compilationCache, k)
			break
		}
	}
	compilationCache[key] = prog
}

// ResolveNext resolves the next node based on node type and conditions
func ResolveNext(node *engine.Node, variables map[string]interface{}) (string, error) {
	if len(node.Outgoing) == 0 {
		return "", fmt.Errorf("node %s has no outgoing flows", node.ID)
	}

	// For UserTask and ServiceTask, filter out boundary event flows
	if node.Type == engine.UserTaskType || node.Type == engine.ServiceTaskType {
		var normalFlows []engine.Flow
		for _, flow := range node.Outgoing {
			if flow.IsBoundary {
				continue
			}
			normalFlows = append(normalFlows, flow)
		}

		if len(normalFlows) == 0 {
			return "", fmt.Errorf("task %s has no normal outgoing flows (only boundary events)", node.ID)
		}

		if len(normalFlows) > 1 {
			return "", fmt.Errorf("task %s has multiple outgoing flows (%d)", node.ID, len(normalFlows))
		}

		return normalFlows[0].TargetRef, nil
	}

	// For ExclusiveGateway, evaluate conditions
	if node.Type == engine.ExclusiveGatewayType {
		var selected *engine.Flow
		for _, flow := range node.Outgoing {
			condition := strings.TrimSpace(flow.Condition)

			// Empty condition = always true
			if condition == "" {
				if selected != nil {
					return "", fmt.Errorf("multiple conditions true on exclusive gateway: flow %s and %s both have no condition", selected.ID, flow.ID)
				}
				selected = &flow
				continue
			}

			ok, err := EvaluateCondition(condition, variables)
			if err != nil {
				return "", fmt.Errorf("condition evaluation failed for flow %s: %w", flow.ID, err)
			}
			if ok {
				if selected != nil {
					return "", fmt.Errorf("multiple conditions true on exclusive gateway: flow %s and %s", selected.ID, flow.ID)
				}
				selected = &flow
			}
		}

		if selected == nil {
			return "", fmt.Errorf("no matching condition on exclusive gateway")
		}
		return selected.TargetRef, nil
	}

	// For ParallelGateway, return first outgoing
	if node.Type == engine.ParallelGatewayType {
		if len(node.Outgoing) == 0 {
			return "", fmt.Errorf("parallel gateway %s has no outgoing flows", node.ID)
		}
		return node.Outgoing[0].TargetRef, nil
	}

	// Default: single outgoing flow expected
	if len(node.Outgoing) > 1 {
		return "", fmt.Errorf("node %s of type %s has multiple outgoing flows (%d), expected 1", node.ID, node.Type, len(node.Outgoing))
	}
	return node.Outgoing[0].TargetRef, nil
}

// ResolveExpression resolves expression strings like ${variableName}
func ResolveExpression(expr string, vars map[string]interface{}) string {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "${") && strings.HasSuffix(expr, "}") {
		key := expr[2 : len(expr)-1]
		if val, ok := vars[key]; ok {
			return fmt.Sprintf("%v", val)
		}
		return ""
	}
	return expr
}

// ClearConditionCache clears the compilation cache
func ClearConditionCache() {
	compilationCacheLock.Lock()
	defer compilationCacheLock.Unlock()
	compilationCache = make(map[string]*cachedProgram)
}
