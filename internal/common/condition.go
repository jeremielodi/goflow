// internal/common/condition.go
package common

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/jeremielodi/goflow/internal/engine"
)

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

// ResolveNext resolves the next node based on conditions
func ResolveNext(node *engine.Node, variables map[string]interface{}) (string, error) {
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

// ResolveExpression resolves expression strings like ${variableName}
func ResolveExpression(expr string, vars map[string]interface{}) string {
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