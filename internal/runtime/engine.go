package runtime

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/jeremielodi/goflow/internal/engine"
)

type Runtime struct {
	Graph *engine.ProcessGraph
}

// EvaluateCondition translates a BPMN condition (FEEL) into CEL and evaluates it.
func EvaluateCondition(expr string, variables map[string]interface{}) (bool, error) {
	if expr == "" {
		return true, nil // no condition = always true
	}

	// 1. Normalize common BPMN syntax
	expr = strings.TrimSpace(expr)
	// Remove surrounding quotes if present (some modelers add them)
	if len(expr) > 1 && ((expr[0] == '"' && expr[len(expr)-1] == '"') ||
		(expr[0] == '\'' && expr[len(expr)-1] == '\'')) {
		expr = expr[1 : len(expr)-1]
	}
	// Remove leading "=" (sometimes used in JUEL)
	expr = strings.TrimPrefix(expr, "=")
	// Convert FEEL operators to CEL
	expr = convertFEELToCEL(expr)

	// 2. Build CEL environment with variables extracted from the map
	envOpts := []cel.EnvOption{}
	for varName := range variables {
		// Declare each variable as Dyn (dynamic type) – CEL will infer
		envOpts = append(envOpts, cel.Variable(varName, cel.DynType))
	}
	env, err := cel.NewEnv(envOpts...)
	if err != nil {
		return false, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	// 3. Compile expression
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf("compilation error: %w", issues.Err())
	}

	// 4. Create program
	prg, err := env.Program(ast)
	if err != nil {
		return false, fmt.Errorf("program creation failed: %w", err)
	}

	// 5. Evaluate
	out, _, err := prg.Eval(variables)
	if err != nil {
		return false, fmt.Errorf("evaluation error: %w", err)
	}

	// 6. Convert result
	val, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("condition did not return a boolean (got %T)", out.Value())
	}
	return val, nil
}

// convertFEELToCEL replaces FEEL‑style operators with CEL equivalents.
func convertFEELToCEL(expr string) string {
	// Note: simple string replacement; for full FEEL you'd need a parser.
	replacer := strings.NewReplacer(
		" and ", " && ",
		" or ", " || ",
		" = ", " == ",
		" not ", " !",
	)
	return replacer.Replace(expr)
}

// Execute starts or resumes process execution from the given node ID.
// For UserTasks it returns nil after printing – caller must persist state.
func (r *Runtime) Execute(startID string, variables map[string]interface{}) (string, error) {
	currentID := startID

	for {
		node, ok := r.Graph.Nodes[currentID]
		if !ok {
			return "", fmt.Errorf("node %s does not exist", currentID)
		}

		fmt.Printf("Executing: %s (%s)\n", node.ID, node.Type)

		switch node.Type {

		case engine.StartEventType, engine.ServiceTaskType:
			// automatic execution
			next, err := r.resolveNext(node, variables)
			if err != nil {
				return "", err
			}
			currentID = next

		case engine.UserTaskType:
			// 🚨 THIS is your WAITING POINT
			fmt.Printf("UserTask '%s' – waiting for human action\n", node.Name)

			// return taskID so caller can resume later
			return node.ID, nil

		case engine.EndEventType:
			fmt.Println("Process completed.")
			return "", nil

		default:
			return "", fmt.Errorf("unsupported node type: %s", node.Type)
		}
	}
}

func (r *Runtime) NextAfter(nodeID string, variables map[string]interface{}) (string, error) {
	node, ok := r.Graph.Nodes[nodeID]
	if !ok {
		return "", fmt.Errorf("node not found: %s", nodeID)
	}

	// UserTask must move forward using outgoing flow
	if len(node.Outgoing) == 0 {
		return "", fmt.Errorf("user task has no outgoing flow: %s", nodeID)
	}

	return node.Outgoing[0].TargetRef, nil
}
func (r *Runtime) ResolveNext(node *engine.Node, variables map[string]interface{}) (string, error) {
	return r.resolveNext(node, variables)
}

// resolveNext determines the next node ID based on outgoing flows.
func (r *Runtime) resolveNext(node *engine.Node, variables map[string]interface{}) (string, error) {
	if len(node.Outgoing) == 0 {
		return "", fmt.Errorf("node %s has no outgoing flows", node.ID)
	}
	if len(node.Outgoing) == 1 {
		return node.Outgoing[0].TargetRef, nil
	}

	// Exclusive gateway – evaluate conditions in order
	var matchedFlow *engine.Flow
	for _, flow := range node.Outgoing {
		condOk, err := EvaluateCondition(flow.Condition, variables)
		if err != nil {
			return "", fmt.Errorf("condition on flow %s failed: %w", flow.ID, err)
		}
		if condOk {
			if matchedFlow != nil {
				return "", fmt.Errorf("multiple conditions true on exclusive gateway %s", node.ID)
			}
			matchedFlow = &flow
		}
	}
	if matchedFlow == nil {
		return "", fmt.Errorf("no condition true on exclusive gateway %s", node.ID)
	}
	return matchedFlow.TargetRef, nil
}
