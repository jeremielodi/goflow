package engine

import (
	"fmt"

	"github.com/google/cel-go/cel"
)

type CELEvaluator struct {
	env *cel.Env
}

func NewCELEvaluator() (*CELEvaluator, error) {
	env, err := cel.NewEnv()

	if err != nil {
		return nil, err
	}

	return &CELEvaluator{env: env}, nil
}

func convertVars(vars map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{})

	for k, v := range vars {
		switch val := v.(type) {
		case int:
			out[k] = float64(val)
		default:
			out[k] = val
		}
	}

	return out
}
func (e *CELEvaluator) Evaluate(expression string, vars map[string]interface{}) (interface{}, error) {

	ast, issues := e.env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compile error: %w", issues.Err())
	}

	prg, err := e.env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("program error: %w", err)
	}

	out, _, err := prg.Eval(convertVars(vars))
	if err != nil {
		return nil, fmt.Errorf("eval error: %w", err)
	}

	return out.Value(), nil
}
