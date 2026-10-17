package runtime

import (
	"context"
	"fmt"
	"strings"

	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/feel"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// executeScriptTask runs the script, updates variables, advances to the next node.
// Returns the next node ID so the caller can continue the loop.
func (r *Runtime) executeScriptTask(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, variables map[string]interface{}) (nextID string, err error) {
	if node.Script != nil {
		var scriptResult map[string]interface{}
		var err error
		if node.ScriptFormat != nil && strings.EqualFold(*node.ScriptFormat, "feel") {
			scriptResult, err = evaluateFEELScript(node, variables)
		} else {
			scriptResult, err = executeScript(*node.Script, variables)
		}
		if err != nil {
			return "", fmt.Errorf("script execution failed: %w", err)
		}

		if len(scriptResult) > 0 {
			for k, v := range scriptResult {
				variables[k] = v
			}

			tx, err := r.DB.Beginx()
			if err != nil {
				return "", err
			}
			defer tx.Rollback()

			if err := engineRepo.UpdateProcessVariablesTx(tx, exec.ProcessInstanceID, variables); err != nil {
				return "", err
			}
			if err := tx.Commit(); err != nil {
				return "", err
			}
		}
	}

	next, err := common.ResolveNext(node, variables)
	if err != nil {
		return "", err
	}

	tx, err := r.DB.Beginx()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	if node.CompensationHandler != "" {
		if err := engineRepo.RecordCompensableTx(tx, exec.ProcessInstanceID, exec.ID, node.ID, node.CompensationHandler); err != nil {
			return "", err
		}
	}
	if err := engineRepo.UpdateExecutionNodeTx(tx, exec.ProcessInstanceID, exec.ID, next); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	service.LogAuditErr("execution moved", r.auditService.LogExecutionMoved(exec.ID, exec.ProcessInstanceID, node.ID, next, variables))
	if p := eventlog.Global(); p != nil {
		p.ExecutionMoved(exec.ProcessInstanceID.String(), exec.ID.String(), node.ID, next)
	}

	return next, nil
}

// evaluateFEELScript runs a Camunda 8 script task (zeebe:script, or
// scriptFormat="feel"): its expression is evaluated by the native FEEL
// interpreter — whatever FEEL_MODE is, since script tasks have no legacy
// behavior to preserve — with the task's input mappings visible, and the
// result is written to the result variable. With output mappings, only the
// mapped values reach the process, as in Camunda 8.
func evaluateFEELScript(node *engine.Node, variables map[string]interface{}) (map[string]interface{}, error) {
	scope := variables
	if inputs := common.EvaluateIOMappings(node.InputMappings, variables); len(inputs) > 0 {
		scope = make(map[string]interface{}, len(variables)+len(inputs))
		for k, v := range variables {
			scope[k] = v
		}
		for k, v := range inputs {
			scope[k] = v
		}
	}
	expr := strings.TrimPrefix(strings.TrimSpace(*node.Script), "=")
	value, err := feel.Evaluate(expr, scope)
	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{}
	if node.ResultVariable != "" {
		result[node.ResultVariable] = value
	}
	if len(node.OutputMappings) == 0 {
		return result, nil
	}
	withResult := make(map[string]interface{}, len(scope)+len(result))
	for k, v := range scope {
		withResult[k] = v
	}
	for k, v := range result {
		withResult[k] = v
	}
	return common.EvaluateIOMappings(node.OutputMappings, withResult), nil
}
