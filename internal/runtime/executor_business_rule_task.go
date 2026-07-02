package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/dmn"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
)

// executeBusinessRuleTask evaluates the DMN decision referenced by
// node.DecisionKey (zeebe:calledDecision), stores the result under
// node.ResultVariable, and advances to the next node — synchronous, like a
// script task.
func (r *Runtime) executeBusinessRuleTask(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, variables map[string]interface{}) (nextID string, err error) {
	if node.DecisionKey == "" {
		return "", fmt.Errorf("business rule task %s has no decision key (zeebe:calledDecision)", node.ID)
	}

	dmnRepo := repository.NewDMNRepository(r.DB)
	decision, err := dmnRepo.FindLatestDecisionByKey(node.DecisionKey)
	if err != nil {
		return "", fmt.Errorf("decision %q not found: %w", node.DecisionKey, err)
	}

	var parsed dmn.Decision
	if err := json.Unmarshal(decision.ParsedTable, &parsed); err != nil {
		return "", fmt.Errorf("failed to parse decision %q: %w", node.DecisionKey, err)
	}
	if parsed.DecisionTable == nil {
		return "", fmt.Errorf("decision %q has no decision table", node.DecisionKey)
	}

	// zeebe:ioMapping inputs are local to the activity (not persisted to
	// process scope) — they only shape what the decision table itself sees.
	evalVars := variables
	if mapped := common.EvaluateIOMappings(node.InputMappings, variables); len(mapped) > 0 {
		evalVars = make(map[string]interface{}, len(variables)+len(mapped))
		for k, v := range variables {
			evalVars[k] = v
		}
		for k, v := range mapped {
			evalVars[k] = v
		}
	}

	result, err := dmn.EvaluateDecisionTable(parsed.DecisionTable, evalVars)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate decision %q: %w", node.DecisionKey, err)
	}

	resultVar := node.ResultVariable
	if resultVar == "" {
		resultVar = "result"
	}
	variables[resultVar] = result

	tx, err := r.DB.Beginx()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	if err := engineRepo.UpdateProcessVariablesTx(tx, exec.ProcessInstanceID, variables); err != nil {
		return "", err
	}

	next, err := common.ResolveNext(node, variables)
	if err != nil {
		return "", err
	}
	if err := engineRepo.UpdateExecutionNodeTx(tx, exec.ID, next); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}

	return next, nil
}
