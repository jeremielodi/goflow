package runtime

import (
	"context"
	"fmt"

	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
)

// executeScriptTask runs the script, updates variables, advances to the next node.
// Returns the next node ID so the caller can continue the loop.
func (r *Runtime) executeScriptTask(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, variables map[string]interface{}) (nextID string, err error) {
	if node.Script != nil {
		scriptResult, err := executeScript(*node.Script, variables)
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

	if err := engineRepo.UpdateExecutionNodeTx(tx, exec.ID, next); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}

	return next, nil
}
