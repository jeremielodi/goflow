package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/feel"
	"github.com/jeremielodi/goflow/internal/repository"
)

// executeCallActivity starts a child process instance and parks the parent execution.
func (r *Runtime) executeCallActivity(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, variables map[string]interface{}) error {
	calledID := strings.TrimSpace(node.CalledElement)
	if strings.HasPrefix(calledID, "=") {
		// zeebe:calledElement processId given as a FEEL expression.
		v, err := feel.Evaluate(trimFEEL(calledID), variables)
		if err != nil {
			return fmt.Errorf("callActivity %s: processId %q: %w", node.ID, calledID, err)
		}
		s, ok := v.(string)
		if !ok || s == "" {
			return fmt.Errorf("callActivity %s: processId %q is %v, not a process id", node.ID, calledID, v)
		}
		calledID = s
	}
	if calledID == "" {
		return fmt.Errorf("callActivity %s has no calledElement", node.ID)
	}

	// Find the called process definition
	procDef, err := engineRepo.FindLatestProcessDefinitionByKey(calledID)
	if err != nil {
		return fmt.Errorf("callActivity: cannot find process '%s': %w", calledID, err)
	}

	// Build the child process graph
	childGraph, err := engine.BuildGraphForProcess([]byte(procDef.BpmnXML), calledID)
	if err != nil {
		return fmt.Errorf("callActivity: build child graph: %w", err)
	}

	// Find start node in child graph
	var startNodeID string
	for _, n := range childGraph.Nodes {
		if n.IsProcessStart() || n.Type == engine.MessageStartEventType {
			startNodeID = n.ID
			break
		}
	}
	if startNodeID == "" {
		return fmt.Errorf("callActivity: no start event in called process '%s'", node.CalledElement)
	}

	now := time.Now()
	childInstanceID := uuid.New()
	childExecID := uuid.New()

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("callActivity: begin tx: %w", err)
	}
	defer tx.Rollback()

	// Create child process instance with parent tracking
	_, err = tx.Exec(`
		INSERT INTO process_instances
		  (id, process_definition_id, status, started_at, parent_instance_id, parent_execution_id)
		VALUES ($1, $2, 'running', $3, $4, $5)
	`, childInstanceID, procDef.ID, now, exec.ProcessInstanceID, exec.ID)
	if err != nil {
		return fmt.Errorf("callActivity: create child instance: %w", err)
	}

	// The child's variables: every variable the call activity sees (unless
	// propagateAllParentVariables is off), plus its input mappings — which
	// are also the call activity's local variables, as in Camunda 8.
	childVars := map[string]interface{}{}
	if !node.NoParentVariables {
		for k, v := range variables {
			childVars[k] = v
		}
	}
	if inputs := common.EvaluateIOMappings(node.InputMappings, variables); len(inputs) > 0 {
		for k, v := range inputs {
			childVars[k] = v
		}
		if err := repository.SetLocalVariablesTx(tx, exec.ProcessInstanceID, exec.ID, inputs); err != nil {
			return fmt.Errorf("callActivity: input mappings: %w", err)
		}
	}
	if err := engineRepo.CreateVariablesTx(tx, childInstanceID, childVars, now); err != nil {
		return fmt.Errorf("callActivity: copy variables: %w", err)
	}

	// Create child execution at start node
	_, err = tx.Exec(`
		INSERT INTO executions
		  (id, process_instance_id, current_element_id, status, is_active, created_at, updated_at)
		VALUES ($1, $2, $3, 'active', true, $4, $4)
	`, childExecID, childInstanceID, startNodeID, now)
	if err != nil {
		return fmt.Errorf("callActivity: create child execution: %w", err)
	}

	// Park parent execution (waiting for child to complete)
	_, err = tx.Exec(`
		UPDATE executions SET status = 'waiting', updated_at = NOW() WHERE process_instance_id = $1 AND id = $2
	`, exec.ProcessInstanceID, exec.ID)
	if err != nil {
		return fmt.Errorf("callActivity: park parent: %w", err)
	}

	parentGraph, err := engineRepo.GetProcessGraphByInstanceID(exec.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("callActivity: load process graph: %w", err)
	}
	if err := r.armBoundaryEventsTx(tx, engineRepo, parentGraph, exec, node); err != nil {
		return fmt.Errorf("callActivity: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("callActivity: commit: %w", err)
	}

	// Execute the child process (synchronously within this goroutine)
	childRT := NewRuntime(childGraph, r.DB, r.dispatcher)
	return childRT.ExecuteExecution(ctx, childExecID)
}
