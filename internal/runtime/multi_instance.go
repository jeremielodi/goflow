// internal/runtime/multi_instance.go
package runtime

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type MultiInstanceHandler struct {
	db *sqlx.DB
}

func NewMultiInstanceHandler(db *sqlx.DB) *MultiInstanceHandler {
	return &MultiInstanceHandler{db: db}
}

// ParseLoopCardinality evaluates the loop cardinality expression
func (h *MultiInstanceHandler) ParseLoopCardinality(cardinalityExpr string, variables map[string]interface{}) (int, error) {
	if cardinalityExpr == "" {
		return 0, fmt.Errorf("loop cardinality is required")
	}

	// Try to parse as integer
	var count int
	if _, err := fmt.Sscanf(cardinalityExpr, "%d", &count); err == nil {
		return count, nil
	}

	// Try to evaluate as expression
	result, err := EvaluateCondition(cardinalityExpr, variables)
	if err == nil {
		if result {
			return 1, nil
		}
		return 0, nil
	}

	return 0, fmt.Errorf("failed to parse loop cardinality: %s", cardinalityExpr)
}

// GetInputCollection gets the input collection variable
func (h *MultiInstanceHandler) GetInputCollection(collectionName string, variables map[string]interface{}) ([]interface{}, error) {
	if collectionName == "" {
		return nil, nil
	}

	val, ok := variables[collectionName]
	if !ok {
		return nil, fmt.Errorf("input collection '%s' not found in variables", collectionName)
	}

	// Handle slice/array
	if slice, ok := val.([]interface{}); ok {
		return slice, nil
	}

	// Handle string that might be JSON
	if str, ok := val.(string); ok {
		var slice []interface{}
		if err := json.Unmarshal([]byte(str), &slice); err == nil {
			return slice, nil
		}
	}

	// Handle array from database (as string)
	return nil, fmt.Errorf("input collection '%s' is not an array, got type %T", collectionName, val)
}

// InitializeMultiInstance creates the multi-instance execution record
func (h *MultiInstanceHandler) InitializeMultiInstance(
	tx *sqlx.Tx,
	processInstanceID, executionID uuid.UUID,
	activityID string,
	isSequential bool,
	totalCount int,
	completionCondition, elementVariable, inputCollection, outputCollection string,
) error {
	miExec := &models.MultiInstanceExecution{
		ID:                     uuid.New(),
		ProcessInstanceID:      processInstanceID,
		ExecutionID:            executionID,
		ActivityID:             activityID,
		IsSequential:           isSequential,
		TotalCount:             totalCount,
		CompletedCount:         0,
		NrOfActiveInstances:    0,
		NrOfCompletedInstances: 0,
		LoopCounter:            0,
		CompletionCondition:    &completionCondition,
		ElementVariable:        &elementVariable,
		InputCollection:        &inputCollection,
		OutputCollection:       &outputCollection,
		Status:                 "active",
	}

	query := `
        INSERT INTO public.multi_instance_executions 
        (id, process_instance_id, execution_id, activity_id, is_sequential, total_count, 
         completion_condition, element_variable, input_collection, output_collection, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW())
    `
	_, err := tx.Exec(query, miExec.ID, miExec.ProcessInstanceID, miExec.ExecutionID,
		miExec.ActivityID, miExec.IsSequential, miExec.TotalCount, miExec.CompletionCondition,
		miExec.ElementVariable, miExec.InputCollection, miExec.OutputCollection, miExec.Status)
	return err
}

// CreateChildExecution creates a child execution for a specific loop iteration
// internal/runtime/multi_instance.go

func (h *MultiInstanceHandler) CreateChildExecution(
	tx *sqlx.Tx,
	parentExecutionID uuid.UUID,
	loopIndex int,
	elementValue string,
	taskNodeID string, // This should be the service task node ID
) (uuid.UUID, error) {
	childExecID := uuid.New()

	// Get the process instance ID from parent execution
	var processInstanceID uuid.UUID
	err := tx.Get(&processInstanceID, `SELECT process_instance_id FROM public.executions WHERE id = $1`, parentExecutionID)
	if err != nil {
		return uuid.Nil, err
	}

	// ✅ Create child execution with the task node as current element
	_, err = tx.Exec(`
        INSERT INTO public.executions 
        (id, process_instance_id, current_element_id, status, is_active, parent_execution_id, created_at, updated_at)
        VALUES ($1, $2, $3, 'active', true, $4, NOW(), NOW())
    `, childExecID, processInstanceID, taskNodeID, parentExecutionID)
	if err != nil {
		return uuid.Nil, err
	}

	// Record child relationship
	_, err = tx.Exec(`
        INSERT INTO public.multi_instance_children 
        (parent_execution_id, child_execution_id, loop_index, element_value, status, created_at)
        VALUES ($1, $2, $3, $4, 'active', NOW())
    `, parentExecutionID, childExecID, loopIndex, elementValue)
	if err != nil {
		return uuid.Nil, err
	}

	return childExecID, nil
}

// SetProcessVariable sets a variable in the process instance
func (h *MultiInstanceHandler) SetProcessVariable(tx *sqlx.Tx, processInstanceID uuid.UUID, key string, value interface{}) error {
	// Get existing variables
	var data []byte
	err := tx.Get(&data, `SELECT data FROM public.variables WHERE process_instance_id = $1`, processInstanceID)

	vars := make(map[string]interface{})
	if err == nil && len(data) > 0 {
		if err := json.Unmarshal(data, &vars); err != nil {
			return err
		}
	}

	vars[key] = value

	newData, err := json.Marshal(vars)
	if err != nil {
		return err
	}

	if err == nil && len(data) > 0 {
		_, err = tx.Exec(`UPDATE public.variables SET data = $1, updated_at = NOW() WHERE process_instance_id = $2`, newData, processInstanceID)
	} else {
		_, err = tx.Exec(`INSERT INTO public.variables (id, process_instance_id, data, updated_at) VALUES ($1, $2, $3, NOW())`,
			uuid.New(), processInstanceID, newData)
	}

	return err
}

// UpdateLoopCounter updates the loop counter for sequential execution
func (h *MultiInstanceHandler) UpdateLoopCounter(tx *sqlx.Tx, miExecID uuid.UUID, loopCounter int) error {
	_, err := tx.Exec(`UPDATE public.multi_instance_executions SET loop_counter = $1, updated_at = NOW() WHERE id = $2`, loopCounter, miExecID)
	return err
}

// GetCompletedChildrenCount returns the number of completed child executions
func (h *MultiInstanceHandler) GetCompletedChildrenCount(parentExecutionID uuid.UUID) (int, error) {
	var count int
	err := h.db.Get(&count, `SELECT COUNT(*) FROM public.multi_instance_children WHERE parent_execution_id = $1 AND status = 'completed'`, parentExecutionID)
	return count, err
}

// CompleteMultiInstance marks the multi-instance as completed
func (h *MultiInstanceHandler) CompleteMultiInstance(tx *sqlx.Tx, miExecID uuid.UUID) error {
	_, err := tx.Exec(`UPDATE public.multi_instance_executions SET status = 'completed', updated_at = NOW() WHERE id = $1`, miExecID)
	return err
}

// internal/runtime/multi_instance.go

// GetOutputCollection retrieves the output collection variable
func (h *MultiInstanceHandler) GetOutputCollection(collectionName string, variables map[string]interface{}) ([]interface{}, error) {
	if collectionName == "" {
		return nil, nil
	}

	val, ok := variables[collectionName]
	if !ok {
		return []interface{}{}, nil
	}

	if slice, ok := val.([]interface{}); ok {
		return slice, nil
	}

	// Handle string that might be JSON
	if str, ok := val.(string); ok {
		var slice []interface{}
		if err := json.Unmarshal([]byte(str), &slice); err == nil {
			return slice, nil
		}
	}

	return []interface{}{}, nil
}

// AppendToOutputCollection appends a value to an output collection
func (h *MultiInstanceHandler) AppendToOutputCollection(tx *sqlx.Tx, processInstanceID uuid.UUID, collectionName string, value interface{}) error {
	if collectionName == "" {
		return nil
	}

	// Get current variables
	var data []byte
	err := tx.Get(&data, `SELECT data FROM public.variables WHERE process_instance_id = $1`, processInstanceID)

	vars := make(map[string]interface{})
	if err == nil && len(data) > 0 {
		if err := json.Unmarshal(data, &vars); err != nil {
			return err
		}
	}

	// Get or create collection
	var collection []interface{}
	if existing, ok := vars[collectionName]; ok {
		if slice, ok := existing.([]interface{}); ok {
			collection = slice
		}
	}

	collection = append(collection, value)
	vars[collectionName] = collection

	newData, err := json.Marshal(vars)
	if err != nil {
		return err
	}

	if err == nil && len(data) > 0 {
		_, err = tx.Exec(`UPDATE public.variables SET data = $1, updated_at = NOW() WHERE process_instance_id = $2`, newData, processInstanceID)
	} else {
		_, err = tx.Exec(`INSERT INTO public.variables (id, process_instance_id, data, updated_at) VALUES ($1, $2, $3, NOW())`,
			uuid.New(), processInstanceID, newData)
	}

	return err
}

// EvaluateCompletionCondition checks if completion condition is met
func (h *MultiInstanceHandler) EvaluateCompletionCondition(condition string, completedCount, totalCount int, variables map[string]interface{}) (bool, error) {
	if condition == "" {
		return completedCount >= totalCount, nil
	}

	// Add special variables for condition evaluation
	evalVars := make(map[string]interface{})
	for k, v := range variables {
		evalVars[k] = v
	}
	evalVars["nrOfCompletedInstances"] = completedCount
	evalVars["nrOfActiveInstances"] = totalCount - completedCount
	evalVars["nrOfInstances"] = totalCount

	return EvaluateCondition(condition, evalVars)
}

// ParseInputCollectionItems parses input collection and returns items with proper typing
func (h *MultiInstanceHandler) ParseInputCollectionItems(inputCollection string, variables map[string]interface{}) ([]interface{}, error) {
	if inputCollection == "" {
		return nil, nil
	}

	// Check if it's a variable reference like ${items}
	if strings.HasPrefix(inputCollection, "${") && strings.HasSuffix(inputCollection, "}") {
		varName := inputCollection[2 : len(inputCollection)-1]
		return h.GetInputCollection(varName, variables)
	}

	// Try to parse as JSON array
	var items []interface{}
	if err := json.Unmarshal([]byte(inputCollection), &items); err == nil {
		return items, nil
	}

	// Try as comma-separated values
	if strings.Contains(inputCollection, ",") {
		parts := strings.Split(inputCollection, ",")
		for _, p := range parts {
			items = append(items, strings.TrimSpace(p))
		}
		return items, nil
	}

	return nil, nil
}
