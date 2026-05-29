package runtime

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
)

// TimerCycle represents a repeating timer configuration
type TimerCycle struct {
	Remaining int           // Number of remaining repetitions (-1 = infinite)
	Interval  time.Duration // Time between repetitions
}

// ParseTimerCycle parses a BPMN timer cycle expression
func ParseTimerCycle(cycleStr string) (*TimerCycle, error) {
	// Format: R{count}/{duration}
	// count can be omitted for infinite (R/PT1M)

	parts := strings.Split(cycleStr, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid timer cycle format: %s", cycleStr)
	}

	// Parse repetition count
	countPart := strings.TrimPrefix(parts[0], "R")
	var remaining int = -1 // -1 = infinite

	if countPart != "" {
		count, err := strconv.Atoi(countPart)
		if err != nil {
			return nil, fmt.Errorf("invalid cycle count: %s", countPart)
		}
		remaining = count
	}

	// Parse duration
	durationStr := parts[1]
	if !strings.HasPrefix(durationStr, "PT") {
		return nil, fmt.Errorf("invalid duration format: %s", durationStr)
	}

	// Remove PT prefix
	durationStr = strings.TrimPrefix(durationStr, "PT")
	var duration time.Duration

	// Parse hours
	if strings.Contains(durationStr, "H") {
		hoursStr := strings.Split(durationStr, "H")[0]
		if hours, err := strconv.Atoi(hoursStr); err == nil {
			duration += time.Duration(hours) * time.Hour
		}
		durationStr = strings.TrimPrefix(durationStr, hoursStr+"H")
	}

	// Parse minutes
	if strings.Contains(durationStr, "M") {
		minsStr := strings.Split(durationStr, "M")[0]
		if mins, err := strconv.Atoi(minsStr); err == nil {
			duration += time.Duration(mins) * time.Minute
		}
		durationStr = strings.TrimPrefix(durationStr, minsStr+"M")
	}

	// Parse seconds
	if strings.Contains(durationStr, "S") {
		secsStr := strings.Split(durationStr, "S")[0]
		if secs, err := strconv.Atoi(secsStr); err == nil {
			duration += time.Duration(secs) * time.Second
		}
	}

	if duration == 0 {
		return nil, fmt.Errorf("invalid duration: no time component found")
	}

	return &TimerCycle{
		Remaining: remaining,
		Interval:  duration,
	}, nil
}

func (r *Runtime) createChildExecutions(ctx context.Context, parentExecID uuid.UUID, node *engine.Node, totalCount int) error {
	tx, err := r.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get process instance ID
	var processInstanceID uuid.UUID
	err = tx.Get(&processInstanceID, `SELECT process_instance_id FROM public.executions WHERE id = $1`, parentExecID)
	if err != nil {
		return err
	}

	for i := 0; i < totalCount; i++ {
		childExecID := uuid.New()

		// Child points to the SAME task node (the multi-instance task)
		_, err = tx.Exec(`
            INSERT INTO public.executions 
            (id, process_instance_id, current_element_id, status, is_active, parent_execution_id, created_at, updated_at)
            VALUES ($1, $2, $3, 'active', true, $4, NOW(), NOW())
        `, childExecID, processInstanceID, node.ID, parentExecID)
		if err != nil {
			return err
		}

		// Track child relationship
		_, err = tx.Exec(`
            INSERT INTO public.multi_instance_children 
            (parent_execution_id, child_execution_id, loop_index, element_value, status, created_at)
            VALUES ($1, $2, $3, $4, 'active', NOW())
        `, parentExecID, childExecID, i, fmt.Sprintf("%d", i+1))
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Execute all children in parallel
	rt := NewRuntime(r.Graph, r.DB)
	for i := 0; i < totalCount; i++ {
		var childExecID uuid.UUID
		r.DB.Get(&childExecID, `
            SELECT child_execution_id FROM public.multi_instance_children 
            WHERE parent_execution_id = $1 AND loop_index = $2
        `, parentExecID, i)

		go func(idx int, execID uuid.UUID) {
			log.Printf("🚀 Starting child execution %d", idx+1)
			if err := rt.ExecuteExecution(context.Background(), execID); err != nil {
				log.Printf("❌ Child execution %d failed: %v", idx+1, err)
			}
		}(i, childExecID)
	}

	log.Printf("🔄 Created %d child executions for multi-instance task %s", totalCount, node.ID)
	return nil
}

// internal/runtime/runtime.go

func (r *Runtime) checkMultiInstanceCompletion(ctx context.Context, exec *repository.Execution, node *engine.Node, miExec *models.MultiInstanceExecution, variables map[string]interface{}) error {
	engineRepo := repository.NewEngineRepository(r.DB)

	// Count completed children
	var completedCount int
	err := r.DB.Get(&completedCount, `
        SELECT COUNT(*) FROM public.multi_instance_children 
        WHERE parent_execution_id = $1 AND status = 'completed'
    `, exec.ID)
	if err != nil {
		return err
	}

	log.Printf("📊 Multi-instance %s: %d/%d completed", node.ID, completedCount, miExec.TotalCount)

	if completedCount >= miExec.TotalCount {
		// All children completed - resume parent
		tx, err := r.DB.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// Mark multi-instance as completed
		_, err = tx.Exec(`
            UPDATE public.multi_instance_executions 
            SET status = 'completed', completed_count = $1, updated_at = NOW()
            WHERE id = $2
        `, completedCount, miExec.ID)
		if err != nil {
			return err
		}

		// Find the next node after the multi-instance task
		var nextNodeID string
		if len(node.Outgoing) > 0 {
			nextNodeID = node.Outgoing[0].TargetRef
		}

		log.Printf("✅ Multi-instance task %s completed, moving parent to %s", node.ID, nextNodeID)

		// Update parent execution to active and move to next node
		err = engineRepo.UpdateExecutionStatusAndNodeTx(tx, exec.ID, "active", nextNodeID)
		if err != nil {
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		// Resume parent execution
		rt := NewRuntime(r.Graph, r.DB)
		return rt.ExecuteExecution(ctx, exec.ID)
	}

	return nil
}

func (r *Runtime) handleMultiInstanceParent(ctx context.Context, exec *repository.Execution, node *engine.Node, variables map[string]interface{}) error {
	engineRepo := repository.NewEngineRepository(r.DB)

	// Parse total count
	totalCount := 3 // default
	if node.MultiInstance.LoopCardinality != "" {
		fmt.Sscanf(node.MultiInstance.LoopCardinality, "%d", &totalCount)
	}

	// Create child executions for the SAME task node
	tx, err := r.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i := 0; i < totalCount; i++ {
		childExecID := uuid.New()

		// Child execution points to the SAME node (Task_ProcessItem)
		_, err = tx.Exec(`
            INSERT INTO public.executions 
            (id, process_instance_id, current_element_id, status, is_active, parent_execution_id, created_at, updated_at)
            VALUES ($1, $2, $3, 'active', true, $4, NOW(), NOW())
        `, childExecID, exec.ProcessInstanceID, node.ID, exec.ID)
		if err != nil {
			return err
		}

		// Track child
		_, err = tx.Exec(`
            INSERT INTO public.multi_instance_children 
            (parent_execution_id, child_execution_id, loop_index, element_value, status, created_at)
            VALUES ($1, $2, $3, $4, 'active', NOW())
        `, exec.ID, childExecID, i, fmt.Sprintf("Item %d", i+1))
		if err != nil {
			return err
		}
	}

	// Set parent to waiting
	err = engineRepo.UpdateExecutionStatusTx(tx, exec.ID, "waiting")
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Execute all children in parallel
	rt := NewRuntime(r.Graph, r.DB)
	for i := 0; i < totalCount; i++ {
		var childExecID uuid.UUID
		r.DB.Get(&childExecID, `
            SELECT child_execution_id FROM public.multi_instance_children 
            WHERE parent_execution_id = $1 AND loop_index = $2
        `, exec.ID, i)

		go func(idx int, execID uuid.UUID) {
			log.Printf("🚀 Starting child execution %d", idx+1)
			if err := rt.ExecuteExecution(ctx, execID); err != nil {
				log.Printf("❌ Child execution %d failed: %v", idx+1, err)
			}
		}(i, childExecID)
	}

	return nil
}

func (r *Runtime) createParallelChildren(ctx context.Context, parentExecID uuid.UUID, miExec *models.MultiInstanceExecution, node *engine.Node, taskNodeID string, totalCount int, variables map[string]interface{}, inputItems []interface{}) error {
	tx, err := r.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	childExecIDs := make([]uuid.UUID, 0, totalCount)
	miHandler := NewMultiInstanceHandler(r.DB)

	for i := 0; i < totalCount; i++ {
		var elementValue string
		if len(inputItems) > i && inputItems[i] != nil {
			elementValue = fmt.Sprintf("%v", inputItems[i])
		} else {
			elementValue = fmt.Sprintf("%d", i+1)
		}

		childExecID := uuid.New()

		// Create child execution
		_, err = tx.Exec(`
            INSERT INTO public.executions 
            (id, process_instance_id, current_element_id, status, is_active, parent_execution_id, created_at, updated_at)
            VALUES ($1, $2, $3, 'active', true, $4, NOW(), NOW())
        `, childExecID, miExec.ProcessInstanceID, taskNodeID, parentExecID)
		if err != nil {
			return err
		}

		// Record child relationship
		_, err = tx.Exec(`
            INSERT INTO public.multi_instance_children 
            (parent_execution_id, child_execution_id, loop_index, element_value, status, created_at)
            VALUES ($1, $2, $3, $4, 'active', NOW())
        `, parentExecID, childExecID, i, elementValue)
		if err != nil {
			return err
		}

		// Set element variable
		if node.MultiInstance.ElementVariable != "" && elementValue != "" {
			err = miHandler.SetProcessVariable(tx, miExec.ProcessInstanceID, node.MultiInstance.ElementVariable, elementValue)
			if err != nil {
				log.Printf("Warning: failed to set element variable: %v", err)
			}
		}

		childExecIDs = append(childExecIDs, childExecID)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Execute all children in parallel
	rt := NewRuntime(r.Graph, r.DB)
	for _, childExecID := range childExecIDs {
		go func(execID uuid.UUID) {
			if err := rt.ExecuteExecution(ctx, execID); err != nil {
				log.Printf("❌ Parallel child failed: %v", err)
			}
		}(childExecID)
	}

	log.Printf("🔄 Created %d parallel children for multi-instance task %s", totalCount, node.ID)

	return nil
}

// extractCycleString extracts the timeCycle value from timer definition XML
func extractCycleString(timerDef string) string {
	re := regexp.MustCompile(`<timeCycle>(.*?)</timeCycle>`)
	matches := re.FindStringSubmatch(timerDef)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

func executeScript(script string, variables map[string]interface{}) (map[string]interface{}, error) {
	// Simple implementation - just parse variable assignments
	// For production, use a JavaScript engine like goja or otto

	result := make(map[string]interface{})

	// Simple parsing for "items = [\"Item A\", \"Item B\", \"Item C\"];"
	if strings.Contains(script, "items = [") {
		// Extract items
		re := regexp.MustCompile(`items = \[(.*?)\]`)
		matches := re.FindStringSubmatch(script)
		if len(matches) > 1 {
			itemsStr := matches[1]
			items := strings.Split(itemsStr, ",")
			var itemsList []string
			for _, item := range items {
				item = strings.TrimSpace(item)
				item = strings.Trim(item, `"`)
				itemsList = append(itemsList, item)
			}
			result["items"] = itemsList
		}
	}

	return result, nil
}

// formatDuration formats a duration as ISO 8601 string
func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	result := "PT"
	if hours > 0 {
		result += fmt.Sprintf("%dH", hours)
	}
	if minutes > 0 {
		result += fmt.Sprintf("%dM", minutes)
	}
	if seconds > 0 {
		result += fmt.Sprintf("%dS", seconds)
	}
	return result
}

// OnMultiInstanceChildCompleted MUST be called from your external-task/complete API
// In runtime.go - update OnMultiInstanceChildCompleted
func (r *Runtime) OnMultiInstanceChildCompleted(ctx context.Context, childExecID uuid.UUID, result interface{}) error {
	engineRepo := repository.NewEngineRepository(r.DB)

	// Get the child execution
	childExec, err := engineRepo.GetExecutionByID(childExecID)
	if err != nil {
		return fmt.Errorf("failed to get child execution: %w", err)
	}
	if childExec == nil {
		return fmt.Errorf("child execution not found: %s", childExecID)
	}

	// Only process multi-instance children (have a parent)
	if childExec.ParentExecutionID == nil {
		return nil // not a multi-instance child
	}
	parentExecID := *childExec.ParentExecutionID

	// Get the multi-instance record for the parent
	var miExec models.MultiInstanceExecution
	err = r.DB.Get(&miExec, `
        SELECT * FROM public.multi_instance_executions 
        WHERE execution_id = $1 AND status = 'active'
    `, parentExecID)
	if err != nil {
		return fmt.Errorf("no active multi-instance for parent %s: %w", parentExecID, err)
	}

	// Get the node
	node, err := engineRepo.GetNodeByID(r.Graph, miExec.ActivityID)
	if err != nil {
		return fmt.Errorf("failed to get multi-instance activity node: %w", err)
	}

	// Get variables
	variables, err := engineRepo.GetProcessVariables(miExec.ProcessInstanceID)
	if err != nil {
		variables = make(map[string]interface{})
	}

	// Get child info
	var child struct {
		LoopIndex    int    `db:"loop_index"`
		ElementValue string `db:"element_value"`
	}
	err = r.DB.Get(&child, `
        SELECT loop_index, element_value FROM public.multi_instance_children 
        WHERE child_execution_id = $1
    `, childExecID)
	if err != nil {
		return err
	}

	miHandler := NewMultiInstanceHandler(r.DB)

	// --- Mark child completed and store output ---
	tx, err := r.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Add result to output collection if configured
	if node.MultiInstance != nil && node.MultiInstance.OutputDataItem != "" && result != nil {
		err = miHandler.AppendToOutputCollection(tx, miExec.ProcessInstanceID, node.MultiInstance.OutputDataItem, result)
		if err != nil {
			log.Printf("Warning: failed to append to output collection: %v", err)
		}
	}

	_, err = tx.Exec(`
        UPDATE public.multi_instance_children 
        SET status = 'completed', completed_at = NOW() 
        WHERE child_execution_id = $1
    `, childExecID)
	if err != nil {
		return fmt.Errorf("failed to mark child completed: %w", err)
	}

	_, err = tx.Exec(`
        UPDATE public.executions 
        SET status = 'completed', is_active = false, updated_at = NOW() 
        WHERE id = $1
    `, childExecID)
	if err != nil {
		return fmt.Errorf("failed to complete child execution: %w", err)
	}

	// Count completed children
	var completedCount int
	err = tx.Get(&completedCount, `
        SELECT COUNT(*) FROM public.multi_instance_children 
        WHERE parent_execution_id = $1 AND status = 'completed'
    `, parentExecID)
	if err != nil {
		return fmt.Errorf("failed to count completed: %w", err)
	}

	_, err = tx.Exec(`
        UPDATE public.multi_instance_executions 
        SET completed_count = $1, updated_at = NOW() 
        WHERE id = $2
    `, completedCount, miExec.ID)
	if err != nil {
		return fmt.Errorf("failed to update progress: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("📊 Multi-instance progress: %d/%d completed (sequential=%v)",
		completedCount, miExec.TotalCount, miExec.IsSequential)

	// Check completion condition
	isComplete, err := miHandler.EvaluateCompletionCondition(*miExec.CompletionCondition, completedCount, miExec.TotalCount, variables)
	if err != nil {
		log.Printf("Warning: completion condition evaluation failed: %v", err)
		isComplete = completedCount >= miExec.TotalCount
	}

	if isComplete {
		log.Printf("✅ All %d children done — completing multi-instance", miExec.TotalCount)
		return r.completeMultiInstance(ctx, parentExecID, &miExec, node, variables)
	}

	// Sequential: spawn next child immediately
	if miExec.IsSequential && completedCount < miExec.TotalCount {
		log.Printf("🔄 Sequential: spawning child %d/%d", completedCount+1, miExec.TotalCount)

		// Get fresh variables for next child
		vars, err := engineRepo.GetProcessVariables(miExec.ProcessInstanceID)
		if err != nil {
			vars = make(map[string]interface{})
		}

		// Get input items if specified
		var inputItems []interface{}
		if node.MultiInstance != nil && node.MultiInstance.InputDataItem != "" {
			inputItems, _ = miHandler.GetInputCollection(node.MultiInstance.InputDataItem, vars)
		}

		return r.createSequentialChild(ctx, parentExecID, &miExec, node, node.ID, completedCount, vars, inputItems)
	}

	log.Printf("🔍 Parallel - waiting for all children")
	return nil
}

func (r *Runtime) completeMultiInstance(ctx context.Context, parentExecID uuid.UUID, miExec *models.MultiInstanceExecution, node *engine.Node, variables map[string]interface{}) error {
	// engineRepo := repository.NewEngineRepository(r.DB)

	tx, err := r.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		UPDATE public.multi_instance_executions 
		SET status = 'completed', completed_count = total_count, updated_at = NOW() 
		WHERE id = $1
	`, miExec.ID)
	if err != nil {
		return err
	}

	var nextNodeID string
	if len(node.Outgoing) > 0 {
		nextNodeID = node.Outgoing[0].TargetRef
	}

	// CRITICAL FIX: Also set is_active = true so GetActiveExecution finds it
	_, err = tx.Exec(`
		UPDATE public.executions 
		SET status = 'active', is_active = true, current_element_id = $1, updated_at = NOW() 
		WHERE id = $2
	`, nextNodeID, parentExecID)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	rt := NewRuntime(r.Graph, r.DB)
	go func() {
		log.Printf("🚀 Resuming parent %s at %s", parentExecID, nextNodeID)
		if err := rt.ExecuteExecution(ctx, parentExecID); err != nil {
			log.Printf("❌ Parent resume failed: %v", err)
		}
	}()
	return nil
}
