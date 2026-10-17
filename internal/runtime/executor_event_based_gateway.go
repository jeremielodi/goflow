package runtime

import (
	"context"
	"fmt"

	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/repository"
)

// executeEventBasedGateway parks the execution and creates subscriptions for all
// competing events on the gateway's outgoing paths.
func (r *Runtime) executeEventBasedGateway(ctx context.Context, engineRepo *repository.EngineRepository, exec *repository.Execution, node *engine.Node, graph *engine.ProcessGraph) error {
	if len(node.Outgoing) == 0 {
		return fmt.Errorf("event-based gateway %s has no outgoing flows", node.ID)
	}

	tx, err := r.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("EBG: begin tx: %w", err)
	}
	defer tx.Rollback()

	for _, flow := range node.Outgoing {
		catchNode, ok := graph.Nodes[flow.TargetRef]
		if !ok {
			return fmt.Errorf("EBG: target node %s not found", flow.TargetRef)
		}

		// Determine the next node after the catch event
		var targetElementID string
		if len(catchNode.Outgoing) > 0 {
			targetElementID = catchNode.Outgoing[0].TargetRef
		}

		switch catchNode.Type {
		case engine.IntermediateMessageCatchEventType:
			err = insertMessageSubscriptionTx(tx, exec.ProcessInstanceID, exec.ID, catchNode.MessageRef, r.messageCorrelationKey(catchNode, exec.ProcessInstanceID), targetElementID)
			if err != nil {
				return fmt.Errorf("EBG: insert message subscription: %w", err)
			}

		case engine.IntermediateSignalCatchEventType:
			_, err = tx.Exec(`
				INSERT INTO event_subscriptions
				  (process_instance_id, execution_id, event_type, event_name, target_element_id)
				VALUES ($1, $2, 'signal', $3, $4)
			`, exec.ProcessInstanceID, exec.ID, catchNode.SignalRef, targetElementID)
			if err != nil {
				return fmt.Errorf("EBG: insert signal subscription: %w", err)
			}

		case engine.IntermediateTimerEventType, engine.IntermediateCatchEventType:
			// The timer moves the execution straight past the catch event
			// when it fires; whichever event comes first cancels the others
			// (correlateSubscription / ResumeTimerExecution).
			_, err = r.createTimerTx(tx, engineRepo, exec, catchNode.TimerDefinition, targetElementID, "eventBasedGateway")
			if err != nil {
				return fmt.Errorf("EBG: insert timer subscription: %w", err)
			}
		}
	}

	// Park the execution at the EBG node (waiting for first event)
	_, err = tx.Exec(`
		UPDATE executions SET status = 'waiting', updated_at = NOW() WHERE process_instance_id = $1 AND id = $2
	`, exec.ProcessInstanceID, exec.ID)
	if err != nil {
		return fmt.Errorf("EBG: set waiting: %w", err)
	}

	return tx.Commit()
}
