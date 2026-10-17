package runtime

import (
	"fmt"

	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

// BroadcastSignal resumes every execution waiting for the given signal name,
// merging variables into each resumed process instance. Shared by the REST
// APIs (internal/api's SignalController) and by signal throw/end events.
func BroadcastSignal(db *sqlx.DB, dispatcher *events.TaskEventDispatcher, name string, variables map[string]interface{}) (int, []string, error) {
	subRepo := repository.NewEventSubscriptionRepository(db)
	subs, err := subRepo.FindBySignalName(name)
	if err != nil {
		return 0, nil, err
	}

	if len(subs) == 0 {
		return 0, nil, nil
	}

	engineRepo := repository.NewEngineRepository(db, dispatcher)
	auditService := service.NewAuditService(db)

	var errs []string
	resumed := 0

	for _, sub := range subs {
		// Node the execution is currently parked at, for audit purposes.
		var currentNodeID string
		db.QueryRow(`SELECT current_element_id FROM executions WHERE process_instance_id = $1 AND id = $2`, sub.ProcessInstanceID, sub.ExecutionID).Scan(&currentNodeID)

		// Determine target element
		targetElementID := ""
		if sub.TargetElementID != nil && *sub.TargetElementID != "" {
			// Event-based gateway path
			targetElementID = *sub.TargetElementID
			subRepo.CancelSiblingSubscriptions(sub.ExecutionID, sub.ID)
		} else {
			// Regular signal catch: advance to catchNode's outgoing
			graph, err := engineRepo.GetProcessGraphByInstanceID(sub.ProcessInstanceID)
			if err == nil && graph != nil {
				if catchNode, ok := graph.Nodes[currentNodeID]; ok && len(catchNode.Outgoing) > 0 {
					targetElementID = catchNode.Outgoing[0].TargetRef
				}
			}
		}

		// Merge variables
		if len(variables) > 0 {
			existing, _ := engineRepo.GetProcessVariables(sub.ProcessInstanceID)
			merged := existing
			if merged == nil {
				merged = make(map[string]interface{})
			}
			for k, v := range variables {
				merged[k] = v
			}
			tx, err := db.Beginx()
			if err == nil {
				engineRepo.UpdateProcessVariablesTx(tx, sub.ProcessInstanceID, merged)
				if err := tx.Commit(); err == nil {
					service.LogAuditErr("variables merged", auditService.LogVariablesMerged(sub.ProcessInstanceID, variables, "signal"))
				}
			}
		}

		subRepo.Consume(sub.ID)

		if err := repository.ResumeWaitingExecution(db, sub.ProcessInstanceID, sub.ExecutionID, targetElementID); err != nil {
			errs = append(errs, fmt.Sprintf("exec %s: %v", sub.ExecutionID, err))
			continue
		}
		if currentNodeID != "" && targetElementID != "" {
			service.LogAuditErr("execution moved", auditService.LogExecutionMoved(sub.ExecutionID, sub.ProcessInstanceID, currentNodeID, targetElementID, variables))
			if p := eventlog.Global(); p != nil {
				p.ExecutionMoved(sub.ProcessInstanceID.String(), sub.ExecutionID.String(), currentNodeID, targetElementID)
			}
		}
		if err := ResumeExecution(db, sub.ExecutionID, dispatcher); err != nil {
			errs = append(errs, fmt.Sprintf("resume %s: %v", sub.ExecutionID, err))
			continue
		}
		resumed++
	}
	service.LogAuditErr("signal broadcast", auditService.LogSignalBroadcast(name, resumed))
	if p := eventlog.Global(); p != nil {
		p.SignalBroadcast(name, int32(resumed))
	}

	return resumed, errs, nil
}
