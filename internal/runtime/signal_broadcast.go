package runtime

import (
	"context"
	"fmt"

	"github.com/jeremielodi/goflow/internal/common"
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
				if catchNode, ok := graph.Nodes[currentNodeID]; ok {
					targetElementID, _ = common.ResolveNext(catchNode, nil)
				}
			}
		}

		// Merge variables
		if len(variables) > 0 {
			// From the waiting element's scope, as in Camunda 8.
			tx, err := db.Beginx()
			if err == nil {
				scope := repository.VariableScopeOf(tx, sub.ProcessInstanceID, sub.ExecutionID)
				if err := repository.SetVariablesTx(tx, sub.ProcessInstanceID, scope, variables); err != nil {
					tx.Rollback()
				} else if err := tx.Commit(); err == nil {
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

	// Signal start events: one new instance per process waiting on it.
	started, startErrs := startBySignal(context.Background(), db, dispatcher, name, variables)
	resumed += len(started)
	errs = append(errs, startErrs...)

	service.LogAuditErr("signal broadcast", auditService.LogSignalBroadcast(name, resumed))
	if p := eventlog.Global(); p != nil {
		p.SignalBroadcast(name, int32(resumed))
	}

	return resumed, errs, nil
}
