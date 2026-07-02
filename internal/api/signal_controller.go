package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jmoiron/sqlx"
)

type SignalController struct {
	db         *sqlx.DB
	dispatcher *events.TaskEventDispatcher
}

func NewSignalController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *SignalController {
	return &SignalController{db: db, dispatcher: dispatcher}
}

type BroadcastSignalRequest struct {
	Name      string                 `json:"name"`
	Variables map[string]interface{} `json:"variables"`
}

// BroadcastSignal handles POST /engine-rest/signal
// Finds ALL executions waiting for the signal and resumes them (broadcast semantics).
func (sc *SignalController) BroadcastSignal(c *fiber.Ctx) error {
	var req BroadcastSignalRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
	}
	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "name is required"})
	}

	resumed, errs, err := sc.Broadcast(req.Name, req.Variables)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
	}

	return c.JSON(fiber.Map{
		"signalName": req.Name,
		"count":      resumed,
		"errors":     errs,
	})
}

// Broadcast resumes every execution waiting for the given signal name,
// merging variables into each resumed process instance. Shared by the
// Camunda 7 style handler above and the v2 API.
func (sc *SignalController) Broadcast(name string, variables map[string]interface{}) (int, []string, error) {
	subRepo := repository.NewEventSubscriptionRepository(sc.db)
	subs, err := subRepo.FindBySignalName(name)
	if err != nil {
		return 0, nil, err
	}

	if len(subs) == 0 {
		return 0, nil, nil
	}

	engineRepo := repository.NewEngineRepository(sc.db, sc.dispatcher)

	var errs []string
	resumed := 0

	for _, sub := range subs {
		// Determine target element
		targetElementID := ""
		if sub.TargetElementID != nil && *sub.TargetElementID != "" {
			// Event-based gateway path
			targetElementID = *sub.TargetElementID
			subRepo.CancelSiblingSubscriptions(sub.ExecutionID, sub.ID)
		} else {
			// Regular signal catch: advance to catchNode's outgoing
			var currentNodeID string
			sc.db.QueryRow(`SELECT current_element_id FROM executions WHERE id = $1`, sub.ExecutionID).Scan(&currentNodeID)
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
			tx, err := sc.db.Beginx()
			if err == nil {
				engineRepo.UpdateProcessVariablesTx(tx, sub.ProcessInstanceID, merged)
				tx.Commit()
			}
		}

		subRepo.Consume(sub.ID)

		if err := repository.ResumeWaitingExecution(sc.db, sub.ExecutionID, targetElementID); err != nil {
			errs = append(errs, fmt.Sprintf("exec %s: %v", sub.ExecutionID, err))
			continue
		}
		if err := runtime.ResumeExecution(sc.db, sub.ExecutionID, sc.dispatcher); err != nil {
			errs = append(errs, fmt.Sprintf("resume %s: %v", sub.ExecutionID, err))
			continue
		}
		resumed++
	}

	return resumed, errs, nil
}
