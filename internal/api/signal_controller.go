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

	subRepo := repository.NewEventSubscriptionRepository(sc.db)
	subs, err := subRepo.FindBySignalName(req.Name)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
	}

	if len(subs) == 0 {
		return c.JSON(fiber.Map{"count": 0, "message": "no subscribers found for signal: " + req.Name})
	}

	engineRepo := repository.NewEngineRepository(sc.db, sc.dispatcher)

	var errors []string
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
		if len(req.Variables) > 0 {
			existing, _ := engineRepo.GetProcessVariables(sub.ProcessInstanceID)
			merged := existing
			if merged == nil {
				merged = make(map[string]interface{})
			}
			for k, v := range req.Variables {
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
			errors = append(errors, fmt.Sprintf("exec %s: %v", sub.ExecutionID, err))
			continue
		}
		if err := runtime.ResumeExecution(sc.db, sub.ExecutionID, sc.dispatcher); err != nil {
			errors = append(errors, fmt.Sprintf("resume %s: %v", sub.ExecutionID, err))
			continue
		}
		resumed++
	}

	return c.JSON(fiber.Map{
		"signalName": req.Name,
		"count":      resumed,
		"errors":     errors,
	})
}
