package api

import (
	"fmt"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jmoiron/sqlx"
)

type MessageController struct {
	db         *sqlx.DB
	dispatcher *events.TaskEventDispatcher
}

func NewMessageController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *MessageController {
	return &MessageController{db: db, dispatcher: dispatcher}
}

type CorrelateMessageRequest struct {
	MessageName     string                 `json:"messageName"`
	CorrelationKeys map[string]interface{} `json:"correlationKeys"` // varName → value
	Variables       map[string]interface{} `json:"variables"`
	// Direct process instance targeting (optional)
	ProcessInstanceID string `json:"processInstanceId"`
}

// CorrelateMessage handles POST /engine-rest/message
// Finds all executions waiting for the given message and resumes them.
func (mc *MessageController) CorrelateMessage(c *fiber.Ctx) error {
	var req CorrelateMessageRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": err.Error()})
	}
	if req.MessageName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "messageName is required"})
	}

	subRepo := repository.NewEventSubscriptionRepository(mc.db)

	// Determine correlation filtering
	var corrVarName, corrVarValue string
	for k, v := range req.CorrelationKeys {
		corrVarName = k
		if v != nil {
			corrVarValue = asString(v)
		}
		break // use the first (and usually only) key
	}

	subs, err := subRepo.FindByMessageName(req.MessageName, corrVarName, corrVarValue)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
	}

	if len(subs) == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"message": "no process instance found waiting for message: " + req.MessageName,
		})
	}

	// Correlate to the first matching subscription (1:1 message correlation)
	sub := subs[0]

	// Determine target element (for regular catch event: next outgoing; stored in target_element_id for EBG)
	targetElementID := ""
	if sub.TargetElementID != nil && *sub.TargetElementID != "" {
		// Event-based gateway path: jump directly to stored target
		targetElementID = *sub.TargetElementID
		// Cancel competing subscriptions for the same execution
		subRepo.CancelSiblingSubscriptions(sub.ExecutionID, sub.ID)
	} else {
		// Regular intermediate catch event: advance to catch event's outgoing node
		var nextNode string
		mc.db.QueryRow(`
			SELECT current_element_id FROM executions WHERE id = $1
		`, sub.ExecutionID).Scan(&nextNode)

		// Load the graph to find the outgoing flow
		engineRepo := repository.NewEngineRepository(mc.db, mc.dispatcher)
		graph, err := engineRepo.GetProcessGraphByInstanceID(sub.ProcessInstanceID)
		if err == nil && graph != nil {
			if catchNode, ok := graph.Nodes[nextNode]; ok && len(catchNode.Outgoing) > 0 {
				targetElementID = catchNode.Outgoing[0].TargetRef
			}
		}
	}

	// Merge variables into the process instance
	if len(req.Variables) > 0 {
		engineRepo := repository.NewEngineRepository(mc.db, mc.dispatcher)
		existing, _ := engineRepo.GetProcessVariables(sub.ProcessInstanceID)
		merged := existing
		if merged == nil {
			merged = make(map[string]interface{})
		}
		for k, v := range req.Variables {
			merged[k] = v
		}
		tx, err := mc.db.Beginx()
		if err == nil {
			engineRepo.UpdateProcessVariablesTx(tx, sub.ProcessInstanceID, merged)
			tx.Commit()
		}
	}

	// Mark subscription consumed
	subRepo.Consume(sub.ID)

	// Reactivate the execution and advance to target node
	if err := repository.ResumeWaitingExecution(mc.db, sub.ExecutionID, targetElementID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
	}

	// Resume execution
	if err := runtime.ResumeExecution(mc.db, sub.ExecutionID, mc.dispatcher); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
	}

	return c.JSON(fiber.Map{
		"processInstanceId": sub.ProcessInstanceID,
		"executionId":       sub.ExecutionID,
	})
}

func asString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", val)
	}
}
