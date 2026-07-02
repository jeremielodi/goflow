package api

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

// ErrNoMatchingSubscription is returned by Correlate when no execution is
// currently waiting for the given message name (+ correlation key, if any).
var ErrNoMatchingSubscription = errors.New("no process instance found waiting for this message")

type MessageController struct {
	db           *sqlx.DB
	dispatcher   *events.TaskEventDispatcher
	auditService *service.AuditService
}

func NewMessageController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *MessageController {
	return &MessageController{db: db, dispatcher: dispatcher, auditService: service.NewAuditService(db)}
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

	processInstanceID, executionID, err := mc.Correlate(req.MessageName, req.CorrelationKeys, req.Variables)
	if err != nil {
		status := fiber.StatusInternalServerError
		if errors.Is(err, ErrNoMatchingSubscription) {
			status = fiber.StatusNotFound
		}
		return c.Status(status).JSON(fiber.Map{"message": err.Error()})
	}

	return c.JSON(fiber.Map{
		"processInstanceId": processInstanceID,
		"executionId":       executionID,
	})
}

// Correlate resumes the first execution waiting for messageName (optionally
// scoped by a single correlation key) and merges variables into that process
// instance. Shared by the Camunda 7 style handler above and the v2 API.
func (mc *MessageController) Correlate(messageName string, correlationKeys, variables map[string]interface{}) (uuid.UUID, uuid.UUID, error) {
	subRepo := repository.NewEventSubscriptionRepository(mc.db)

	// Determine correlation filtering
	var corrVarName, corrVarValue string
	for k, v := range correlationKeys {
		corrVarName = k
		if v != nil {
			corrVarValue = asString(v)
		}
		break // use the first (and usually only) key
	}

	subs, err := subRepo.FindByMessageName(messageName, corrVarName, corrVarValue)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	if len(subs) == 0 {
		return uuid.Nil, uuid.Nil, fmt.Errorf("%w: %s", ErrNoMatchingSubscription, messageName)
	}

	// Correlate to the first matching subscription (1:1 message correlation)
	sub := subs[0]

	// Node the execution is currently parked at, for audit purposes.
	var fromNode string
	mc.db.QueryRow(`SELECT current_element_id FROM executions WHERE id = $1`, sub.ExecutionID).Scan(&fromNode)

	// Determine target element (for regular catch event: next outgoing; stored in target_element_id for EBG)
	targetElementID := ""
	if sub.TargetElementID != nil && *sub.TargetElementID != "" {
		// Event-based gateway path: jump directly to stored target
		targetElementID = *sub.TargetElementID
		// Cancel competing subscriptions for the same execution
		subRepo.CancelSiblingSubscriptions(sub.ExecutionID, sub.ID)
	} else {
		// Regular intermediate catch event: advance to catch event's outgoing node
		// Load the graph to find the outgoing flow
		engineRepo := repository.NewEngineRepository(mc.db, mc.dispatcher)
		graph, err := engineRepo.GetProcessGraphByInstanceID(sub.ProcessInstanceID)
		if err == nil && graph != nil {
			if catchNode, ok := graph.Nodes[fromNode]; ok && len(catchNode.Outgoing) > 0 {
				targetElementID = catchNode.Outgoing[0].TargetRef
			}
		}
	}

	// Merge variables into the process instance
	if len(variables) > 0 {
		engineRepo := repository.NewEngineRepository(mc.db, mc.dispatcher)
		existing, _ := engineRepo.GetProcessVariables(sub.ProcessInstanceID)
		merged := existing
		if merged == nil {
			merged = make(map[string]interface{})
		}
		for k, v := range variables {
			merged[k] = v
		}
		tx, err := mc.db.Beginx()
		if err == nil {
			engineRepo.UpdateProcessVariablesTx(tx, sub.ProcessInstanceID, merged)
			if err := tx.Commit(); err == nil {
				service.LogAuditErr("variables merged", mc.auditService.LogVariablesMerged(sub.ProcessInstanceID, variables, "message"))
			}
		}
	}

	// Mark subscription consumed
	subRepo.Consume(sub.ID)

	// Reactivate the execution and advance to target node
	if err := repository.ResumeWaitingExecution(mc.db, sub.ExecutionID, targetElementID); err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	service.LogAuditErr("message correlated", mc.auditService.LogMessageCorrelated(sub.ProcessInstanceID, messageName))
	if fromNode != "" && targetElementID != "" {
		service.LogAuditErr("execution moved", mc.auditService.LogExecutionMoved(sub.ExecutionID, sub.ProcessInstanceID, fromNode, targetElementID, variables))
	}

	// Resume execution
	if err := runtime.ResumeExecution(mc.db, sub.ExecutionID, mc.dispatcher); err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	return sub.ProcessInstanceID, sub.ExecutionID, nil
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
