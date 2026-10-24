package api

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/cmdbus"
	"github.com/jeremielodi/goflow/internal/common"
	"github.com/jeremielodi/goflow/internal/eventlog"
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
	registerInstanceCommands(db, dispatcher)
	cmdbus.RegisterError("no_matching_subscription", ErrNoMatchingSubscription)
	return &MessageController{db: db, dispatcher: dispatcher, auditService: service.NewAuditService(db)}
}

// DB and Dispatcher let the /v2 controller publish through the same
// dependencies.
func (mc *MessageController) DB() *sqlx.DB                            { return mc.db }
func (mc *MessageController) Dispatcher() *events.TaskEventDispatcher { return mc.dispatcher }

// CorrelateMessageRequest is Camunda 7's CorrelationMessageDto: values plain
// ({"orderId": "A-1"}) or typed ({"orderId": {"value": "A-1", "type":
// "String"}}), the variables to set under Camunda 7's "processVariables" (or
// GoFlow's older "variables"), the instance targeted by processInstanceId or
// businessKey.
type CorrelateMessageRequest struct {
	MessageName       string                 `json:"messageName"`
	CorrelationKeys   map[string]interface{} `json:"correlationKeys"` // varName → value
	Variables         map[string]interface{} `json:"variables"`
	ProcessVariables  map[string]interface{} `json:"processVariables"`
	ProcessInstanceID string                 `json:"processInstanceId"`
	BusinessKey       string                 `json:"businessKey"`
	// All correlates every waiting execution, not just the first.
	All bool `json:"all"`
	// ResultEnabled returns the correlations (otherwise 204), with the
	// instance's variables when VariablesInResultEnabled.
	ResultEnabled            bool `json:"resultEnabled"`
	VariablesInResultEnabled bool `json:"variablesInResultEnabled"`
}

// MessageTarget narrows a correlation to one process instance, by id or
// business key.
type MessageTarget struct {
	ProcessInstanceID string `json:"processInstanceId,omitempty"`
	BusinessKey       string `json:"businessKey,omitempty"`
}

// maxCorrelateAll bounds an "all" correlation.
const maxCorrelateAll = 1000

// CorrelateMessage handles POST /engine-rest/message as Camunda 7 does: 204,
// or with resultEnabled the list of correlations; 400
// MismatchingMessageCorrelationException when nothing waits for the message.
func (mc *MessageController) CorrelateMessage(c *fiber.Ctx) error {
	var req CorrelateMessageRequest
	if err := c.BodyParser(&req); err != nil {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", err.Error())
	}
	if req.MessageName == "" {
		return c7Error(c, fiber.StatusBadRequest, "InvalidRequestException", "messageName is required")
	}

	variables := normalizeVariables(req.Variables)
	for k, v := range normalizeVariables(req.ProcessVariables) {
		variables[k] = v
	}
	keys := normalizeVariables(req.CorrelationKeys)
	target := MessageTarget{ProcessInstanceID: req.ProcessInstanceID, BusinessKey: req.BusinessKey}

	var results []fiber.Map
	for len(results) < maxCorrelateAll {
		instanceID, executionID, err := mc.Correlate(c.Context(), req.MessageName, keys, variables, target)
		if errors.Is(err, ErrNoMatchingSubscription) && len(results) > 0 {
			break // "all": nothing more waits
		}
		if errors.Is(err, ErrNoMatchingSubscription) {
			return c7Error(c, fiber.StatusBadRequest, "RestException", fmt.Sprintf(
				"org.camunda.bpm.engine.MismatchingMessageCorrelationException: Cannot correlate message '%s': No process definition or execution matches the parameters",
				req.MessageName))
		}
		if err != nil {
			return c7Error(c, fiber.StatusInternalServerError, "RestException", err.Error())
		}
		result := fiber.Map{
			"resultType":      "Execution",
			"execution":       fiber.Map{"id": executionID, "processInstanceId": instanceID, "ended": false, "tenantId": nil},
			"processInstance": nil,
		}
		if req.VariablesInResultEnabled {
			if vars, err := repository.NewProcessInstanceRepository(mc.db).GetVariables(instanceID); err == nil {
				result["variables"] = toC7Variables(vars)
			}
		}
		results = append(results, result)
		if !req.All {
			break
		}
	}

	if !req.ResultEnabled {
		return c.SendStatus(fiber.StatusNoContent)
	}
	return c.JSON(results)
}

// Correlate resumes the first execution waiting for messageName (optionally
// scoped by a single correlation key and a target instance) and merges
// variables into that process instance. Like a published message, it runs
// on the partition of its correlation key.
func (mc *MessageController) Correlate(ctx context.Context, messageName string, correlationKeys, variables map[string]interface{}, target MessageTarget) (uuid.UUID, uuid.UUID, error) {
	key := cmdbus.NameKey("message", messageName, fmt.Sprint(correlationKeys))
	var out correlateOutcome
	err := cmdbus.Do(ctx, cmdbus.Key(key), cmdCorrelateMessage,
		correlateArgs{MessageName: messageName, CorrelationKeys: correlationKeys, Variables: variables, Target: target}, &out)
	return out.ProcessInstanceID, out.ExecutionID, err
}

// keepTarget drops the subscriptions outside the targeted instance.
func (mc *MessageController) keepTarget(subs []repository.EventSubscription, target MessageTarget) ([]repository.EventSubscription, error) {
	if target.ProcessInstanceID == "" && target.BusinessKey == "" {
		return subs, nil
	}
	allowed := map[uuid.UUID]bool{}
	if target.BusinessKey != "" {
		var ids []uuid.UUID
		if err := mc.db.Select(&ids, `SELECT id FROM public.process_instances WHERE business_key = $1`, target.BusinessKey); err != nil {
			return nil, err
		}
		for _, id := range ids {
			allowed[id] = true
		}
	}
	var kept []repository.EventSubscription
	for _, s := range subs {
		if target.ProcessInstanceID != "" && s.ProcessInstanceID.String() != target.ProcessInstanceID {
			continue
		}
		if target.BusinessKey != "" && !allowed[s.ProcessInstanceID] {
			continue
		}
		kept = append(kept, s)
	}
	return kept, nil
}

func (mc *MessageController) correlate(messageName string, correlationKeys, variables map[string]interface{}, target MessageTarget) (uuid.UUID, uuid.UUID, error) {
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
	if subs, err = mc.keepTarget(subs, target); err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	if len(subs) == 0 {
		return uuid.Nil, uuid.Nil, fmt.Errorf("%w: %s", ErrNoMatchingSubscription, messageName)
	}

	// Correlate to the first matching subscription (1:1 message correlation)
	sub := subs[0]

	// Node the execution is currently parked at, for audit purposes.
	var fromNode string
	mc.db.QueryRow(`SELECT current_element_id FROM executions WHERE process_instance_id = $1 AND id = $2`, sub.ProcessInstanceID, sub.ExecutionID).Scan(&fromNode)

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
			if catchNode, ok := graph.Nodes[fromNode]; ok {
				targetElementID, _ = common.ResolveNext(catchNode, nil)
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
	if err := repository.ResumeWaitingExecution(mc.db, sub.ProcessInstanceID, sub.ExecutionID, targetElementID); err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	service.LogAuditErr("message correlated", mc.auditService.LogMessageCorrelated(sub.ProcessInstanceID, messageName))
	if fromNode != "" && targetElementID != "" {
		service.LogAuditErr("execution moved", mc.auditService.LogExecutionMoved(sub.ExecutionID, sub.ProcessInstanceID, fromNode, targetElementID, variables))
	}
	if p := eventlog.Global(); p != nil {
		p.MessageCorrelated(sub.ProcessInstanceID.String(), messageName)
		if fromNode != "" && targetElementID != "" {
			p.ExecutionMoved(sub.ProcessInstanceID.String(), sub.ExecutionID.String(), fromNode, targetElementID)
		}
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
