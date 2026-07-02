package v2

import (
	"errors"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/api"
)

type MessageController struct {
	inner *api.MessageController
}

func NewMessageController(inner *api.MessageController) *MessageController {
	return &MessageController{inner: inner}
}

// PublishMessageRequest matches Camunda 8's message publication body.
type PublishMessageRequest struct {
	Name           string                 `json:"name"`
	CorrelationKey string                 `json:"correlationKey"`
	Variables      map[string]interface{} `json:"variables"`
}

// PublishMessage handles POST /v2/messages/publication.
func (mc *MessageController) PublishMessage(c *fiber.Ctx) error {
	var req PublishMessageRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"title": "INVALID_ARGUMENT", "detail": err.Error()})
	}
	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"title": "INVALID_ARGUMENT", "detail": "name is required"})
	}

	// Camunda 8 correlates by a single opaque key rather than a named
	// process variable; GoFlow's subscriptions are keyed by variable name,
	// so we correlate against a synthetic "correlationKey" variable.
	var correlationKeys map[string]interface{}
	if req.CorrelationKey != "" {
		correlationKeys = map[string]interface{}{"correlationKey": req.CorrelationKey}
	}

	processInstanceID, executionID, err := mc.inner.Correlate(req.Name, correlationKeys, req.Variables)
	if err != nil {
		status := fiber.StatusInternalServerError
		if errors.Is(err, api.ErrNoMatchingSubscription) {
			status = fiber.StatusNotFound
		}
		return c.Status(status).JSON(fiber.Map{"title": "CORRELATION_FAILED", "detail": err.Error()})
	}

	return c.JSON(fiber.Map{
		"processInstanceKey": processInstanceID,
		"executionKey":       executionID,
	})
}
