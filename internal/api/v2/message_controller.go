package v2

import (
	"context"
	"errors"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/api"
	"github.com/jeremielodi/goflow/internal/runtime"
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
	TimeToLive     int64                  `json:"timeToLive"` // milliseconds
	MessageID      string                 `json:"messageId"`
	TenantID       string                 `json:"tenantId"`
}

// PublishMessage handles POST /v2/messages/publication with Camunda 8
// semantics (see runtime.PublishMessage).
func (mc *MessageController) PublishMessage(c *fiber.Ctx) error {
	var req PublishMessageRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"title": "INVALID_ARGUMENT", "detail": err.Error()})
	}
	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"title": "INVALID_ARGUMENT", "detail": "name is required"})
	}

	res, err := runtime.PublishMessage(context.Background(), mc.inner.DB(), mc.inner.Dispatcher(), runtime.PublishedMessage{
		Name:           req.Name,
		CorrelationKey: req.CorrelationKey,
		Variables:      req.Variables,
		TimeToLive:     time.Duration(req.TimeToLive) * time.Millisecond,
		MessageID:      req.MessageID,
		TenantID:       req.TenantID,
	})
	if err != nil {
		if errors.Is(err, runtime.ErrMessageAlreadyPublished) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"title": "ALREADY_EXISTS", "detail": err.Error()})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "PUBLISH_FAILED", "detail": err.Error()})
	}

	body := fiber.Map{
		"key":                           res.MessageID,
		"tenantId":                      req.TenantID,
		"correlatedProcessInstanceKeys": nonNilIDs(res.Correlated),
		"startedProcessInstanceKeys":    nonNilIDs(res.Started),
	}
	// Kept from GoFlow's earlier 1:1 response.
	if len(res.Correlated) > 0 {
		body["processInstanceKey"] = res.Correlated[0]
	} else if len(res.Started) > 0 {
		body["processInstanceKey"] = res.Started[0]
	}
	return c.JSON(body)
}

func nonNilIDs(ids []uuid.UUID) []uuid.UUID {
	if ids == nil {
		return []uuid.UUID{}
	}
	return ids
}
