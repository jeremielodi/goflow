package v2

import (
	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/api"
)

type SignalController struct {
	inner *api.SignalController
}

func NewSignalController(inner *api.SignalController) *SignalController {
	return &SignalController{inner: inner}
}

// BroadcastSignalRequest matches Camunda 8's signal broadcast body.
type BroadcastSignalRequest struct {
	SignalName string                 `json:"signalName"`
	Variables  map[string]interface{} `json:"variables"`
}

// BroadcastSignal handles POST /v2/signals/broadcast.
func (sc *SignalController) BroadcastSignal(c *fiber.Ctx) error {
	var req BroadcastSignalRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"title": "INVALID_ARGUMENT", "detail": err.Error()})
	}
	if req.SignalName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"title": "INVALID_ARGUMENT", "detail": "signalName is required"})
	}

	resumed, errs, err := sc.inner.Broadcast(req.SignalName, req.Variables)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"title": "INTERNAL_ERROR", "detail": err.Error()})
	}

	return c.JSON(fiber.Map{
		"signalName": req.SignalName,
		"resumed":    resumed,
		"errors":     errs,
	})
}
