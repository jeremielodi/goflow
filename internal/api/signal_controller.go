package api

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

type SignalController struct {
	db           *sqlx.DB
	dispatcher   *events.TaskEventDispatcher
	auditService *service.AuditService
}

func NewSignalController(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *SignalController {
	return &SignalController{db: db, dispatcher: dispatcher, auditService: service.NewAuditService(db)}
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
	return runtime.Signal(context.Background(), name, variables)
}
