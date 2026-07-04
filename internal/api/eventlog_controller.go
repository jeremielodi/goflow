package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/eventlog"
)

// EventLogController exposes a dev-only debug view into Phase 0's
// shadow-mode event log consumer — see
// architecture/event-sourced-kafka-design.md. It is not part of the
// engine's real API surface; it exists purely so a human or a test can
// verify the Kafka/Redpanda pipe is actually receiving and ordering events
// correctly.
type EventLogController struct {
	consumer *eventlog.Consumer
}

func NewEventLogController(consumer *eventlog.Consumer) *EventLogController {
	return &EventLogController{consumer: consumer}
}

// GET /engine-rest/eventlog/recent
func (ctrl *EventLogController) GetRecent(c *fiber.Ctx) error {
	if ctrl.consumer == nil {
		return c.Status(503).JSON(fiber.Map{"message": "event log shadow-mode consumer is not configured"})
	}
	return c.JSON(fiber.Map{"events": ctrl.consumer.Recent()})
}
