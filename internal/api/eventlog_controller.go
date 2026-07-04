package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// EventLogController exposes a dev-only debug view into Phase 0's
// shadow-mode event log consumer — see
// architecture/event-sourced-kafka-design.md. It is not part of the
// engine's real API surface; it exists purely so a human or a test can
// verify the Kafka/Redpanda pipe is actually receiving and ordering events
// correctly, and that the read-model projector's tables agree with the
// real engine's data.
type EventLogController struct {
	consumer *eventlog.Consumer
	projRepo *repository.EventLogProjectionRepository
}

func NewEventLogController(consumer *eventlog.Consumer, db *sqlx.DB) *EventLogController {
	return &EventLogController{
		consumer: consumer,
		projRepo: repository.NewEventLogProjectionRepository(db),
	}
}

// GET /engine-rest/eventlog/recent
func (ctrl *EventLogController) GetRecent(c *fiber.Ctx) error {
	if ctrl.consumer == nil {
		return c.Status(503).JSON(fiber.Map{"message": "event log shadow-mode consumer is not configured"})
	}
	return c.JSON(fiber.Map{"events": ctrl.consumer.Recent()})
}

// GET /engine-rest/eventlog/projected
func (ctrl *EventLogController) GetProjected(c *fiber.Ctx) error {
	instances, err := ctrl.projRepo.ListInstances()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to load projected instances", "error": err.Error()})
	}
	jobs, err := ctrl.projRepo.ListJobs()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"message": "failed to load projected jobs", "error": err.Error()})
	}
	return c.JSON(fiber.Map{"instances": instances, "jobs": jobs})
}
