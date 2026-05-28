package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type TimerController struct {
	db *sqlx.DB
}

func NewTimerController(db *sqlx.DB) *TimerController {
	return &TimerController{db: db}
}

// GetTimers handles GET /timers - returns timers for a process instance
func (c *TimerController) GetTimers(ctx *fiber.Ctx) error {
	processInstanceID := ctx.Query("processInstanceId")

	var timers []models.TimerJob
	query := `
		SELECT id, process_instance_id, execution_id, event_type, due_at, payload, 
		       is_triggered, created_at, triggered_at, cycle_count, total_cycles, repeat_interval
		FROM public.timer_jobs
		WHERE 1=1
	`

	if processInstanceID != "" {
		pid, err := uuid.Parse(processInstanceID)
		if err != nil {
			return ctx.Status(400).JSON(fiber.Map{
				"error": "Invalid processInstanceId",
			})
		}
		query += " AND process_instance_id = $1 ORDER BY created_at DESC"
		err = c.db.Select(&timers, query, pid)
		if err != nil {
			return ctx.Status(500).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	} else {
		query += " ORDER BY created_at DESC"
		err := c.db.Select(&timers, query)
		if err != nil {
			return ctx.Status(500).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	return ctx.JSON(timers)
}

// GetTimerByID handles GET /timers/:id - returns a specific timer
func (c *TimerController) GetTimerByID(ctx *fiber.Ctx) error {
	timerID, err := uuid.Parse(ctx.Params("id"))
	if err != nil {
		return ctx.Status(400).JSON(fiber.Map{
			"error": "Invalid timer ID",
		})
	}

	var timer models.TimerJob
	query := `
		SELECT id, process_instance_id, execution_id, event_type, due_at, payload, 
		       is_triggered, created_at, triggered_at, cycle_count, total_cycles, repeat_interval
		FROM public.timer_jobs
		WHERE id = $1
	`

	err = c.db.Get(&timer, query, timerID)
	if err != nil {
		return ctx.Status(404).JSON(fiber.Map{
			"error": "Timer not found",
		})
	}

	return ctx.JSON(timer)
}
