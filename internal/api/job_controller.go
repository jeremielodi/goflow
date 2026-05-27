package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type JobController struct {
	db *sqlx.DB
}

func NewJobController(db *sqlx.DB) *JobController {
	return &JobController{db: db}
}

func (c *JobController) GetJobs(ctx *fiber.Ctx) error {
	processInstanceID := ctx.Query("processInstanceId")
	jobType := ctx.Query("jobType")

	var jobs []models.Job
	query := `SELECT id, process_instance_id, execution_id, job_type, status, payload, retries, error_message, locked_by, locked_until, created_at, completed_at, updated_at 
	          FROM public.jobs WHERE 1=1`
	args := []interface{}{}
	argIndex := 1

	if processInstanceID != "" {
		pid, err := uuid.Parse(processInstanceID)
		if err == nil {
			query += " AND process_instance_id = $" + string(rune(argIndex+'0'))
			args = append(args, pid)
			argIndex++
		}
	}

	if jobType != "" {
		query += " AND job_type = $" + string(rune(argIndex+'0'))
		args = append(args, jobType)
		argIndex++
	}

	query += " ORDER BY created_at DESC"

	err := c.db.Select(&jobs, query, args...)
	if err != nil {
		return ctx.Status(500).JSON(fiber.Map{
			"title": "Database Error",
			"error": err.Error(),
		})
	}

	return ctx.JSON(jobs)
}
