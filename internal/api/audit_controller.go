// api/audit_controller.go
package api

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jmoiron/sqlx"
)

type AuditController struct {
	auditRepo *repository.AuditRepository
	taskRepo  *repository.TaskRepository
}

func NewAuditController(db *sqlx.DB) *AuditController {
	return &AuditController{
		auditRepo: repository.NewAuditRepository(db),
		taskRepo:  repository.NewTaskRepository(db, nil),
	}
}

// GetProcessAuditLogs retrieves audit logs for a process instance
func (c *AuditController) GetProcessAuditLogs(ctx *fiber.Ctx) error {
	processID, err := uuid.Parse(ctx.Params("processId"))
	if err != nil {
		return ctx.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "Invalid process instance ID",
		})
	}

	logs, err := c.auditRepo.GetProcessAuditLogs(processID)
	if err != nil {
		return ctx.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "Failed to fetch audit logs",
			"error":   err.Error(),
		})
	}

	return ctx.JSON(fiber.Map{
		"processInstanceId": processID,
		"logs":              logs,
		"count":             len(logs),
	})
}

// GetTokenHistory retrieves the derived, human-readable token timeline for a
// process instance (GET /audit/process/:processId/token-history). Unlike
// GetProcessAuditLogs (raw audit rows) or the executions-backed
// /engine-rest/history/activity-instance endpoint (which can only show a
// token's current position, since executions are mutated in place), this
// reconstructs the full path a token took by walking every audit_logs entry.
func (c *AuditController) GetTokenHistory(ctx *fiber.Ctx) error {
	processID, err := uuid.Parse(ctx.Params("processId"))
	if err != nil {
		return ctx.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "Invalid process instance ID",
		})
	}

	logs, err := c.auditRepo.GetProcessAuditLogs(processID)
	if err != nil {
		return ctx.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "Failed to fetch audit logs",
			"error":   err.Error(),
		})
	}

	tasks, err := c.taskRepo.FindTasksByProcessInstance(processID)
	if err != nil {
		return ctx.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "Failed to fetch tasks",
			"error":   err.Error(),
		})
	}

	steps := service.BuildTokenHistory(logs, tasks)

	return ctx.JSON(fiber.Map{
		"processInstanceId": processID,
		"steps":             steps,
		"count":             len(steps),
	})
}

// GetTaskAuditLogs retrieves audit logs for a task
func (c *AuditController) GetTaskAuditLogs(ctx *fiber.Ctx) error {
	taskID, err := uuid.Parse(ctx.Params("taskId"))
	if err != nil {
		return ctx.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "Invalid task ID",
		})
	}

	logs, err := c.auditRepo.GetTaskAuditLogs(taskID)
	if err != nil {
		return ctx.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "Failed to fetch audit logs",
			"error":   err.Error(),
		})
	}

	return ctx.JSON(fiber.Map{
		"taskId": taskID,
		"logs":   logs,
		"count":  len(logs),
	})
}

// GetAuditLogsByDateRange retrieves audit logs within a date range
func (c *AuditController) GetAuditLogsByDateRange(ctx *fiber.Ctx) error {
	var req struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	}

	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "Invalid request body",
		})
	}

	logs, err := c.auditRepo.GetAuditLogsByDateRange(req.Start, req.End)
	if err != nil {
		return ctx.Status(500).JSON(fiber.Map{
			"title":   "Database Error",
			"message": "Failed to fetch audit logs",
			"error":   err.Error(),
		})
	}

	return ctx.JSON(fiber.Map{
		"start": req.Start,
		"end":   req.End,
		"logs":  logs,
		"count": len(logs),
	})
}
