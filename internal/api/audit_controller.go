// api/audit_controller.go
package api

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type AuditController struct {
	db          *sqlx.DB
	auditRepo   *repository.AuditRepository
	taskRepo    *repository.TaskRepository
	archiveRepo *repository.HistoricArchiveRepository
}

func NewAuditController(db *sqlx.DB) *AuditController {
	return &AuditController{
		db:          db,
		auditRepo:   repository.NewAuditRepository(db),
		taskRepo:    repository.NewTaskRepository(db, nil),
		archiveRepo: repository.NewHistoricArchiveRepository(db),
	}
}

// checkAuditAccess verifies the caller has at least `level` access to the
// process instance owning instanceID. Returns true (fail open) if there's
// no authenticated user in context.
func (c *AuditController) checkAuditAccess(ctx *fiber.Ctx, instanceID uuid.UUID, level string) bool {
	userID, err := uuid.Parse(common.GetUserUuid(ctx))
	if err != nil {
		return true
	}
	allowed, _ := service.CanAccessProcessInstance(c.db, userID, instanceID, level)
	return allowed
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

	if !c.checkAuditAccess(ctx, processID, "VIEW") {
		return ctx.Status(404).JSON(fiber.Map{
			"title":   "Not Found",
			"message": "Process instance not found",
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
//
// Once an instance is archived (see internal/service/archive_service.go),
// its audit_logs/tasks rows are gone — this falls back to
// historic_activity_instances, which was populated from this exact
// derivation at archive time, so the response shape here never changes
// regardless of whether the instance is still live.
func (c *AuditController) GetTokenHistory(ctx *fiber.Ctx) error {
	processID, err := uuid.Parse(ctx.Params("processId"))
	if err != nil {
		return ctx.Status(400).JSON(fiber.Map{
			"title":   "Validation Error",
			"message": "Invalid process instance ID",
		})
	}

	if !c.checkAuditAccess(ctx, processID, "VIEW") {
		return ctx.Status(404).JSON(fiber.Map{
			"title":   "Not Found",
			"message": "Process instance not found",
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

	var steps []service.TokenHistoryStep
	if len(logs) > 0 {
		tasks, err := c.taskRepo.FindTasksByProcessInstance(processID)
		if err != nil {
			return ctx.Status(500).JSON(fiber.Map{
				"title":   "Database Error",
				"message": "Failed to fetch tasks",
				"error":   err.Error(),
			})
		}
		steps = service.BuildTokenHistory(logs, tasks)
	} else {
		rows, err := c.archiveRepo.FindActivityInstancesByProcessInstance(processID)
		if err != nil {
			return ctx.Status(500).JSON(fiber.Map{
				"title":   "Database Error",
				"message": "Failed to fetch historic activity instances",
				"error":   err.Error(),
			})
		}
		steps = make([]service.TokenHistoryStep, len(rows))
		for i, row := range rows {
			steps[i] = service.TokenHistoryStep{
				Timestamp:   row.OccurredAt,
				Action:      row.Action,
				ElementId:   stringOrEmpty(row.ElementID),
				ElementName: stringOrEmpty(row.ElementName),
				ExecutionId: row.ExecutionID,
				TaskId:      row.TaskID,
				Detail:      stringOrEmpty(row.Detail),
			}
		}
	}

	return ctx.JSON(fiber.Map{
		"processInstanceId": processID,
		"steps":             steps,
		"count":             len(steps),
	})
}

func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
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

	if task, terr := c.taskRepo.FindTaskByID(taskID); terr == nil {
		if !c.checkAuditAccess(ctx, task.ProcessInstanceID, "VIEW") {
			return ctx.Status(404).JSON(fiber.Map{
				"title":   "Not Found",
				"message": "task not found",
			})
		}
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

	// Filter out process-scoped entries the caller lacks VIEW access to.
	// Pure user-action entries (ProcessInstanceID == nil — e.g. login,
	// user management) aren't process-scoped, so they pass through
	// unfiltered; there's no per-process ACL to check for them.
	if userID, parseErr := uuid.Parse(common.GetUserUuid(ctx)); parseErr == nil {
		visible := make([]models.AuditLog, 0, len(logs))
		for _, l := range logs {
			if l.ProcessInstanceID == nil {
				visible = append(visible, l)
				continue
			}
			if allowed, _ := service.CanAccessProcessInstance(c.db, userID, *l.ProcessInstanceID, "VIEW"); allowed {
				visible = append(visible, l)
			}
		}
		logs = visible
	}
	if logs == nil {
		logs = []models.AuditLog{}
	}

	return ctx.JSON(fiber.Map{
		"start": req.Start,
		"end":   req.End,
		"logs":  logs,
		"count": len(logs),
	})
}
