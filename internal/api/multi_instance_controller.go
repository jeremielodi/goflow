package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type MultiInstanceController struct {
	db *sqlx.DB
}

func NewMultiInstanceController(db *sqlx.DB) *MultiInstanceController {
	return &MultiInstanceController{db: db}
}

// internal/api/multi_instance_controller.go
func (c *MultiInstanceController) GetByProcessInstance(ctx *fiber.Ctx) error {
	instanceId := ctx.Params("instanceId")
	pid, err := uuid.Parse(instanceId)
	if err != nil {
		return ctx.Status(400).JSON(fiber.Map{"error": "invalid id"})
	}

	var miExec models.MultiInstanceExecution
	query := `SELECT * FROM public.multi_instance_executions WHERE process_instance_id = $1 ORDER BY created_at DESC LIMIT 1`
	err = c.db.Get(&miExec, query, pid)
	if err != nil {
		return ctx.Status(404).JSON(fiber.Map{"error": err.Error()})
	}

	var children []models.MultiInstanceChild
	query2 := `SELECT * FROM public.multi_instance_children WHERE parent_execution_id = $1`
	c.db.Select(&children, query2, miExec.ExecutionID)

	return ctx.JSON(fiber.Map{
		"execution": miExec,
		"children":  children,
	})
}
