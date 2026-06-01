// internal/api/action_controller.go
package api

import (
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type ActionController struct {
	actionRepo *repository.ActionRepository
}

func NewActionController(db *sqlx.DB) *ActionController {
	return &ActionController{
		actionRepo: repository.NewActionRepository(db),
	}
}

// ListActions handles GET /actions
func (ac *ActionController) ListActions(c *fiber.Ctx) error {
	actions, err := ac.actionRepo.List()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to fetch actions",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"actions": actions,
		"count":   len(actions),
	})
}

// AssignActionToRole handles POST /roles/:roleId/actions/:actionId
func (ac *ActionController) AssignActionToRole(c *fiber.Ctx) error {
	roleID, err := uuid.Parse(c.Params("roleId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid role ID",
			"message": err.Error(),
		})
	}

	actionID, err := strconv.Atoi(c.Params("actionId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid action ID",
			"message": err.Error(),
		})
	}

	if err := ac.actionRepo.AssignActionToRole(roleID, actionID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to assign action",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "Action assigned to role successfully",
	})
}

// RemoveActionFromRole handles DELETE /roles/:roleId/actions/:actionId
func (ac *ActionController) RemoveActionFromRole(c *fiber.Ctx) error {
	roleID, err := uuid.Parse(c.Params("roleId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid role ID",
			"message": err.Error(),
		})
	}

	actionID, err := strconv.Atoi(c.Params("actionId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid action ID",
			"message": err.Error(),
		})
	}

	if err := ac.actionRepo.RemoveActionFromRole(roleID, actionID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to remove action",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "Action removed from role successfully",
	})
}

// GetRoleActions handles GET /roles/:roleId/actions
func (ac *ActionController) GetRoleActions(c *fiber.Ctx) error {
	roleID, err := uuid.Parse(c.Params("roleId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid role ID",
			"message": err.Error(),
		})
	}

	actions, err := ac.actionRepo.GetRoleActions(roleID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to fetch role actions",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"actions": actions,
		"count":   len(actions),
	})
}
