// internal/api/role_controller.go
package api

import (
	"database/sql"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

type RoleController struct {
	roleRepo *repository.RoleRepository
}

func NewRoleController(db *sqlx.DB) *RoleController {
	return &RoleController{
		roleRepo: repository.NewRoleRepository(db),
	}
}

// CreateRole handles POST /roles
func (rc *RoleController) CreateRole(c *fiber.Ctx) error {
	var req struct {
		Label     string `json:"label"`
		IsDefault int    `json:"is_default"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid request body",
			"message": err.Error(),
		})
	}

	if req.Label == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Validation error",
			"message": "Role label is required",
		})
	}

	role := models.Role{
		ID:        uuid.New(),
		Label:     req.Label,
		IsDefault: req.IsDefault,
	}

	if err := rc.roleRepo.Create(role); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to create role",
			"message": err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "Role created successfully",
		"role":    role,
	})
}

// ListRoles handles GET /roles
func (rc *RoleController) ListRoles(c *fiber.Ctx) error {
	roles, err := rc.roleRepo.List()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to fetch roles",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"roles": roles,
		"count": len(roles),
	})
}

// GetRole handles GET /roles/:id
func (rc *RoleController) GetRole(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid role ID",
			"message": err.Error(),
		})
	}

	role, err := rc.roleRepo.FindByID(id)
	if err != nil {
		if err == sql.ErrNoRows {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error":   "Role not found",
				"message": "No role exists with the provided ID",
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Database error",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"role": role,
	})
}

// UpdateRole handles PUT /roles/:id
func (rc *RoleController) UpdateRole(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid role ID",
			"message": err.Error(),
		})
	}

	var req struct {
		Label     string `json:"label"`
		IsDefault int    `json:"is_default"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid request body",
			"message": err.Error(),
		})
	}

	if err := rc.roleRepo.Update(id, req.Label, req.IsDefault); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to update role",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "Role updated successfully",
	})
}

// DeleteRole handles DELETE /roles/:id
func (rc *RoleController) DeleteRole(c *fiber.Ctx) error {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid role ID",
			"message": err.Error(),
		})
	}

	if err := rc.roleRepo.Delete(id); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to delete role",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "Role deleted successfully",
	})
}

// AssignRoleToUser handles POST /users/:userId/roles/:roleId
func (rc *RoleController) AssignRoleToUser(c *fiber.Ctx) error {
	userID, err := uuid.Parse(c.Params("userId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid user ID",
			"message": err.Error(),
		})
	}

	roleID, err := uuid.Parse(c.Params("roleId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid role ID",
			"message": err.Error(),
		})
	}

	if err := rc.roleRepo.AssignRoleToUser(userID, roleID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to assign role",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "Role assigned successfully",
	})
}

// RemoveRoleFromUser handles DELETE /users/:userId/roles/:roleId
func (rc *RoleController) RemoveRoleFromUser(c *fiber.Ctx) error {
	userID, err := uuid.Parse(c.Params("userId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid user ID",
			"message": err.Error(),
		})
	}

	roleID, err := uuid.Parse(c.Params("roleId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid role ID",
			"message": err.Error(),
		})
	}

	if err := rc.roleRepo.RemoveRoleFromUser(userID, roleID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to remove role",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "Role removed successfully",
	})
}

// GetUserRoles handles GET /users/:userId/roles
func (rc *RoleController) GetUserRoles(c *fiber.Ctx) error {
	userID, err := uuid.Parse(c.Params("userId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid user ID",
			"message": err.Error(),
		})
	}

	roles, err := rc.roleRepo.GetUserRoles(userID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Failed to fetch user roles",
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"roles": roles,
		"count": len(roles),
	})
}
