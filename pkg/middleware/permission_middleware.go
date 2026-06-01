// internal/middleware/permission_middleware.go
package middleware

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

// PermissionMiddleware checks if user has specific permission
func PermissionMiddleware(db *sqlx.DB, actionCode string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		userUUID := common.GetUserUuid(c)
		if userUUID == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "User not authenticated",
			})
		}

		userID, err := uuid.Parse(userUUID)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "Invalid user ID",
			})
		}

		actionRepo := repository.NewActionRepository(db)
		hasPermission, err := actionRepo.UserHasPermission(userID, actionCode)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error":   "Permission check failed",
				"message": err.Error(),
			})
		}

		if !hasPermission {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error":           "Forbidden",
				"message":         "You don't have permission to perform this action",
				"required_action": actionCode,
			})
		}

		return c.Next()
	}
}

// RequireAnyPermission checks if user has any of the specified permissions
func RequireAnyPermission(db *sqlx.DB, actionCodes ...string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		userUUID := common.GetUserUuid(c)
		if userUUID == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "User not authenticated",
			})
		}

		userID, err := uuid.Parse(userUUID)
		if err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error":   "Unauthorized",
				"message": "Invalid user ID",
			})
		}

		actionRepo := repository.NewActionRepository(db)

		for _, actionCode := range actionCodes {
			hasPermission, err := actionRepo.UserHasPermission(userID, actionCode)
			if err != nil {
				continue
			}
			if hasPermission {
				return c.Next()
			}
		}

		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error":            "Forbidden",
			"message":          "You don't have permission to perform this action",
			"required_actions": actionCodes,
		})
	}
}
