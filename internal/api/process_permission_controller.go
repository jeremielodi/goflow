// internal/api/process_permission_controller.go
package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

type ProcessPermissionController struct {
	db       *sqlx.DB
	permRepo *repository.ProcessPermissionRepository
}

func NewProcessPermissionController(db *sqlx.DB) *ProcessPermissionController {
	return &ProcessPermissionController{
		db:       db,
		permRepo: repository.NewProcessPermissionRepository(db),
	}
}

// requireManage 403s unless the caller has MANAGE on processKey (via an
// explicit grant or the global bypass action) — used to gate who may
// view/change a process's own ACL.
func (pc *ProcessPermissionController) requireManage(c *fiber.Ctx, processKey string) error {
	userID, err := uuid.Parse(common.GetUserUuid(c))
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"message": "unauthorized"})
	}
	allowed, err := service.CanAccessProcess(pc.db, userID, processKey, "MANAGE")
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
	}
	if !allowed {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"message": "you do not have MANAGE access to this process"})
	}
	return nil
}

// GET /engine-rest/process-definition/key/:key/permissions
func (pc *ProcessPermissionController) ListPermissions(c *fiber.Ctx) error {
	key := c.Params("key")
	if resp := pc.requireManage(c, key); resp != nil {
		return resp
	}

	perms, err := pc.permRepo.ListByProcessKey(key)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
	}
	return c.JSON(fiber.Map{"permissions": perms})
}

// POST /engine-rest/process-definition/key/:key/permissions
func (pc *ProcessPermissionController) GrantPermission(c *fiber.Ctx) error {
	key := c.Params("key")
	if resp := pc.requireManage(c, key); resp != nil {
		return resp
	}

	var req struct {
		GranteeType string `json:"granteeType"`
		GranteeID   string `json:"granteeId"`
		Permission  string `json:"permission"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "invalid request body"})
	}
	if req.GranteeType != "user" && req.GranteeType != "role" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "granteeType must be 'user' or 'role'"})
	}
	if req.Permission != "VIEW" && req.Permission != "START" && req.Permission != "MANAGE" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "permission must be VIEW, START, or MANAGE"})
	}
	granteeID, err := uuid.Parse(req.GranteeID)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "invalid granteeId"})
	}

	perm, err := pc.permRepo.Create(key, req.GranteeType, granteeID, req.Permission)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
	}
	return c.Status(fiber.StatusCreated).JSON(fiber.Map{"permission": perm})
}

// DELETE /engine-rest/process-definition/key/:key/permissions/:id
func (pc *ProcessPermissionController) RevokePermission(c *fiber.Ctx) error {
	key := c.Params("key")
	if resp := pc.requireManage(c, key); resp != nil {
		return resp
	}

	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"message": "invalid permission id"})
	}
	if err := pc.permRepo.Delete(id); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"message": err.Error()})
	}
	return c.JSON(fiber.Map{"message": "permission revoked"})
}
