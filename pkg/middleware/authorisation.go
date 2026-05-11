package middleware

import (
	"net/http"

	"github.com/gofiber/fiber/v2"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

const (
	CAN_EDIT_ROLE      = 1
	CAN_RESET_PASSWORD = 2
	CAN_MANAGE_PROJECT = 3
	CAN_CREATE_USER    = 4
)

type Authorization struct {
	db *sqlx.DB
}

func NewAuthorization(db *sqlx.DB) *Authorization {
	return &Authorization{
		db: db,
	}
}

func (auth Authorization) FormateAPI(c *fiber.Ctx, actionId int) error {
	userId := common.GetUserUuid(c)

	userRepo := repository.NewUserRepository(auth.db)

	ok, err := userRepo.IsAllow(&actionId, &userId)
	if err != nil || !ok {
		return c.Status(http.StatusUnauthorized).JSON(fiber.Map{
			"title": "PERMISSION_DENIED",
			"msg":   "You are not allowed to perfom this action",
			"error": err,
		})

	}

	return c.Next()
}

func (auth *Authorization) CanEditRole(c *fiber.Ctx) error {
	return auth.FormateAPI(c, CAN_EDIT_ROLE)
}
func (auth *Authorization) CanReserPassword(c *fiber.Ctx) error {
	return auth.FormateAPI(c, CAN_RESET_PASSWORD)
}
func (auth Authorization) CanManageProject(c *fiber.Ctx) error {
	return auth.FormateAPI(c, CAN_MANAGE_PROJECT)
}

func (auth *Authorization) CanCreateUser(c *fiber.Ctx) error {
	return auth.FormateAPI(c, CAN_CREATE_USER)
}
