package common

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/session"
)

var UserContextKey = "user"

var store = session.New()

func SetUserUuid(c *fiber.Ctx, userId string) {
	sess, err := store.Get(c)
	if c == nil || userId == "" || err != nil {
		return
	}

	sess.Set(UserContextKey, userId)
	sess.Save()
}

func GetUserUuid(c *fiber.Ctx) string {
	sess, err := store.Get(c)
	if c == nil || err != nil {
		return ""
	}

	val := sess.Get(UserContextKey)

	if val == nil {
		return ""
	}

	userUuid, ok := val.(string)
	if !ok {
		return ""
	}

	return userUuid

}
