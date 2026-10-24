package v2

import (
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/pkg/common"
	"github.com/jmoiron/sqlx"
)

// instanceAccess answers whether the caller has `level` access to a
// process instance (service.CanAccessProcessInstance), remembering each
// answer for the request: a search's rows often share a few instances.
// Like the v1 controllers' checks, it fails open without an authenticated
// user, the route-level auth already covering that case.
type instanceAccess struct {
	db     *sqlx.DB
	userID uuid.UUID
	anon   bool
	level  string
	seen   map[uuid.UUID]bool
}

func newInstanceAccess(c *fiber.Ctx, db *sqlx.DB, level string) *instanceAccess {
	userID, err := uuid.Parse(common.GetUserUuid(c))
	return &instanceAccess{db: db, userID: userID, anon: err != nil, level: level, seen: map[uuid.UUID]bool{}}
}

func (a *instanceAccess) allowed(instanceID uuid.UUID) bool {
	if a.anon {
		return true
	}
	if ok, done := a.seen[instanceID]; done {
		return ok
	}
	ok, err := service.CanAccessProcessInstance(a.db, a.userID, instanceID, a.level)
	ok = ok && err == nil
	a.seen[instanceID] = ok
	return ok
}
