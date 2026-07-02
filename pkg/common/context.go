package common

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/session"
)

var UserContextKey = "user"
var TenantContextKey = "tenant"

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

// SetTenantId records the authenticated caller's tenant for the duration of
// the request. An empty tenantId means "no tenant" (superuser / untenanted
// deployment) and is a no-op, matching SetUserUuid's behavior.
func SetTenantId(c *fiber.Ctx, tenantId string) {
	sess, err := store.Get(c)
	if c == nil || tenantId == "" || err != nil {
		return
	}

	sess.Set(TenantContextKey, tenantId)
	sess.Save()
}

// GetTenantId returns the current request's tenant, or "" if the caller has
// no tenant assigned (superuser / untenanted deployment).
func GetTenantId(c *fiber.Ctx) string {
	sess, err := store.Get(c)
	if c == nil || err != nil {
		return ""
	}

	val := sess.Get(TenantContextKey)
	if val == nil {
		return ""
	}

	tenantId, ok := val.(string)
	if !ok {
		return ""
	}

	return tenantId
}

// TenantMatches reports whether a resource's tenant is visible to the
// caller's tenant. An empty callerTenant means "no tenant restriction"
// (superuser / untenanted deployment) and always matches.
func TenantMatches(resourceTenant *string, callerTenant string) bool {
	if callerTenant == "" {
		return true
	}
	return resourceTenant != nil && *resourceTenant == callerTenant
}

// InstanceTenant decides which tenant a newly-started process instance
// should be stamped with. A tenant-scoped caller always stamps their own
// tenant onto the instance — this is what actually enforces isolation,
// since process definition versions aren't tenant-partitioned (two tenants
// can deploy the same process key). A superuser/untenanted caller (no
// tenant of their own) falls back to the resolved definition's tenant.
func InstanceTenant(definitionTenant *string, callerTenant string) *string {
	if callerTenant != "" {
		return &callerTenant
	}
	return definitionTenant
}
