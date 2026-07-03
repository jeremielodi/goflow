// internal/service/process_authorization_service.go
package service

import (
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// CanAccessProcess reports whether userID may access processKey at the
// given level ("VIEW", "START", or "MANAGE"). A process_key with no ACL
// grants at all is unrestricted — open to any authenticated user,
// preserving today's behavior for every process until someone
// explicitly adds a grant. Once restricted, access requires an explicit
// grant (direct, or via a role the user holds) at or above level, or the
// global CAN_MANAGE_DEPLOY_PROCESS action (already the "manage all
// deploy/process stuff" superpower, held by super_admin).
func CanAccessProcess(db *sqlx.DB, userID uuid.UUID, processKey, level string) (bool, error) {
	permRepo := repository.NewProcessPermissionRepository(db)

	restricted, err := permRepo.IsRestricted(processKey)
	if err != nil {
		return false, err
	}
	if !restricted {
		return true, nil
	}

	hasGrant, err := permRepo.UserHasProcessPermission(userID, processKey, level)
	if err != nil {
		return false, err
	}
	if hasGrant {
		return true, nil
	}

	actionRepo := repository.NewActionRepository(db)
	return actionRepo.UserHasPermission(userID, "CAN_MANAGE_DEPLOY_PROCESS")
}
