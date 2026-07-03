// internal/repository/zeebe_key_repository.go
package repository

import (
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type ZeebeKeyRepository struct {
	db *sqlx.DB
}

func NewZeebeKeyRepository(db *sqlx.DB) *ZeebeKeyRepository {
	return &ZeebeKeyRepository{db: db}
}

// GetOrAssignKey returns the int64 Zeebe-protocol key for (resourceType,
// resourceID), assigning a new one on first use. The ON CONFLICT ... DO
// UPDATE (a no-op column rewrite) makes RETURNING fire even when the row
// already exists, so this is race-safe under concurrent callers.
func (r *ZeebeKeyRepository) GetOrAssignKey(resourceType string, resourceID uuid.UUID) (int64, error) {
	var key int64
	err := r.db.Get(&key, `
		INSERT INTO public.zeebe_keys (resource_type, resource_id)
		VALUES ($1, $2)
		ON CONFLICT (resource_type, resource_id)
		DO UPDATE SET resource_type = EXCLUDED.resource_type
		RETURNING key
	`, resourceType, resourceID)
	return key, err
}

// ResolveKey reverses GetOrAssignKey — looks up the UUID a previously
// issued int64 key refers to, scoped to resourceType.
func (r *ZeebeKeyRepository) ResolveKey(resourceType string, key int64) (uuid.UUID, error) {
	var resourceID uuid.UUID
	err := r.db.Get(&resourceID, `
		SELECT resource_id FROM public.zeebe_keys
		WHERE resource_type = $1 AND key = $2
	`, resourceType, key)
	return resourceID, err
}
