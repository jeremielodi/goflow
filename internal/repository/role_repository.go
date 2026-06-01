// internal/repository/role_repository.go
package repository

import (
	"database/sql"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

type RoleRepository struct {
	db *sqlx.DB
}

func NewRoleRepository(db *sqlx.DB) *RoleRepository {
	return &RoleRepository{db: db}
}

func (r *RoleRepository) Create(role models.Role) error {
	adapter := database.NewDabaseAdapter(r.db)
	_, err := adapter.Insert("public.roles", []database.QueryParameter{
		{Key: "id", Value: role.ID},
		{Key: "label", Value: role.Label},
		{Key: "isDefault", Value: role.IsDefault},
	})
	return err
}

func (r *RoleRepository) CreateWithTx(tx *sqlx.Tx, role models.Role) error {
	adapter := database.NewDabaseAdapter(r.db)
	_, err := adapter.Insert("public.roles", []database.QueryParameter{
		{Key: "id", Value: role.ID},
		{Key: "label", Value: role.Label},
		{Key: "isDefault", Value: role.IsDefault},
	})
	return err
}

func (r *RoleRepository) List() ([]models.Role, error) {
	var roles []models.Role
	err := r.db.Select(&roles, `SELECT id, label, isDefault FROM public.roles ORDER BY label`)
	return roles, err
}

func (r *RoleRepository) FindByID(id uuid.UUID) (models.Role, error) {
	var role models.Role
	err := r.db.Get(&role, `SELECT id, label, isDefault FROM public.roles WHERE id = $1`, id)
	return role, err
}

func (r *RoleRepository) Update(id uuid.UUID, label string, isDefault int) error {
	_, err := r.db.Exec(`UPDATE public.roles SET label = $1, isDefault = $2 WHERE id = $3`, label, isDefault, id)
	return err
}

func (r *RoleRepository) UpdateWithTx(tx *sqlx.Tx, id uuid.UUID, label string, isDefault int) error {
	_, err := tx.Exec(`UPDATE public.roles SET label = $1, isDefault = $2 WHERE id = $3`, label, isDefault, id)
	return err
}

func (r *RoleRepository) Delete(id uuid.UUID) error {
	_, err := r.db.Exec(`DELETE FROM public.roles WHERE id = $1`, id)
	return err
}

func (r *RoleRepository) DeleteWithTx(tx *sqlx.Tx, id uuid.UUID) error {
	_, err := tx.Exec(`DELETE FROM public.roles WHERE id = $1`, id)
	return err
}

// AssignRoleToUser assigns a role to a user
func (r *RoleRepository) AssignRoleToUser(userID, roleID uuid.UUID) error {
	_, err := r.db.Exec(`
		INSERT INTO public.user_roles (id, user_id, roles_id) 
		VALUES ($1, $2, $3)
	`, uuid.New(), userID, roleID)
	return err
}

// AssignRoleToUserWithTx assigns a role to a user within a transaction
func (r *RoleRepository) AssignRoleToUserWithTx(tx *sqlx.Tx, userID, roleID uuid.UUID) error {
	_, err := tx.Exec(`
		INSERT INTO public.user_roles (id, user_id, roles_id) 
		VALUES ($1, $2, $3)
	`, uuid.New(), userID, roleID)
	return err
}

// AssignRolesToUserWithTx assigns multiple roles to a user within a transaction
func (r *RoleRepository) AssignRolesToUserWithTx(tx *sqlx.Tx, userID uuid.UUID, roleIDs []uuid.UUID) error {
	for _, roleID := range roleIDs {
		if err := r.AssignRoleToUserWithTx(tx, userID, roleID); err != nil {
			return err
		}
	}
	return nil
}

// RemoveRoleFromUser removes a role from a user
func (r *RoleRepository) RemoveRoleFromUser(userID, roleID uuid.UUID) error {
	_, err := r.db.Exec(`DELETE FROM public.user_roles WHERE user_id = $1 AND roles_id = $2`, userID, roleID)
	return err
}

// RemoveRoleFromUserWithTx removes a role from a user within a transaction
func (r *RoleRepository) RemoveRoleFromUserWithTx(tx *sqlx.Tx, userID, roleID uuid.UUID) error {
	_, err := tx.Exec(`DELETE FROM public.user_roles WHERE user_id = $1 AND roles_id = $2`, userID, roleID)
	return err
}

// GetUserRoles retrieves all roles for a user
func (r *RoleRepository) GetUserRoles(userID uuid.UUID) ([]models.Role, error) {
	var roles []models.Role
	err := r.db.Select(&roles, `
		SELECT r.id, r.label, r.isDefault 
		FROM public.roles r
		JOIN public.user_roles ur ON ur.roles_id = r.id
		WHERE ur.user_id = $1
	`, userID)
	return roles, err
}

// GetUserRolesWithTx retrieves all roles for a user within a transaction
func (r *RoleRepository) GetUserRolesWithTx(tx *sqlx.Tx, userID uuid.UUID) ([]models.Role, error) {
	var roles []models.Role
	err := tx.Select(&roles, `
		SELECT r.id, r.label, r.isDefault 
		FROM public.roles r
		JOIN public.user_roles ur ON ur.roles_id = r.id
		WHERE ur.user_id = $1
	`, userID)
	return roles, err
}

// GetUsersByRole retrieves all users with a specific role
func (r *RoleRepository) GetUsersByRole(roleID uuid.UUID) ([]models.User, error) {
	var users []models.User
	err := r.db.Select(&users, `
		SELECT u.id, u.email, u.full_name, u.is_active, u.created_at, u.updated_at
		FROM public.users u
		JOIN public.user_roles ur ON ur.user_id = u.id
		WHERE ur.roles_id = $1
	`, roleID)
	return users, err
}

// UserHasRole checks if a user has a specific role
func (r *RoleRepository) UserHasRole(userID, roleID uuid.UUID) (bool, error) {
	var count int
	err := r.db.Get(&count, `
		SELECT COUNT(*) 
		FROM public.user_roles 
		WHERE user_id = $1 AND roles_id = $2
	`, userID, roleID)
	return count > 0, err
}

// GetDefaultRole retrieves the default role (isDefault = 1)
func (r *RoleRepository) GetDefaultRole() (*models.Role, error) {
	var role models.Role
	err := r.db.Get(&role, `SELECT id, label, isDefault FROM public.roles WHERE isDefault = 1 LIMIT 1`)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &role, nil
}

// AssignDefaultRoleToUser assigns the default role to a user
func (r *RoleRepository) AssignDefaultRoleToUser(userID uuid.UUID) error {
	defaultRole, err := r.GetDefaultRole()
	if err != nil {
		return err
	}
	if defaultRole == nil {
		return nil // No default role configured
	}
	return r.AssignRoleToUser(userID, defaultRole.ID)
}

// AssignDefaultRoleToUserWithTx assigns the default role to a user within a transaction
func (r *RoleRepository) AssignDefaultRoleToUserWithTx(tx *sqlx.Tx, userID uuid.UUID) error {
	defaultRole, err := r.GetDefaultRole()
	if err != nil {
		return err
	}
	if defaultRole == nil {
		return nil // No default role configured
	}
	return r.AssignRoleToUserWithTx(tx, userID, defaultRole.ID)
}
