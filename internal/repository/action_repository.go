// internal/repository/action_repository.go
package repository

import (
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type ActionRepository struct {
	db *sqlx.DB
}

func NewActionRepository(db *sqlx.DB) *ActionRepository {
	return &ActionRepository{db: db}
}

func (r *ActionRepository) List() ([]models.Action, error) {
	var actions []models.Action
	err := r.db.Select(&actions, `SELECT id, code, description FROM public.actions ORDER BY id`)
	return actions, err
}

func (r *ActionRepository) AssignActionToRole(roleID uuid.UUID, actionID int) error {
	_, err := r.db.Exec(`
		INSERT INTO public.role_actions (uuid, roles_id, action_id) 
		VALUES ($1, $2, $3)
	`, uuid.New(), roleID, actionID)
	return err
}

func (r *ActionRepository) RemoveActionFromRole(roleID uuid.UUID, actionID int) error {
	_, err := r.db.Exec(`DELETE FROM public.role_actions WHERE roles_id = $1 AND action_id = $2`, roleID, actionID)
	return err
}

func (r *ActionRepository) GetRoleActions(roleID uuid.UUID) ([]models.Action, error) {
	var actions []models.Action
	err := r.db.Select(&actions, `
		SELECT a.id, a.code, a.description 
		FROM public.actions a
		JOIN public.role_actions ra ON ra.action_id = a.id
		WHERE ra.roles_id = $1
	`, roleID)
	return actions, err
}

func (r *ActionRepository) UserHasPermission(userID uuid.UUID, actionCode string) (bool, error) {
	var count int
	err := r.db.Get(&count, `
		SELECT COUNT(*)
		FROM public.user_roles ur
		JOIN public.role_actions ra ON ra.roles_id = ur.roles_id
		JOIN public.actions a ON a.id = ra.action_id
		WHERE ur.user_id = $1 AND a.code = $2
	`, userID, actionCode)
	return count > 0, err
}

func (r *ActionRepository) GetUserPermissions(userID uuid.UUID) ([]models.Action, error) {
	var actions []models.Action
	err := r.db.Select(&actions, `
		SELECT DISTINCT a.id, a.code, a.description 
		FROM public.user_roles ur
		JOIN public.role_actions ra ON ra.roles_id = ur.roles_id
		JOIN public.actions a ON a.id = ra.action_id
		WHERE ur.user_id = $1
	`, userID)
	return actions, err
}
