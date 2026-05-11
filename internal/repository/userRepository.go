package repository

import (
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type UserRepository struct {
	db *sqlx.DB
}

func NewUserRepository(db *sqlx.DB) *UserRepository {
	return &UserRepository{
		db: db,
	}
}

func (u *UserRepository) IsAllow(actionId *int, userUuid *string) (bool, error) {

	const sql = `
		  SELECT count(ra.uuid) as nbr
		  FROM role_actions ra
		  JOIN user_roles as ur ON ur.rolesUuid = ra.roleUuid
		  WHERE actionsId =$1 AND ur.userUuid =$2
		`
	result := models.NewAllowPlayload()
	err := u.db.Get(result, sql, actionId, userUuid)
	return result.Nbr > 0, err
}
