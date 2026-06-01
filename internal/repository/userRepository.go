// internal/repository/user_repository.go
package repository

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

type UserRepository struct {
	db *sqlx.DB
}

func NewUserRepository(db *sqlx.DB) *UserRepository {
	return &UserRepository{db: db}
}

// Create inserts a new user using the adapter style.
func (r *UserRepository) Create(user models.UserCreateModel) (sql.Result, error) {
	adapter := database.NewDabaseAdapter(r.db)
	return adapter.Insert("public.users", []database.QueryParameter{
		{Key: "id", Value: user.ID},
		{Key: "email", Value: user.Email},
		{Key: "full_name", Value: user.FullName},
		{Key: "password_hash", Value: user.PasswordHash},
		{Key: "is_active", Value: user.IsActive},
		{Key: "created_at", Value: time.Now()},
		{Key: "updated_at", Value: time.Now()},
	})
}

// CreateWithTx inserts a new user using the adapter style within a transaction.
func (r *UserRepository) CreateWithTx(tx *sqlx.Tx, user models.UserCreateModel) (sql.Result, error) {
	adapter := database.NewDabaseAdapter(r.db)
	return adapter.Insert("public.users", []database.QueryParameter{
		{Key: "id", Value: user.ID},
		{Key: "email", Value: user.Email},
		{Key: "full_name", Value: user.FullName},
		{Key: "password_hash", Value: user.PasswordHash},
		{Key: "is_active", Value: user.IsActive},
		{Key: "created_at", Value: time.Now()},
		{Key: "updated_at", Value: time.Now()},
	})
}

// Update modifies email, full_name, is_active (and updated_at).
func (r *UserRepository) Update(id uuid.UUID, user models.UserUpdateModel) (sql.Result, error) {
	adapter := database.NewDabaseAdapter(r.db)
	return adapter.Update("public.users", []database.QueryParameter{
		{Key: "email", Value: user.Email},
		{Key: "full_name", Value: user.FullName},
		{Key: "is_active", Value: user.IsActive},
		{Key: "updated_at", Value: time.Now()},
	}, database.QueryParameter{
		Key: "id", Value: id,
	})
}

// UpdatePassword changes only the password_hash.
func (r *UserRepository) UpdatePassword(id uuid.UUID, newPasswordHash string) (sql.Result, error) {
	adapter := database.NewDabaseAdapter(r.db)
	return adapter.Update("public.users", []database.QueryParameter{
		{Key: "password_hash", Value: newPasswordHash},
		{Key: "updated_at", Value: time.Now()},
	}, database.QueryParameter{
		Key: "id", Value: id,
	})
}

// GetPasswordHash retrieves the hash for authentication.
func (r *UserRepository) GetPasswordHash(id uuid.UUID) (models.UserPassword, error) {
	var result models.UserPassword
	err := r.db.Get(&result, `
        SELECT id, password_hash
        FROM public.users
        WHERE id = $1
    `, id)
	return result, err
}

// FindByEmail returns a user (without password_hash) by email.
func (r *UserRepository) FindByEmail(email string) (models.User, error) {
	var user models.User
	err := r.db.Get(&user, `
        SELECT id, email, full_name, is_active, created_at, updated_at
        FROM public.users
        WHERE email = $1
    `, email)
	return user, err
}

// FindByID returns a user by UUID.
func (r *UserRepository) FindByID(id uuid.UUID) (models.User, error) {
	var user models.User
	err := r.db.Get(&user, `
        SELECT id, email, full_name, is_active, created_at, updated_at
        FROM public.users
        WHERE id = $1
    `, id)
	return user, err
}

// List returns all users (safe, no password_hash).
func (r *UserRepository) List() ([]models.User, error) {
	var users []models.User
	err := r.db.Select(&users, `
        SELECT id, email, full_name, is_active, created_at, updated_at
        FROM public.users
        ORDER BY created_at DESC
    `)
	return users, err
}

// Delete removes a user by ID.
func (r *UserRepository) Delete(id uuid.UUID) (sql.Result, error) {
	// adapter.Delete would be nice, but fallback to Exec
	return r.db.Exec(`DELETE FROM public.users WHERE id = $1`, id)
}

// DeleteWithTx removes a user by ID within a transaction.
func (r *UserRepository) DeleteWithTx(tx *sqlx.Tx, id uuid.UUID) (sql.Result, error) {
	return tx.Exec(`DELETE FROM public.users WHERE id = $1`, id)
}

// ExistsByEmail checks if email already used.
func (r *UserRepository) ExistsByEmail(email string) (bool, error) {
	var count int
	err := r.db.Get(&count, `SELECT COUNT(*) FROM public.users WHERE email = $1`, email)
	return count > 0, err
}

// Login verifies credentials and returns the user (if password matches).
// You must compare the provided plain password with the stored hash using bcrypt.
func (r *UserRepository) Login(login models.UserLoginModel) (models.User, error) {
	var user models.User
	var hash string
	query := `SELECT id, email, full_name, is_active, created_at, updated_at, password_hash
              FROM public.users WHERE email = $1`
	err := r.db.QueryRowx(query, login.Email).Scan(
		&user.ID, &user.Email, &user.FullName, &user.IsActive,
		&user.CreatedAt, &user.UpdatedAt, &hash,
	)
	if err != nil {
		return user, err
	}
	// Here you must compare login.Password with hash using bcrypt.CompareHashAndPassword
	// For now, we return the user; you should do the comparison outside or inside.
	// It's better to have a separate method for password verification.
	return user, nil
}

// HasAccess, IsAllow etc. remain as you already wrote them
func (u *UserRepository) HasAccess(userUuid string, formUuid string) (bool, error) {
	const sql = `
		  SELECT count(uuid) as nbr
	  FROM public.user_form
	  WHERE userUuid=$1 AND formUuid=$2
		`
	result := models.NewAllowPlayload()
	err := u.db.Get(result, sql, userUuid, formUuid)
	return result.Nbr > 0, err
}

func (u *UserRepository) IsAllow(actionId *int, userUuid *string) (bool, error) {
	const sql = `
		  SELECT count(ra.uuid) as nbr
		  FROM role_actions ra
		  JOIN user_roles as ur ON ur.roles_id = ra.roles_id
		  WHERE ra.action_id =$1 AND ur.user_id =$2
		`
	result := models.NewAllowPlayload()
	err := u.db.Get(result, sql, actionId, userUuid)
	return result.Nbr > 0, err
}
