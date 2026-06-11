package models

import (
	"time"

	"github.com/google/uuid"
)

// User for safe JSON responses (no password_hash)
type User struct {
	ID        uuid.UUID `json:"id" db:"id"`
	Email     string    `json:"email" db:"email"`
	FullName  string    `json:"full_name,omitempty" db:"full_name"`
	IsActive  bool      `json:"is_active" db:"is_active"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// UserCreateModel for INSERT
type UserCreateModel struct {
	ID           uuid.UUID `json:"id"`
	Email        string    `json:"email" validate:"required,email"`
	FullName     string    `json:"full_name"`
	PasswordHash string    `json:"-"` // set from plain password using bcrypt
	IsActive     bool      `json:"is_active"`
}

// UserUpdateModel for UPDATE (only mutable fields)
type UserUpdateModel struct {
	Email    string `json:"email" validate:"required,email"`
	FullName string `json:"full_name"`
	IsActive bool   `json:"is_active"`
}

// UserLoginModel for authentication
type UserLoginModel struct {
	Email    string `json:"email" validate:"required,email"`
	Password string `json:"password" validate:"required"` // plain password
}

// UserPassword (hash only, for verification)
type UserPassword struct {
	ID           uuid.UUID `db:"id"`
	PasswordHash string    `db:"password_hash"`
}

type IsAllowPlayload struct {
	Nbr int
}

func NewAllowPlayload() *IsAllowPlayload {
	return &IsAllowPlayload{
		Nbr: 0,
	}
}

type CreateUserRequest struct {
	Email    string      `json:"email"`
	FullName string      `json:"full_name"`
	Password string      `json:"password"`
	RoleIDs  []uuid.UUID `json:"role_ids,omitempty"`
}

type UpdateUserRequest struct {
	Email    string `json:"email" validate:"required,email"`
	FullName string `json:"full_name"`
	IsActive bool   `json:"is_active"`
}

type UpdatePasswordRequest struct {
	CurrentPassword string `json:"current_password" validate:"required"`
	NewPassword     string `json:"new_password" validate:"required,min=6"`
}

// LoginRequest represents the login request body
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type RefreshTokenRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// RefreshTokenResponse represents the refresh token response
type RefreshTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
}

// LoginResponse with refresh token
type LoginResponse struct {
	AccessToken  string   `json:"access_token"`
	RefreshToken string   `json:"refresh_token"`
	ExpiresIn    int64    `json:"expires_in"`
	User         User     `json:"user"`
	Roles        []string `json:"roles,omitempty"`
	Actions      []string `json:"actions,omitempty"`
}
