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
