package models

import (
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID           uuid.UUID `json:"id"`
	Email        string    `json:"email"`
	FullName     string    `json:"full_name,omitempty"`
	PasswordHash string    `json:"-"`
	IsActive     bool      `json:"is_active"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type IsAllowPlayload struct {
	Nbr int
}

func NewAllowPlayload() *IsAllowPlayload {
	return &IsAllowPlayload{
		Nbr: 0,
	}
}
