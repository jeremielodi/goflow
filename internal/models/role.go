// internal/models/role.go
package models

import "github.com/google/uuid"

type Role struct {
	ID        uuid.UUID `db:"id" json:"id"`
	Label     string    `db:"label" json:"label"`
	IsDefault int       `db:"isDefault" json:"is_default"`
}

type Action struct {
	ID          int    `db:"id" json:"id"`
	Code        string `db:"code" json:"code"`
	Description string `db:"description" json:"description"`
}
