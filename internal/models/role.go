// internal/models/role.go
package models

import "github.com/google/uuid"

type Role struct {
	ID    uuid.UUID `db:"id" json:"id"`
	Label string    `db:"label" json:"label"`
	// The Postgres column is `isdefault` (no separator, all lowercase —
	// unquoted DDL identifiers fold to lowercase) even though every query
	// site writes `isDefault`; Postgres itself resolves that unquoted
	// identifier case-insensitively, but the column name it reports BACK
	// in a result set is the real stored name (`isdefault`), which sqlx's
	// StructScan then fails to match against a `db:"isDefault"` tag
	// case-sensitively — every read (FindByID, List) was crashing with
	// "missing destination name isdefault" until this tag was corrected
	// to match the actual column.
	IsDefault int `db:"isdefault" json:"is_default"`
}

type Action struct {
	ID          int    `db:"id" json:"id"`
	Code        string `db:"code" json:"code"`
	Description string `db:"description" json:"description"`
}
