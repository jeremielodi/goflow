package repository

import (
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type FormRepository struct {
	db *sqlx.DB
}

func NewFormRepository(db *sqlx.DB) *FormRepository {
	return &FormRepository{db: db}
}

// GetNextFormVersion returns the next version number for a form id.
func (r *FormRepository) GetNextFormVersion(formId string) (int, error) {
	var version int
	err := r.db.Get(&version, `
		SELECT COALESCE(MAX(version), 0) + 1
		FROM forms
		WHERE form_id = $1
	`, formId)
	return version, err
}

// CreateForm inserts a new form version and returns its id + version.
func (r *FormRepository) CreateForm(f models.FormCreateModel) (uuid.UUID, int, error) {
	version, err := r.GetNextFormVersion(f.FormId)
	if err != nil {
		return uuid.Nil, 0, err
	}

	id := uuid.New()
	_, err = r.db.Exec(`
		INSERT INTO forms (id, deployment_id, form_id, version, schema)
		VALUES ($1, $2, $3, $4, $5)
	`, id, f.DeploymentID, f.FormId, version, f.Schema)
	if err != nil {
		return uuid.Nil, 0, err
	}
	return id, version, nil
}

// FindLatestFormByFormId returns the highest-versioned form for a form id.
func (r *FormRepository) FindLatestFormByFormId(formId string) (*models.Form, error) {
	var f models.Form
	err := r.db.Get(&f, `
		SELECT id, deployment_id, form_id, version, schema, created_at
		FROM forms
		WHERE form_id = $1
		ORDER BY version DESC
		LIMIT 1
	`, formId)
	if err != nil {
		return nil, err
	}
	return &f, nil
}
