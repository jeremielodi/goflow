package repository

import (
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type IncidentRepository struct {
	db *sqlx.DB
}

func NewIncidentRepository(db *sqlx.DB) *IncidentRepository {
	return &IncidentRepository{db: db}
}

func (r *IncidentRepository) CreateIncidentTx(tx *sqlx.Tx, c *models.IncidentCreate) (*models.Incident, error) {
	inc := &models.Incident{}
	err := tx.QueryRowx(`
		INSERT INTO public.incidents
			(process_instance_id, job_id, incident_type, activity_id, error_message, error_code, state)
		VALUES ($1, $2, $3, $4, $5, $6, 'open')
		RETURNING id, process_instance_id, job_id, incident_type, activity_id,
		          error_message, error_code, state, created_at, resolved_at
	`, c.ProcessInstanceID, c.JobID, c.IncidentType, c.ActivityID,
		nullableString(c.ErrorMessage), nullableString(c.ErrorCode),
	).StructScan(inc)
	return inc, err
}

func (r *IncidentRepository) GetByID(id uuid.UUID) (*models.Incident, error) {
	inc := &models.Incident{}
	err := r.db.QueryRowx(`
		SELECT id, process_instance_id, job_id, incident_type, activity_id,
		       error_message, error_code, state, created_at, resolved_at
		FROM public.incidents WHERE id = $1
	`, id).StructScan(inc)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return inc, err
}

type IncidentFilter struct {
	ProcessInstanceID *uuid.UUID
	State             *string
	IncidentType      *string
	ActivityID        *string
}

func (r *IncidentRepository) List(f IncidentFilter) ([]models.Incident, error) {
	query := `
		SELECT id, process_instance_id, job_id, incident_type, activity_id,
		       error_message, error_code, state, created_at, resolved_at
		FROM public.incidents WHERE 1=1`
	args := []interface{}{}
	n := 1

	if f.ProcessInstanceID != nil {
		query += ` AND process_instance_id = $` + itoa(n)
		args = append(args, *f.ProcessInstanceID)
		n++
	}
	if f.State != nil {
		query += ` AND state = $` + itoa(n)
		args = append(args, *f.State)
		n++
	}
	if f.IncidentType != nil {
		query += ` AND incident_type = $` + itoa(n)
		args = append(args, *f.IncidentType)
		n++
	}
	if f.ActivityID != nil {
		query += ` AND activity_id = $` + itoa(n)
		args = append(args, *f.ActivityID)
		n++
	}
	query += ` ORDER BY created_at DESC`
	_ = n

	var result []models.Incident
	err := r.db.Select(&result, query, args...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (r *IncidentRepository) Resolve(id uuid.UUID) error {
	now := time.Now()
	_, err := r.db.Exec(`
		UPDATE public.incidents SET state = 'resolved', resolved_at = $1 WHERE id = $2 AND state = 'open'
	`, now, id)
	return err
}

func (r *IncidentRepository) Delete(id uuid.UUID) error {
	_, err := r.db.Exec(`
		UPDATE public.incidents SET state = 'deleted' WHERE id = $1
	`, id)
	return err
}

func (r *IncidentRepository) ResolveByJobID(tx *sqlx.Tx, jobID uuid.UUID) error {
	now := time.Now()
	_, err := tx.Exec(`
		UPDATE public.incidents SET state = 'resolved', resolved_at = $1
		WHERE job_id = $2 AND state = 'open'
	`, now, jobID)
	return err
}

// nullableString returns sql.NullString
func nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
