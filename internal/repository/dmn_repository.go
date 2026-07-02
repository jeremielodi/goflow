package repository

import (
	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jmoiron/sqlx"
)

type DMNRepository struct {
	db *sqlx.DB
}

func NewDMNRepository(db *sqlx.DB) *DMNRepository {
	return &DMNRepository{db: db}
}

// GetNextDecisionVersion returns the next version number for a decision key.
func (r *DMNRepository) GetNextDecisionVersion(decisionKey string) (int, error) {
	var version int
	err := r.db.Get(&version, `
		SELECT COALESCE(MAX(version), 0) + 1
		FROM dmn_decisions
		WHERE decision_key = $1
	`, decisionKey)
	return version, err
}

// CreateDecision inserts a new decision version and returns its id + version.
func (r *DMNRepository) CreateDecision(d models.DMNDecisionCreateModel) (uuid.UUID, int, error) {
	version, err := r.GetNextDecisionVersion(d.DecisionKey)
	if err != nil {
		return uuid.Nil, 0, err
	}

	id := uuid.New()
	_, err = r.db.Exec(`
		INSERT INTO dmn_decisions
			(id, deployment_id, decision_key, decision_name, version, is_active, dmn_xml, parsed_table)
		VALUES ($1, $2, $3, $4, $5, true, $6, $7)
	`, id, d.DeploymentID, d.DecisionKey, d.DecisionName, version, d.DmnXML, d.ParsedTable)
	if err != nil {
		return uuid.Nil, 0, err
	}
	return id, version, nil
}

// FindLatestDecisionByKey returns the highest-versioned decision for a key.
func (r *DMNRepository) FindLatestDecisionByKey(decisionKey string) (*models.DMNDecision, error) {
	var d models.DMNDecision
	err := r.db.Get(&d, `
		SELECT id, deployment_id, decision_key, decision_name, version, is_active, dmn_xml, parsed_table, created_at
		FROM dmn_decisions
		WHERE decision_key = $1
		ORDER BY version DESC
		LIMIT 1
	`, decisionKey)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// ListDecisions returns all deployed decisions (latest and prior versions).
func (r *DMNRepository) ListDecisions() ([]models.DMNDecision, error) {
	var decisions []models.DMNDecision
	err := r.db.Select(&decisions, `
		SELECT id, deployment_id, decision_key, decision_name, version, is_active, dmn_xml, parsed_table, created_at
		FROM dmn_decisions
		ORDER BY decision_key, version DESC
	`)
	return decisions, err
}
