// internal/repository/process_repository.go
package repository

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/pkg/database"
	"github.com/jmoiron/sqlx"
)

type ProcessRepository struct {
	db *sqlx.DB
}

func NewProcessRepository(db *sqlx.DB) *ProcessRepository {
	return &ProcessRepository{db: db}
}

// ============================================================
// DEPLOYMENTS
// ============================================================

// CreateDeployment inserts a new deployment record.
func (r *ProcessRepository) CreateDeployment(
	deploy models.DeploymentCreateModel,
) (sql.Result, error) {

	adapter := database.NewDabaseAdapter(r.db)

	id := uuid.New()
	now := time.Now()

	return adapter.Insert(
		"public.deployments",
		[]database.QueryParameter{
			{Key: "id", Value: id},
			{Key: "name", Value: deploy.Name},
			{Key: "deployed_by", Value: deploy.DeployedBy},
			{Key: "status", Value: deploy.Status},
			{Key: "created_at", Value: now},
		},
	)
}

// FindDeploymentByID retrieves deployment by ID.
func (r *ProcessRepository) FindDeploymentByID(
	id uuid.UUID,
) (models.Deployment, error) {

	var deploy models.Deployment

	err := r.db.Get(
		&deploy,
		`
        SELECT
            id,
            name,
            deployed_by,
            status,
            created_at
        FROM public.deployments
        WHERE id = $1
        `,
		id,
	)

	return deploy, err
}

// FindDeploymentByName returns latest deployment by name.
func (r *ProcessRepository) FindDeploymentByName(
	name string,
) (models.Deployment, error) {

	var deploy models.Deployment

	err := r.db.Get(
		&deploy,
		`
        SELECT
            id,
            name,
            deployed_by,
            status,
            created_at
        FROM public.deployments
        WHERE name = $1
        ORDER BY created_at DESC
        LIMIT 1
        `,
		name,
	)

	return deploy, err
}

// ============================================================
// PROCESS DEFINITIONS
// ============================================================

// CreateProcessDefinition stores BPMN XML + parsed graph.
//
// IMPORTANT:
// Multiple versions of the same process_key are supported.
// Example:
//
// invoice_process v1
// invoice_process v2
// invoice_process v3
//
// Existing running instances continue using their original version.
func (r *ProcessRepository) CreateProcessDefinition(
	def models.ProcessDefinitionCreateModel,
) (string, int, error) {

	adapter := database.NewDabaseAdapter(r.db)

	id := uuid.New()
	now := time.Now()

	// Automatically calculate next version
	version, err := r.GetNextProcessVersion(def.ProcessKey)
	if err != nil {
		return "", 0, fmt.Errorf(
			"failed to get next process version: %w",
			err,
		)
	}

	_, err = adapter.Insert(
		"public.process_definitions",
		[]database.QueryParameter{
			{Key: "id", Value: id},
			{Key: "deployment_id", Value: def.DeploymentID},
			{Key: "process_key", Value: def.ProcessKey},
			{Key: "process_name", Value: def.ProcessName},
			{Key: "version", Value: version},
			{Key: "tenant_id", Value: def.TenantID},
			{Key: "is_active", Value: def.IsActive},
			{Key: "bpmn_xml", Value: def.BpmnXML},
			{Key: "parsed_graph", Value: def.ParsedGraph},
			{Key: "created_at", Value: now},
		},
	)

	if err != nil {
		return "", 0, fmt.Errorf(
			"failed to create process definition: %w",
			err,
		)
	}

	return id.String(), version, nil
}

// GetNextProcessVersion returns the next version number.
//
// Example:
// Existing highest version = 4
// Returns => 5
func (r *ProcessRepository) GetNextProcessVersion(
	processKey string,
) (int, error) {

	var version int

	err := r.db.Get(
		&version,
		`
        SELECT COALESCE(MAX(version), 0) + 1
        FROM public.process_definitions
        WHERE process_key = $1
        `,
		processKey,
	)

	if err != nil {
		return 0, err
	}

	return version, nil
}

// FindLatestProcessDefinitionByKey returns latest version.
//
// Example:
// invoice_process -> returns v5
func (r *ProcessRepository) FindLatestProcessDefinitionByKey(
	processKey string,
) (models.ProcessDefinition, error) {

	var def models.ProcessDefinition

	err := r.db.Get(
		&def,
		`
        SELECT
            id,
            deployment_id,
            process_key,
            process_name,
            version,
            is_active,
            bpmn_xml,
            parsed_graph,
            created_at
        FROM public.process_definitions
        WHERE process_key = $1
        ORDER BY version DESC
        LIMIT 1
        `,
		processKey,
	)

	return def, err
}

// FindProcessDefinitionByID returns definition by ID.
func (r *ProcessRepository) FindProcessDefinitionByID(
	id uuid.UUID,
) (models.ProcessDefinition, error) {

	var def models.ProcessDefinition

	err := r.db.Get(
		&def,
		`
        SELECT
            id,
            deployment_id,
            process_key,
            process_name,
            version,
            is_active,
            bpmn_xml,
            parsed_graph,
            created_at
        FROM public.process_definitions
        WHERE id = $1
        `,
		id,
	)

	return def, err
}

// FindProcessDefinitionByKeyAndVersion returns exact version.
//
// Example:
// invoice_process + v2
func (r *ProcessRepository) FindProcessDefinitionByKeyAndVersion(
	processKey string,
	version int,
) (models.ProcessDefinition, error) {

	var def models.ProcessDefinition

	err := r.db.Get(
		&def,
		`
        SELECT
            id,
            deployment_id,
            process_key,
            process_name,
            version,
            is_active,
            bpmn_xml,
            parsed_graph,
            created_at
        FROM public.process_definitions
        WHERE process_key = $1
        AND version = $2
        `,
		processKey,
		version,
	)

	return def, err
}

// ListProcessDefinitions returns all deployed versions.
func (r *ProcessRepository) ListProcessDefinitions() (
	[]models.ProcessDefinition,
	error,
) {

	var defs []models.ProcessDefinition

	err := r.db.Select(
		&defs,
		`
        SELECT
            id,
            deployment_id,
            process_key,
            process_name,
            version,
            is_active,
            bpmn_xml,
            parsed_graph,
            created_at
        FROM public.process_definitions
        ORDER BY process_key ASC, version DESC
        `,
	)

	return defs, err
}

// ListLatestProcessDefinitions returns only latest versions.
//
// Useful for UI like:
//
// invoice_process -> v5
// order_process -> v3
func (r *ProcessRepository) ListLatestProcessDefinitions() (
	[]models.ProcessDefinition,
	error,
) {

	var defs []models.ProcessDefinition

	err := r.db.Select(
		&defs,
		`
        SELECT DISTINCT ON (process_key)
            id,
            deployment_id,
            process_key,
            process_name,
            version,
            is_active,
            bpmn_xml,
            parsed_graph,
            created_at
        FROM public.process_definitions
        ORDER BY process_key, version DESC
        `,
	)

	return defs, err
}

// SetProcessDefinitionActive enables/disables a definition.
//
// Useful for:
//
// - suspending old versions
// - disabling broken deployments
func (r *ProcessRepository) SetProcessDefinitionActive(
	id uuid.UUID,
	active bool,
) (sql.Result, error) {

	adapter := database.NewDabaseAdapter(r.db)

	return adapter.Update(
		"public.process_definitions",
		[]database.QueryParameter{
			{Key: "is_active", Value: active},
		},
		database.QueryParameter{
			Key:   "id",
			Value: id,
		},
	)
}

// DeleteProcessDefinition permanently removes definition.
//
// WARNING:
// Existing instances may break if still referencing this version.
func (r *ProcessRepository) DeleteProcessDefinition(
	id uuid.UUID,
) (sql.Result, error) {

	return r.db.Exec(
		`
        DELETE FROM public.process_definitions
        WHERE id = $1
        `,
		id,
	)
}
