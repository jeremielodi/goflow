// internal/service/incident_service.go
package service

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jmoiron/sqlx"
)

// SetJobRetries resets a failed job to pending with new retries and
// resolves any open incident for it — extracted from what used to be the
// entire inline body of internal/api/incident_controller.go's
// SetJobRetries handler, now shared with the Partition Processor
// (internal/partition/processor.go) for jobs belonging to a partitioned-
// write-path instance.
func SetJobRetries(db *sqlx.DB, jobID uuid.UUID, retries int) error {
	incidentRepo := repository.NewIncidentRepository(db)

	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var processInstanceID uuid.UUID
	if err := tx.QueryRowx(`SELECT process_instance_id FROM public.jobs WHERE id = $1`, jobID).Scan(&processInstanceID); err != nil {
		return fmt.Errorf("failed to load job: %w", err)
	}

	if _, err := tx.Exec(`
		UPDATE public.jobs
		SET status = 'pending', retries = $1, locked_by = NULL, locked_until = NULL, updated_at = NOW()
		WHERE id = $2
	`, retries, jobID); err != nil {
		return fmt.Errorf("failed to update job retries: %w", err)
	}

	// Fetch the open incident's identity (if any) before resolving it, so
	// IncidentResolved can be published after commit — see below.
	var incidentID uuid.UUID
	hasOpenIncident := tx.QueryRowx(`
		SELECT id FROM public.incidents WHERE job_id = $1 AND state = 'open' LIMIT 1
	`, jobID).Scan(&incidentID) == nil

	if err := incidentRepo.ResolveByJobID(tx, jobID); err != nil {
		return fmt.Errorf("failed to resolve incident: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if p := eventlog.Global(); p != nil {
		// Published unconditionally, unlike IncidentResolved below, so the
		// Command Gateway's bridge always has a confirmation signal to
		// wait on regardless of whether an incident existed.
		p.JobRetriesSet(processInstanceID.String(), jobID.String(), int32(retries))
		if hasOpenIncident {
			p.IncidentResolved(processInstanceID.String(), incidentID.String(), jobID.String())
		}
	}

	return nil
}

// ResolveIncident flips an incident's state to resolved — extracted from
// what used to be the entire inline body of internal/zeebegrpc/server.go's
// ResolveIncident RPC, now shared with the Partition Processor. Unlike
// SetJobRetries, this never touches the job's own retries/status (a
// pre-existing asymmetry between the two endpoints, not something this
// extraction changes).
func ResolveIncident(db *sqlx.DB, incidentID uuid.UUID) error {
	incidentRepo := repository.NewIncidentRepository(db)

	inc, err := incidentRepo.GetByID(incidentID)
	if err != nil {
		return fmt.Errorf("failed to load incident: %w", err)
	}
	if inc == nil {
		return fmt.Errorf("incident %s not found", incidentID)
	}

	if err := incidentRepo.Resolve(incidentID); err != nil {
		return fmt.Errorf("failed to resolve incident: %w", err)
	}

	if p := eventlog.Global(); p != nil {
		jobID := ""
		if inc.JobID != nil {
			jobID = inc.JobID.String()
		}
		p.IncidentResolved(inc.ProcessInstanceID.String(), incidentID.String(), jobID)
	}

	return nil
}
