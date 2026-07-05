package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/models"
)

// bridgeTimeout bounds how long the Command Gateway waits for the
// Partition Processor's resulting domain event before giving up. Normal
// end-to-end latency (produce -> consume -> apply -> publish -> observe)
// is expected to be single-digit milliseconds; a timeout here means the
// processor is down or badly behind, a real error, not a case to silently
// fall back to the legacy path for.
const bridgeTimeout = 5 * time.Second

// StartProcess is the Command Gateway's bridge for starting an instance of
// a process definition flagged write_path = 'partitioned'. Callers use
// this instead of service.StartProcessInstance + runtime.ExecuteExecution
// for such definitions — see the package doc for why service.CompleteJob
// can't do this branch internally (import cycle) and every call site
// branches itself.
func StartProcess(def *models.ProcessDefinition, tenantID *string, variables map[string]interface{}) (instanceID, execID uuid.UUID, err error) {
	cmdProducer := eventlog.GlobalCommand()
	consumer := eventlog.GlobalConsumer()
	if cmdProducer == nil || consumer == nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("partitioned write path is not configured on this instance")
	}

	if variables == nil {
		variables = map[string]interface{}{}
	}
	varsJSON, err := json.Marshal(variables)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("failed to marshal variables: %w", err)
	}

	instanceID = uuid.New()
	execID = uuid.New()

	// Register before producing — see eventlog.Consumer.Register's doc for
	// why this ordering matters.
	waiter := consumer.Register(instanceID.String(), "ProcessStarted")

	produceCtx, cancel := context.WithTimeout(context.Background(), bridgeTimeout)
	defer cancel()
	if err := cmdProducer.StartProcess(produceCtx, instanceID, execID, def.ID, tenantID, string(varsJSON)); err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	if _, err := waiter.Await(context.Background(), bridgeTimeout); err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("partitioned engine did not confirm process start: %w", err)
	}

	return instanceID, execID, nil
}

// CompleteJob is the Command Gateway's bridge for completing a job that
// belongs to a partitioned-write-path instance. instanceID is the job's
// owning process instance (callers already need it to resolve the Kafka
// partition key — see LookupJobOwner).
func CompleteJob(instanceID, jobID uuid.UUID, variables map[string]interface{}) error {
	cmdProducer := eventlog.GlobalCommand()
	consumer := eventlog.GlobalConsumer()
	if cmdProducer == nil || consumer == nil {
		return fmt.Errorf("partitioned write path is not configured on this instance")
	}

	if variables == nil {
		variables = map[string]interface{}{}
	}
	varsJSON, err := json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("failed to marshal variables: %w", err)
	}

	// Waits on JobCompleted (correlated by job_id), not ProcessCompleted —
	// completing a job doesn't necessarily finish the process once a
	// definition can have nodes after a decision point (Phase 2's diamond
	// shape happens to be exactly one hop from job completion to the
	// shared end event, but this bridge shouldn't assume that in general).
	// eventlog.Consumer only correlates waiters by (instanceID, eventType),
	// not job ID, so loop: pre-register the replacement waiter before
	// awaiting the current one (same register-before-produce discipline
	// Consumer.Register's own doc establishes) so a same-instance
	// JobCompleted for a different job can never be missed in the gap.
	waiter := consumer.Register(instanceID.String(), "JobCompleted")

	produceCtx, cancel := context.WithTimeout(context.Background(), bridgeTimeout)
	defer cancel()
	if err := cmdProducer.CompleteJob(produceCtx, instanceID, jobID, string(varsJSON)); err != nil {
		waiter.Cancel()
		return err
	}

	deadline := time.Now().Add(bridgeTimeout)
	for {
		next := consumer.Register(instanceID.String(), "JobCompleted")

		remaining := time.Until(deadline)
		if remaining <= 0 {
			next.Cancel()
			return fmt.Errorf("partitioned engine did not confirm job completion: timed out waiting for job %s", jobID)
		}
		env, err := waiter.Await(context.Background(), remaining)
		if err != nil {
			next.Cancel()
			return fmt.Errorf("partitioned engine did not confirm job completion: %w", err)
		}
		if jc := env.GetJobCompleted(); jc != nil && jc.JobId == jobID.String() {
			next.Cancel()
			return nil
		}
		waiter = next
	}
}

// FailJob is the Command Gateway's bridge for failing a job that belongs
// to a partitioned-write-path instance. retriesRemaining follows Zeebe's
// own convention (count to leave remaining after this failure, already
// decremented by the caller — matching service.FailJob's own parameter).
func FailJob(instanceID, jobID uuid.UUID, retriesRemaining int, errorMessage, errorCode string) error {
	cmdProducer := eventlog.GlobalCommand()
	consumer := eventlog.GlobalConsumer()
	if cmdProducer == nil || consumer == nil {
		return fmt.Errorf("partitioned write path is not configured on this instance")
	}

	// Waits on JobFailed (correlated by job_id) — already published
	// unconditionally by all three of service.FailJob's outcomes (retry
	// reset, permanent failure with incident, permanent failure with
	// boundary trigger) since Phase 0. Same pre-register-the-replacement
	// retry loop as CompleteJob's JobCompleted wait, for the same reason:
	// Consumer only correlates by (instanceID, eventType), not job ID.
	waiter := consumer.Register(instanceID.String(), "JobFailed")

	produceCtx, cancel := context.WithTimeout(context.Background(), bridgeTimeout)
	defer cancel()
	if err := cmdProducer.FailJob(produceCtx, instanceID, jobID, int32(retriesRemaining), errorMessage, errorCode); err != nil {
		waiter.Cancel()
		return err
	}

	deadline := time.Now().Add(bridgeTimeout)
	for {
		next := consumer.Register(instanceID.String(), "JobFailed")

		remaining := time.Until(deadline)
		if remaining <= 0 {
			next.Cancel()
			return fmt.Errorf("partitioned engine did not confirm job failure: timed out waiting for job %s", jobID)
		}
		env, err := waiter.Await(context.Background(), remaining)
		if err != nil {
			next.Cancel()
			return fmt.Errorf("partitioned engine did not confirm job failure: %w", err)
		}
		if jf := env.GetJobFailed(); jf != nil && jf.JobId == jobID.String() {
			next.Cancel()
			return nil
		}
		waiter = next
	}
}

// SetJobRetries is the Command Gateway's bridge for resetting a job's
// retries (and resolving any open incident for it) on a partitioned-
// write-path instance.
func SetJobRetries(instanceID, jobID uuid.UUID, retries int) error {
	cmdProducer := eventlog.GlobalCommand()
	consumer := eventlog.GlobalConsumer()
	if cmdProducer == nil || consumer == nil {
		return fmt.Errorf("partitioned write path is not configured on this instance")
	}

	// Waits on JobRetriesSet (correlated by job_id) — published
	// unconditionally by service.SetJobRetries regardless of whether an
	// open incident existed, unlike IncidentResolved.
	waiter := consumer.Register(instanceID.String(), "JobRetriesSet")

	produceCtx, cancel := context.WithTimeout(context.Background(), bridgeTimeout)
	defer cancel()
	if err := cmdProducer.SetJobRetries(produceCtx, instanceID, jobID, int32(retries)); err != nil {
		waiter.Cancel()
		return err
	}

	deadline := time.Now().Add(bridgeTimeout)
	for {
		next := consumer.Register(instanceID.String(), "JobRetriesSet")

		remaining := time.Until(deadline)
		if remaining <= 0 {
			next.Cancel()
			return fmt.Errorf("partitioned engine did not confirm retries were set: timed out waiting for job %s", jobID)
		}
		env, err := waiter.Await(context.Background(), remaining)
		if err != nil {
			next.Cancel()
			return fmt.Errorf("partitioned engine did not confirm retries were set: %w", err)
		}
		if jr := env.GetJobRetriesSet(); jr != nil && jr.JobId == jobID.String() {
			next.Cancel()
			return nil
		}
		waiter = next
	}
}

// ResolveIncident is the Command Gateway's bridge for resolving an
// incident belonging to a partitioned-write-path instance.
func ResolveIncident(instanceID, incidentID uuid.UUID) error {
	cmdProducer := eventlog.GlobalCommand()
	consumer := eventlog.GlobalConsumer()
	if cmdProducer == nil || consumer == nil {
		return fmt.Errorf("partitioned write path is not configured on this instance")
	}

	waiter := consumer.Register(instanceID.String(), "IncidentResolved")

	produceCtx, cancel := context.WithTimeout(context.Background(), bridgeTimeout)
	defer cancel()
	if err := cmdProducer.ResolveIncident(produceCtx, instanceID, incidentID); err != nil {
		waiter.Cancel()
		return err
	}

	deadline := time.Now().Add(bridgeTimeout)
	for {
		next := consumer.Register(instanceID.String(), "IncidentResolved")

		remaining := time.Until(deadline)
		if remaining <= 0 {
			next.Cancel()
			return fmt.Errorf("partitioned engine did not confirm incident resolution: timed out waiting for incident %s", incidentID)
		}
		env, err := waiter.Await(context.Background(), remaining)
		if err != nil {
			next.Cancel()
			return fmt.Errorf("partitioned engine did not confirm incident resolution: %w", err)
		}
		if ir := env.GetIncidentResolved(); ir != nil && ir.IncidentId == incidentID.String() {
			next.Cancel()
			return nil
		}
		waiter = next
	}
}

// LookupJobOwner resolves which write path owns jobID, without importing
// internal/service (see package doc). Both of this codebase's job-complete
// call sites (internal/api's REST handler, internal/zeebegrpc's RPC) call
// this first to decide whether to branch into CompleteJob above instead of
// service.CompleteJob.
func LookupJobOwner(db *sqlx.DB, jobID uuid.UUID) (instanceID uuid.UUID, writePath string, err error) {
	var row struct {
		ProcessInstanceID uuid.UUID `db:"process_instance_id"`
		WritePath         string    `db:"write_path"`
	}
	err = db.Get(&row, `
		SELECT j.process_instance_id, pd.write_path
		FROM public.jobs j
		JOIN public.process_instances pi ON pi.id = j.process_instance_id
		JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE j.id = $1
	`, jobID)
	if err != nil {
		return uuid.Nil, "", err
	}
	return row.ProcessInstanceID, row.WritePath, nil
}

// LookupIncidentOwner resolves which write path owns incidentID, the
// ResolveIncident-side counterpart of LookupJobOwner.
func LookupIncidentOwner(db *sqlx.DB, incidentID uuid.UUID) (instanceID uuid.UUID, writePath string, err error) {
	var row struct {
		ProcessInstanceID uuid.UUID `db:"process_instance_id"`
		WritePath         string    `db:"write_path"`
	}
	err = db.Get(&row, `
		SELECT i.process_instance_id, pd.write_path
		FROM public.incidents i
		JOIN public.process_instances pi ON pi.id = i.process_instance_id
		JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE i.id = $1
	`, incidentID)
	if err != nil {
		return uuid.Nil, "", err
	}
	return row.ProcessInstanceID, row.WritePath, nil
}

// LookupInstanceOwner resolves which write path owns instanceID directly
// — used by TimerRouter (timer_router.go), which starts from a timer's
// process_instance_id rather than a job or incident id.
func LookupInstanceOwner(db *sqlx.DB, instanceID uuid.UUID) (writePath string, err error) {
	err = db.Get(&writePath, `
		SELECT pd.write_path
		FROM public.process_instances pi
		JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE pi.id = $1
	`, instanceID)
	if err != nil {
		return "", err
	}
	return writePath, nil
}
