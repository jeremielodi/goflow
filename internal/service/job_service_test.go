package service

import (
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
)

// Regression: Zeebe's FailJobRequest.retries is the count to leave
// REMAINING after this failure, unlike Camunda 7's "current retry count"
// convention — FailJob must treat retriesRemaining as already-final and
// never decrement it itself. This test exercises the retriesRemaining<=0
// boundary directly, matching what a Zeebe client sends on its last retry.
func TestFailJob_RetriesExhausted_PermanentlyFailsAndCreatesIncident(t *testing.T) {
	db, mock := newMockDB(t)

	jobID := uuid.New()
	execID := uuid.New()
	instID := uuid.New()

	mock.ExpectBegin()

	jobRows := sqlmock.NewRows([]string{"execution_id", "process_instance_id", "current_element_id", "status", "retries"}).
		AddRow(execID, instID, "reviewTask", "locked", 1)
	mock.ExpectQuery(regexp.QuoteMeta("j.execution_id")).WithArgs(jobID).WillReturnRows(jobRows)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE public.jobs")).
		WithArgs("boom", "", jobID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.incidents")).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "process_instance_id", "job_id", "incident_type", "activity_id",
			"error_message", "error_code", "state", "created_at", "resolved_at",
		}).AddRow(uuid.New(), instID, jobID, "failedExternalTask", "reviewTask", "boom", "", "open", time.Now(), nil))

	mock.ExpectCommit()

	result, err := FailJob(db, jobID, 0, "boom", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.PermanentlyFailed {
		t.Error("expected retriesRemaining=0 to permanently fail the job")
	}
	if result.BoundaryTriggered {
		t.Error("expected no boundary event without an error code")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// Regression companion to the above: a positive retriesRemaining must
// reset the job for retry, not fail it permanently — this is the other
// side of the boundary FailJob's caller (ExternalTaskController /
// zeebegrpc.Server) relies on to pick the right response message.
func TestFailJob_RetriesRemaining_DoesNotPermanentlyFail(t *testing.T) {
	db, mock := newMockDB(t)

	jobID := uuid.New()
	execID := uuid.New()
	instID := uuid.New()

	mock.ExpectBegin()

	jobRows := sqlmock.NewRows([]string{"execution_id", "process_instance_id", "current_element_id", "status", "retries"}).
		AddRow(execID, instID, "reviewTask", "locked", 3)
	mock.ExpectQuery(regexp.QuoteMeta("j.execution_id")).WithArgs(jobID).WillReturnRows(jobRows)

	// ResetJobForRetryTx goes through pkg/database's named-parameter
	// adapter rather than a plain positional query — match loosely by
	// the table name rather than the exact rebound SQL text.
	mock.ExpectExec("UPDATE public\\.jobs").WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	result, err := FailJob(db, jobID, 2, "transient error", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.PermanentlyFailed {
		t.Error("expected a positive retriesRemaining to NOT permanently fail the job")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
