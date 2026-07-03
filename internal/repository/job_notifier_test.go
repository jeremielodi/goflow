package repository

import (
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

// These tests exercise wakeLocal directly (the pure in-memory broadcast
// logic) rather than NotifyJobAvailable, which now requires a live Postgres
// connection to issue NOTIFY — that cross-instance path is covered by
// TestNotifyJobAvailable_IssuesPgNotify below (via sqlmock) and by the e2e
// suite against a real database.

func TestJobWakeChannel_NotifyWakesExistingWaiter(t *testing.T) {
	jobType := "wake-test-" + t.Name()
	wake := JobWakeChannel(jobType)

	woke := make(chan struct{})
	go func() {
		<-wake
		close(woke)
	}()

	// Give the goroutine a moment to start waiting before notifying.
	time.Sleep(10 * time.Millisecond)
	wakeLocal(jobType)

	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("waiter was not woken within 1s of wakeLocal")
	}
}

func TestJobWakeChannel_UnrelatedTypeIsNotWoken(t *testing.T) {
	watched := "wake-test-watched-" + t.Name()
	other := "wake-test-other-" + t.Name()
	wake := JobWakeChannel(watched)

	wakeLocal(other)

	select {
	case <-wake:
		t.Fatal("wake channel for a different job type fired")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestJobWakeChannel_FreshChannelAfterNotify(t *testing.T) {
	jobType := "wake-test-fresh-" + t.Name()
	first := JobWakeChannel(jobType)
	wakeLocal(jobType)

	select {
	case <-first:
	default:
		t.Fatal("expected the pre-notify channel to be closed")
	}

	second := JobWakeChannel(jobType)
	select {
	case <-second:
		t.Fatal("expected a fresh, still-open channel to be installed after notify")
	default:
	}
}

func TestNotifyJobAvailable_IssuesPgNotify(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()
	sqlxDB := sqlx.NewDb(db, "postgres")

	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_notify($1, $2)")).
		WithArgs(JobAvailableChannel, "my-job-type").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := NotifyJobAvailable(sqlxDB, "my-job-type"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}
