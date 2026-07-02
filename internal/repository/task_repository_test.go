package repository

import (
	"database/sql/driver"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func newMockTaskRepo(t *testing.T) (*TaskRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sqlxDB := sqlx.NewDb(db, "postgres")
	return NewTaskRepository(sqlxDB, nil), mock
}

func taskRow() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "process_instance_id", "execution_id", "task_definition_key",
		"task_name", "assignee", "candidate_group", "status", "priority",
		"form_data", "created_at", "claimed_at", "completed_at",
	}).AddRow(
		uuid.New(), uuid.New(), nil, "ReviewTask",
		"Review order", nil, nil, "created", 50,
		nil, time.Now(), nil, nil,
	)
}

// Regression test: GET /engine-rest/tasks used to return every task
// regardless of status, so a completed task (with completedAt set) looked
// identical to an open one in the frontend — no status filtering happened
// anywhere. FindAll's excludeStatuses parameter is what task_controller.go's
// GetTasks now relies on to default to "open tasks only", matching Camunda
// 7's actual GET /task semantics.
func TestFindAll_ExcludeStatuses_DefaultOmitsCompletedAndCancelled(t *testing.T) {
	repo, mock := newMockTaskRepo(t)

	mock.ExpectQuery(regexp.QuoteMeta("status NOT IN ($1, $2)")).
		WithArgs("completed", "cancelled").
		WillReturnRows(taskRow())

	tasks, err := repo.FindAll(map[string]interface{}{}, "completed", "cancelled")
	if err != nil {
		t.Fatalf("FindAll returned error: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// An explicit ?status= filter must NOT be overridden by the default
// exclusion — an admin explicitly asking for completed tasks should still
// get them back.
func TestFindAll_ExcludeStatuses_SkippedWhenStatusExplicitlyFiltered(t *testing.T) {
	repo, mock := newMockTaskRepo(t)

	mock.ExpectQuery(regexp.QuoteMeta("status = $1")).
		WithArgs(driver.Value("completed")).
		WillReturnRows(taskRow())

	_, err := repo.FindAll(map[string]interface{}{"status": "completed"}, "completed", "cancelled")
	if err != nil {
		t.Fatalf("FindAll returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// The v2 search endpoint calls FindAll with no excludeStatuses at all, and
// must keep returning tasks of every status by default (Camunda 8's search
// API is neutral — callers filter explicitly via the `state` field).
func TestFindAll_NoExclusions_ReturnsEveryStatus(t *testing.T) {
	repo, mock := newMockTaskRepo(t)

	mock.ExpectQuery(regexp.QuoteMeta("FROM public.tasks")).
		WillReturnRows(taskRow())

	_, err := repo.FindAll(map[string]interface{}{})
	if err != nil {
		t.Fatalf("FindAll returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
