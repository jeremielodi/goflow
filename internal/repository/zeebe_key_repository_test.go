package repository

import (
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func newMockZeebeKeyRepo(t *testing.T) (*ZeebeKeyRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sqlxDB := sqlx.NewDb(db, "postgres")
	return NewZeebeKeyRepository(sqlxDB), mock
}

func TestGetOrAssignKey_FirstAssignment(t *testing.T) {
	repo, mock := newMockZeebeKeyRepo(t)
	resourceID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.zeebe_keys")).
		WithArgs("job", resourceID).
		WillReturnRows(sqlmock.NewRows([]string{"key"}).AddRow(int64(1)))

	key, err := repo.GetOrAssignKey("job", resourceID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != 1 {
		t.Errorf("expected key 1, got %d", key)
	}
}

// Regression: a second call for the same resource must return the SAME
// key, not fail or assign a new one — this is what lets a client hand a
// job key from ActivateJobs back to CompleteJob/FailJob later, including
// after the row already exists from a prior call.
func TestGetOrAssignKey_ConflictReturnsExistingKey(t *testing.T) {
	repo, mock := newMockZeebeKeyRepo(t)
	resourceID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.zeebe_keys")).
		WithArgs("job", resourceID).
		WillReturnRows(sqlmock.NewRows([]string{"key"}).AddRow(int64(42)))

	key, err := repo.GetOrAssignKey("job", resourceID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != 42 {
		t.Errorf("expected the existing key 42 to be returned on conflict, got %d", key)
	}
}

func TestResolveKey(t *testing.T) {
	repo, mock := newMockZeebeKeyRepo(t)
	resourceID := uuid.New()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT resource_id FROM public.zeebe_keys")).
		WithArgs("process_instance", int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"resource_id"}).AddRow(resourceID))

	got, err := repo.ResolveKey("process_instance", 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != resourceID {
		t.Errorf("expected %s, got %s", resourceID, got)
	}
}
