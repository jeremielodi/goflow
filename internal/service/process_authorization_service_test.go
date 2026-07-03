package service

import (
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func newMockDB(t *testing.T) (*sqlx.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return sqlx.NewDb(db, "postgres"), mock
}

var (
	isRestrictedRe = regexp.QuoteMeta("SELECT COUNT(*) FROM public.process_permissions WHERE process_key = $1")
	hasGrantRe     = "SELECT permission FROM public\\.process_permissions"
	globalBypassRe = regexp.QuoteMeta("JOIN public.actions a ON a.id = ra.action_id")
)

func TestCanAccessProcess_UnrestrictedAlwaysAllows(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery(isRestrictedRe).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	allowed, err := CanAccessProcess(db, uuid.New(), "myKey", "VIEW")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Error("expected unrestricted process to always be accessible")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCanAccessProcess_RestrictedNoGrantDenied(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery(isRestrictedRe).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(hasGrantRe).WillReturnRows(sqlmock.NewRows([]string{"permission"}))
	mock.ExpectQuery(globalBypassRe).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	allowed, err := CanAccessProcess(db, uuid.New(), "myKey", "VIEW")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Error("expected a restricted process with no grant and no bypass action to deny access")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCanAccessProcess_RestrictedDirectGrantAllows(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery(isRestrictedRe).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(hasGrantRe).WillReturnRows(sqlmock.NewRows([]string{"permission"}).AddRow("VIEW"))

	allowed, err := CanAccessProcess(db, uuid.New(), "myKey", "VIEW")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Error("expected a direct VIEW grant to allow VIEW access")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// The UNION in UserHasProcessPermission covers both direct user grants and
// grants held via a role (see process_permission_repository.go) — from the
// caller's side both surface identically as a returned permission row, so
// this exercises the same "grant found" contract as the role-join half of
// that query. examples/tests/23_admin_and_permissions.test.ts exercises the
// real role-join path end to end against a real database.
func TestCanAccessProcess_RestrictedGrantViaRoleAllows(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery(isRestrictedRe).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(hasGrantRe).WillReturnRows(sqlmock.NewRows([]string{"permission"}).AddRow("START"))

	allowed, err := CanAccessProcess(db, uuid.New(), "myKey", "START")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Error("expected a role-held START grant to allow START access")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCanAccessProcess_ManageImpliesStartAndView(t *testing.T) {
	for _, level := range []string{"VIEW", "START", "MANAGE"} {
		db, mock := newMockDB(t)
		mock.ExpectQuery(isRestrictedRe).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
		mock.ExpectQuery(hasGrantRe).WillReturnRows(sqlmock.NewRows([]string{"permission"}).AddRow("MANAGE"))

		allowed, err := CanAccessProcess(db, uuid.New(), "myKey", level)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Errorf("expected a MANAGE grant to satisfy a %s check", level)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	}
}

func TestCanAccessProcess_GlobalBypassActionAllows(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery(isRestrictedRe).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(hasGrantRe).WillReturnRows(sqlmock.NewRows([]string{"permission"}))
	mock.ExpectQuery(globalBypassRe).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	allowed, err := CanAccessProcess(db, uuid.New(), "myKey", "MANAGE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !allowed {
		t.Error("expected CAN_MANAGE_DEPLOY_PROCESS to bypass a missing per-resource grant")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
