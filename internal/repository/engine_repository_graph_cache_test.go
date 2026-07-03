package repository

import (
	"encoding/json"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func newMockEngineRepo(t *testing.T) (*EngineRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sqlxDB := sqlx.NewDb(db, "postgres")
	return NewEngineRepository(sqlxDB, nil), mock
}

// Regression: the graph cache must be keyed by process_definition_id, not
// process_instance_id, so two instances of the same definition share one
// cached graph and only trigger one DB query between them.
func TestGetProcessGraphByInstanceID_DedupesAcrossInstancesOfSameDefinition(t *testing.T) {
	ClearGraphCache()
	repo, mock := newMockEngineRepo(t)

	defID := uuid.New()
	instanceA := uuid.New()
	instanceB := uuid.New()
	graphJSON, _ := json.Marshal(map[string]interface{}{"nodes": map[string]interface{}{}})

	mock.ExpectQuery(regexp.QuoteMeta("SELECT pd.parsed_graph, pd.id AS process_definition_id")).
		WithArgs(instanceA).
		WillReturnRows(sqlmock.NewRows([]string{"parsed_graph", "process_definition_id"}).AddRow(graphJSON, defID))

	graphA, err := repo.GetProcessGraphByInstanceID(instanceA)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT pd.parsed_graph, pd.id AS process_definition_id")).
		WithArgs(instanceB).
		WillReturnRows(sqlmock.NewRows([]string{"parsed_graph", "process_definition_id"}).AddRow(graphJSON, defID))

	graphB, err := repo.GetProcessGraphByInstanceID(instanceB)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if graphA != graphB {
		t.Errorf("expected instances of the same definition to share the cached graph pointer, got %p vs %p", graphA, graphB)
	}

	// A second call for instanceA must be a pure cache hit — no new
	// expectation registered, so an extra query here would fail the mock.
	graphA2, err := repo.GetProcessGraphByInstanceID(instanceA)
	if err != nil {
		t.Fatalf("unexpected error on cache hit: %v", err)
	}
	if graphA2 != graphA {
		t.Errorf("expected cache hit to return the same graph pointer")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

func TestGetProcessGraphByInstanceID_EvictsOnOverflow(t *testing.T) {
	ClearGraphCache()
	repo, mock := newMockEngineRepo(t)
	graphJSON, _ := json.Marshal(map[string]interface{}{"nodes": map[string]interface{}{}})

	for i := 0; i < graphCacheMaxEntries+5; i++ {
		instanceID := uuid.New()
		defID := uuid.New()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pd.parsed_graph, pd.id AS process_definition_id")).
			WithArgs(instanceID).
			WillReturnRows(sqlmock.NewRows([]string{"parsed_graph", "process_definition_id"}).AddRow(graphJSON, defID))
		if _, err := repo.GetProcessGraphByInstanceID(instanceID); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	graphCacheMu.RLock()
	size := len(graphCache)
	graphCacheMu.RUnlock()
	if size > graphCacheMaxEntries {
		t.Errorf("expected graphCache to stay bounded at %d entries, got %d", graphCacheMaxEntries, size)
	}
}
