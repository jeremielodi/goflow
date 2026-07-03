// internal/repository/analytics_repository.go
package repository

import (
	"time"

	"github.com/jmoiron/sqlx"
)

// AnalyticsRepository backs the GET /engine-rest/analytics/process-stats
// endpoint — a distinct file/type from HistoricArchiveRepository (which
// owns archival writes, not reporting reads) and ProcessInstanceRepository
// (already large, and its own GetStatistics is live-table-only with no
// historic merge — wrong shape for this, left untouched).
type AnalyticsRepository struct {
	db *sqlx.DB
}

func NewAnalyticsRepository(db *sqlx.DB) *AnalyticsRepository {
	return &AnalyticsRepository{db: db}
}

// LiveStatusCount is one (process_key, status) group from the live table —
// only running/suspended instances linger there in practice, since
// ArchiveAndDeleteProcessInstance moves completed/terminated ones out
// promptly.
type LiveStatusCount struct {
	ProcessKey string `db:"process_key"`
	Status     string `db:"status"`
	Count      int    `db:"count"`
}

func (r *AnalyticsRepository) GetLiveStatusCounts(processKey string) ([]LiveStatusCount, error) {
	var rows []LiveStatusCount
	err := r.db.Select(&rows, `
		SELECT pd.process_key AS process_key, pi.status AS status, COUNT(*) AS count
		FROM public.process_instances pi
		JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
		WHERE ($1 = '' OR pd.process_key = $1)
		GROUP BY pd.process_key, pi.status
	`, processKey)
	return rows, err
}

// HistoricStatusStats is one (process_definition_key, state) group from
// historic_process_instances, with duration percentiles computed by
// Postgres itself (a real aggregate, not app-side math).
type HistoricStatusStats struct {
	ProcessKey        string   `db:"process_key"`
	State             string   `db:"state"`
	Count             int      `db:"count"`
	AvgDurationMillis *float64 `db:"avg_duration_millis"`
	P50DurationMillis *float64 `db:"p50_duration_millis"`
	P95DurationMillis *float64 `db:"p95_duration_millis"`
}

func (r *AnalyticsRepository) GetHistoricStatusStats(processKey string, from, to *time.Time) ([]HistoricStatusStats, error) {
	var rows []HistoricStatusStats
	err := r.db.Select(&rows, `
		SELECT
			process_definition_key AS process_key,
			state,
			COUNT(*) AS count,
			AVG(duration_millis) AS avg_duration_millis,
			PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY duration_millis) AS p50_duration_millis,
			PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_millis) AS p95_duration_millis
		FROM public.historic_process_instances
		WHERE ($1 = '' OR process_definition_key = $1)
		  AND ($2::timestamp IS NULL OR started_at >= $2)
		  AND ($3::timestamp IS NULL OR started_at < $3)
		GROUP BY process_definition_key, state
	`, processKey, from, to)
	return rows, err
}

// IncidentCount is one process key's total incident count, summed across
// still-live instances (incidents joined through process_instances) and
// archived ones (historic_incidents, denormalized key) — see
// migrations/009_historic_incidents.sql for why both sources exist.
type IncidentCount struct {
	ProcessKey string `db:"process_key"`
	Count      int    `db:"count"`
}

func (r *AnalyticsRepository) GetIncidentCounts(processKey string) ([]IncidentCount, error) {
	var rows []IncidentCount
	err := r.db.Select(&rows, `
		SELECT process_key, SUM(count)::int AS count FROM (
			SELECT pd.process_key AS process_key, COUNT(*) AS count
			FROM public.incidents i
			JOIN public.process_instances pi ON pi.id = i.process_instance_id
			JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
			WHERE ($1 = '' OR pd.process_key = $1)
			GROUP BY pd.process_key

			UNION ALL

			SELECT process_definition_key AS process_key, COUNT(*) AS count
			FROM public.historic_incidents
			WHERE ($1 = '' OR process_definition_key = $1)
			GROUP BY process_definition_key
		) combined
		GROUP BY process_key
	`, processKey)
	return rows, err
}

// ThroughputPoint is one day's started/completed/terminated counts.
type ThroughputPoint struct {
	Date       time.Time `db:"date"`
	Started    int       `db:"started"`
	Completed  int       `db:"completed"`
	Terminated int       `db:"terminated"`
}

// GetStartedPerDay unions the live and historic tables — an instance is
// "started" whether or not it has finished yet. Keyed by a formatted date
// string ("2006-01-02"), not time.Time — a time.Time carries Location/
// monotonic metadata that makes two values representing the same calendar
// day compare unequal as map keys (e.g. one built via time.Now() using the
// server's Location, the other scanned back from Postgres in UTC), which
// silently drops every row when merged against a separately-built series.
func (r *AnalyticsRepository) GetStartedPerDay(processKey string, from, to time.Time) (map[string]int, error) {
	type row struct {
		Date  time.Time `db:"date"`
		Count int       `db:"count"`
	}
	var rows []row
	err := r.db.Select(&rows, `
		SELECT date_trunc('day', started_at)::date AS date, COUNT(*) AS count
		FROM (
			SELECT pi.started_at AS started_at, pd.process_key AS process_key
			FROM public.process_instances pi
			JOIN public.process_definitions pd ON pd.id = pi.process_definition_id
			UNION ALL
			SELECT started_at, process_definition_key AS process_key
			FROM public.historic_process_instances
		) combined
		WHERE ($1 = '' OR process_key = $1)
		  AND started_at >= $2 AND started_at < $3
		GROUP BY 1
	`, processKey, from, to)
	if err != nil {
		return nil, err
	}
	result := make(map[string]int, len(rows))
	for _, r := range rows {
		result[r.Date.Format("2006-01-02")] = r.Count
	}
	return result, nil
}

// GetFinishedPerDay buckets by ended_at from historic_process_instances
// only — only finished instances have a non-null ended_at, and finished
// instances live there, not in the live table, once archived. See
// GetStartedPerDay for why this is keyed by formatted date string rather
// than time.Time.
func (r *AnalyticsRepository) GetFinishedPerDay(processKey string, from, to time.Time) (map[string]map[string]int, error) {
	type row struct {
		Date  time.Time `db:"date"`
		State string    `db:"state"`
		Count int       `db:"count"`
	}
	var rows []row
	err := r.db.Select(&rows, `
		SELECT date_trunc('day', ended_at)::date AS date, state, COUNT(*) AS count
		FROM public.historic_process_instances
		WHERE ($1 = '' OR process_definition_key = $1)
		  AND ended_at >= $2 AND ended_at < $3
		GROUP BY 1, 2
	`, processKey, from, to)
	if err != nil {
		return nil, err
	}
	result := make(map[string]map[string]int, len(rows))
	for _, r := range rows {
		key := r.Date.Format("2006-01-02")
		if result[key] == nil {
			result[key] = make(map[string]int)
		}
		result[key][r.State] = r.Count
	}
	return result, nil
}
