// Package retention periodically deletes finished process instances once
// they are older than a configured number of days: their historic_* rows,
// their audit_logs rows, and their event-log projections.
//
// Only data of instances that ended (historic_process_instances.ended_at)
// is ever touched. Live instances, and audit_logs rows not tied to an
// instance (logins, user/role administration), are kept forever.
//
// Disabled unless RETENTION_DAYS is set. Several GoFlow instances can run
// it at once: a Postgres advisory lock makes exactly one of them purge per
// round.
package retention

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/metrics"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// advisoryLockKey is an arbitrary constant shared by every GoFlow instance.
const advisoryLockKey int64 = 0x60f10e_7e7e

type Config struct {
	Days      int           // purge instances that ended more than Days ago; 0 = disabled
	Interval  time.Duration // time between purge rounds
	BatchSize int           // instances deleted per transaction
}

func (c Config) Enabled() bool { return c.Days > 0 }

// ConfigFromEnv reads RETENTION_DAYS (empty or 0 = disabled),
// RETENTION_INTERVAL (Go duration, default 1h) and RETENTION_BATCH_SIZE
// (default 500). Invalid values are an error rather than a silent default,
// since a typo here decides what data gets deleted.
func ConfigFromEnv(get func(string) string) (Config, error) {
	cfg := Config{Interval: time.Hour, BatchSize: 500}

	if v := get("RETENTION_DAYS"); v != "" {
		days, err := strconv.Atoi(v)
		if err != nil || days < 0 {
			return cfg, fmt.Errorf("RETENTION_DAYS must be a whole number of days >= 0, got %q", v)
		}
		cfg.Days = days
	}
	if v := get("RETENTION_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Minute {
			return cfg, fmt.Errorf("RETENTION_INTERVAL must be a duration of at least 1m (e.g. 1h, 30m), got %q", v)
		}
		cfg.Interval = d
	}
	if v := get("RETENTION_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			return cfg, fmt.Errorf("RETENTION_BATCH_SIZE must be between 1 and 10000, got %q", v)
		}
		cfg.BatchSize = n
	}
	return cfg, nil
}

// Start runs a purge round right away, then every cfg.Interval, until ctx
// is cancelled. It returns immediately; the rounds run in a goroutine.
func Start(ctx context.Context, db *sqlx.DB, cfg Config) {
	go func() {
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			if deleted, err := RunOnce(ctx, db, cfg); err != nil {
				log.Printf("retention: purge round failed: %v", err)
			} else if deleted > 0 {
				log.Printf("retention: purged %d finished process instances older than %d days", deleted, cfg.Days)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// RunOnce deletes, batch by batch, every instance that ended more than
// cfg.Days ago, and returns how many instances it deleted. It returns 0
// without doing anything if another GoFlow instance holds the purge lock.
func RunOnce(ctx context.Context, db *sqlx.DB, cfg Config) (int, error) {
	if !cfg.Enabled() {
		return 0, nil
	}

	// Advisory locks belong to a session, so hold one connection for the
	// whole round: lock, every batch, unlock.
	conn, err := db.Connx(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	var locked bool
	if err := conn.GetContext(ctx, &locked, `SELECT pg_try_advisory_lock($1)`, advisoryLockKey); err != nil {
		return 0, err
	}
	if !locked {
		return 0, nil
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)

	cutoff := time.Now().UTC().AddDate(0, 0, -cfg.Days)
	total := 0
	for {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		n, err := purgeBatch(ctx, conn, cutoff, cfg.BatchSize)
		total += n
		if err != nil {
			return total, err
		}
		if n < cfg.BatchSize {
			return total, nil
		}
	}
}

// purgeTables lists what is deleted for each purged instance, children
// before the historic_process_instances parent row. The historic_* child
// tables would also go by ON DELETE CASCADE, but deleting them explicitly
// gives accurate per-table counts.
var purgeTables = []struct {
	table  string
	column string
}{
	{"audit_logs", "process_instance_id"},
	{"eventlog_projected_jobs", "process_instance_id"},
	{"eventlog_projected_instances", "id"},
	{"historic_activity_instances", "process_instance_id"},
	{"historic_tasks", "process_instance_id"},
	{"historic_variables", "process_instance_id"},
	{"historic_incidents", "process_instance_id"},
	{"historic_process_instances", "id"},
}

// purgeBatch deletes up to batchSize expired instances in one transaction
// and returns how many it deleted.
func purgeBatch(ctx context.Context, conn *sqlx.Conn, cutoff time.Time, batchSize int) (int, error) {
	tx, err := conn.BeginTxx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var ids []uuid.UUID
	if err := tx.SelectContext(ctx, &ids, `
		SELECT id FROM historic_process_instances
		WHERE ended_at < $1
		ORDER BY ended_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED`, cutoff, batchSize); err != nil {
		return 0, fmt.Errorf("select expired instances: %w", err)
	}
	if len(ids) == 0 {
		return 0, nil
	}

	idStrings := make([]string, len(ids))
	for i, id := range ids {
		idStrings[i] = id.String()
	}
	counts := make(map[string]int64, len(purgeTables))
	for _, t := range purgeTables {
		res, err := tx.ExecContext(ctx,
			fmt.Sprintf(`DELETE FROM %s WHERE %s = ANY($1::uuid[])`, t.table, t.column),
			pq.Array(idStrings))
		if err != nil {
			return 0, fmt.Errorf("delete from %s: %w", t.table, err)
		}
		counts[t.table], _ = res.RowsAffected()
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	for table, n := range counts {
		metrics.RetentionDeletedRows.WithLabelValues(table).Add(float64(n))
	}
	return len(ids), nil
}
