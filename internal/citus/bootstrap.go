// Package citus provides an idempotent, boot-time bootstrap for GoFlow's
// optional Citus-backed distributed-storage deployment (see
// architecture/citus-distributed-storage-design.md and
// docker-compose.citus.yml). Mirrors internal/eventlog.EnsureTopics's
// established pattern in this codebase: check current state before acting,
// safe to run on every restart, fatal-on-failure at boot rather than a
// silent partial setup.
//
// Phase 1 (EnsureReferenceTables) converts global config/lookup tables to
// Citus reference tables. Phase 2 (EnsureDistributedTables) shards the
// per-instance runtime colocation group. jobs/timer_jobs and their global
// FOR UPDATE SKIP LOCKED claim queries remain explicitly out of scope —
// see the design doc for why.
package citus

import (
	"fmt"
	"net"
	"strconv"

	"github.com/jmoiron/sqlx"
)

// referenceTables are read on nearly every request (auth, RBAC, definition
// resolution) and have no process_instance_id — replicating them to every
// Citus node needs zero query changes anywhere, since a reference table
// behaves identically to a normal table for every existing query pattern.
var referenceTables = []string{
	"users",
	"roles",
	"user_roles",
	"actions",
	"role_actions",
	"deployments",
	"process_definitions",
	"dmn_decisions",
	"forms",
	"process_permissions",
	"zeebe_keys",
}

// EnsureReferenceTables registers every worker in workers (host:port pairs,
// skipped if already known via pg_dist_node) and converts every table in
// referenceTables to a Citus reference table (skipped if already converted,
// checked via the citus_tables view). Must be called against a Citus
// coordinator — calling it against a plain Postgres database fails fast
// with a clear error rather than a confusing "function does not exist".
func EnsureReferenceTables(db *sqlx.DB, workers []string) error {
	var hasCitus bool
	if err := db.Get(&hasCitus, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus')`); err != nil {
		return fmt.Errorf("citus: check for citus extension: %w", err)
	}
	if !hasCitus {
		return fmt.Errorf("citus: CITUS_ENABLED is set but the citus extension is not active on this database — is DB_HOST pointed at a Citus coordinator?")
	}

	for _, addr := range workers {
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("citus: invalid CITUS_WORKERS entry %q: %w", addr, err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("citus: invalid port in CITUS_WORKERS entry %q: %w", addr, err)
		}

		var alreadyKnown bool
		if err := db.Get(&alreadyKnown, `SELECT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodename = $1 AND nodeport = $2)`, host, port); err != nil {
			return fmt.Errorf("citus: check worker node %s: %w", addr, err)
		}
		if alreadyKnown {
			continue
		}
		if _, err := db.Exec(`SELECT citus_add_node($1, $2)`, host, port); err != nil {
			return fmt.Errorf("citus: add worker node %s: %w", addr, err)
		}
	}

	for _, table := range referenceTables {
		var alreadyReference bool
		if err := db.Get(&alreadyReference, `
			SELECT EXISTS (
				SELECT 1 FROM citus_tables
				WHERE table_name = $1::regclass AND citus_table_type = 'reference'
			)
		`, "public."+table); err != nil {
			return fmt.Errorf("citus: check reference table status for %s: %w", table, err)
		}
		if alreadyReference {
			continue
		}
		if _, err := db.Exec(`SELECT create_reference_table($1::regclass)`, "public."+table); err != nil {
			return fmt.Errorf("citus: convert %s to a reference table: %w", table, err)
		}
	}

	return nil
}

// distributedTables lists the per-instance colocation group, in dependency
// order (process_instances first, as the colocation-group root distributed
// by its own id — everything else follows, distributed by
// process_instance_id and colocated with it). jobs, timer_jobs, audit_logs,
// event_subscriptions, boundary_events, event_subprocesses,
// multi_instance_children, and execution_variables are deliberately absent
// — see architecture/citus-distributed-storage-design.md for why each
// stays local this phase.
var distributedTables = []struct {
	name               string
	distributionColumn string
}{
	{"process_instances", "id"},
	{"executions", "process_instance_id"},
	{"tasks", "process_instance_id"},
	{"variables", "process_instance_id"},
	{"gateway_state", "process_instance_id"},
	{"multi_instance_executions", "process_instance_id"},
	{"incidents", "process_instance_id"},
}

// EnsureDistributedTables converts every table in distributedTables into a
// Citus distributed table, all colocated with process_instances (skipped
// per-table if already converted, checked via the citus_tables view — same
// idempotency convention as EnsureReferenceTables). Must run after
// EnsureReferenceTables, though the order doesn't matter functionally;
// callers should still call reference tables first since process_instances
// has an FK to the (by-then-already-reference) process_definitions table.
func EnsureDistributedTables(db *sqlx.DB) error {
	var hasCitus bool
	if err := db.Get(&hasCitus, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus')`); err != nil {
		return fmt.Errorf("citus: check for citus extension: %w", err)
	}
	if !hasCitus {
		return fmt.Errorf("citus: CITUS_ENABLED is set but the citus extension is not active on this database — is DB_HOST pointed at a Citus coordinator?")
	}

	for _, t := range distributedTables {
		var alreadyDistributed bool
		if err := db.Get(&alreadyDistributed, `
			SELECT EXISTS (
				SELECT 1 FROM citus_tables
				WHERE table_name = $1::regclass AND citus_table_type = 'distributed'
			)
		`, "public."+t.name); err != nil {
			return fmt.Errorf("citus: check distributed table status for %s: %w", t.name, err)
		}
		if alreadyDistributed {
			continue
		}

		var err error
		if t.name == "process_instances" {
			_, err = db.Exec(`SELECT create_distributed_table($1::regclass, $2, colocate_with => 'none')`, "public."+t.name, t.distributionColumn)
		} else {
			_, err = db.Exec(`SELECT create_distributed_table($1::regclass, $2, colocate_with => 'process_instances')`, "public."+t.name, t.distributionColumn)
		}
		if err != nil {
			return fmt.Errorf("citus: distribute %s by %s: %w", t.name, t.distributionColumn, err)
		}
	}

	return nil
}
