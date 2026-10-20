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
// per-instance runtime colocation group, jobs and timers included (claimed
// without FOR UPDATE, see repository.SetDistributedClaims) — see
// the design doc.
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
// process_instance_id and colocated with it). audit_logs,
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
	// Jobs and timers: claimed without FOR UPDATE on Citus (see
	// repository.SetDistributedClaims), so they can be distributed too.
	{"jobs", "process_instance_id"},
	{"timer_jobs", "process_instance_id"},
	{"scope_variables", "process_instance_id"},
}

// keyedByInstance makes a table's primary key (process_instance_id, id):
// Citus requires the distribution column in every unique constraint. On
// plain Postgres the key stays id alone.
func keyedByInstance(db *sqlx.DB, table string) error {
	var cols int
	if err := db.Get(&cols, `
		SELECT COALESCE(array_length(conkey, 1), 0) FROM pg_constraint
		WHERE conrelid = $1::regclass AND contype = 'p'`, "public."+table); err != nil {
		return fmt.Errorf("citus: read the primary key of %s: %w", table, err)
	}
	if cols >= 2 {
		return nil
	}
	_, err := db.Exec(fmt.Sprintf(`ALTER TABLE public.%[1]s DROP CONSTRAINT IF EXISTS %[1]s_pkey, ADD PRIMARY KEY (process_instance_id, id)`, table))
	if err != nil {
		return fmt.Errorf("citus: re-key %s by process instance: %w", table, err)
	}
	return nil
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

	// Citus won't distribute a table with triggers, and a NOTIFY fired on
	// a worker would never reach the coordinator's LISTEN anyway: the
	// engine sends the variables notifications itself instead
	// (repository.SetAppVariableNotify).
	for table, trigger := range map[string]string{"variables": "variables_notify", "scope_variables": "scope_variables_notify"} {
		var exists bool
		if err := db.Get(&exists, `SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2)`, "public."+table, trigger); err != nil {
			return fmt.Errorf("citus: check trigger %s: %w", trigger, err)
		}
		if !exists {
			continue
		}
		if _, err := db.Exec(fmt.Sprintf(`DROP TRIGGER %s ON public.%s`, trigger, table)); err != nil {
			return fmt.Errorf("citus: drop trigger %s: %w", trigger, err)
		}
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

		if t.name == "jobs" || t.name == "timer_jobs" {
			if err := keyedByInstance(db, t.name); err != nil {
				return err
			}
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

// EnsureLocalTablesInMetadata adds every table still neither reference
// nor distributed (subscriptions, boundary events, audit, history…) to
// Citus's metadata as a "citus local table": they stay on the
// coordinator, and Citus can then plan the queries joining them with the
// distributed tables (a join with a table it doesn't know is refused).
func EnsureLocalTablesInMetadata(db *sqlx.DB, coordinatorHost string, coordinatorPort int) error {
	// Citus local tables need the coordinator in the metadata (group 0).
	var registered bool
	if err := db.Get(&registered, `SELECT EXISTS (SELECT 1 FROM pg_dist_node WHERE groupid = 0)`); err != nil {
		return fmt.Errorf("citus: check the coordinator node: %w", err)
	}
	if !registered {
		if _, err := db.Exec(`SELECT citus_set_coordinator_host($1, $2)`, coordinatorHost, coordinatorPort); err != nil {
			return fmt.Errorf("citus: register the coordinator %s:%d: %w", coordinatorHost, coordinatorPort, err)
		}
	}
	// The coordinator now holds placements too: copy the reference tables
	// to it in a transaction of their own (Citus won't do it inside the
	// one adding a table that references them).
	if _, err := db.Exec(`SELECT replicate_reference_tables(shard_transfer_mode => 'block_writes')`); err != nil {
		return fmt.Errorf("citus: replicate the reference tables: %w", err)
	}

	var tables []string
	if err := db.Select(&tables, `
		SELECT c.relname FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'r'
		  AND NOT EXISTS (SELECT 1 FROM citus_tables t WHERE t.table_name = c.oid::regclass)
		ORDER BY c.relname`); err != nil {
		return fmt.Errorf("citus: list local tables: %w", err)
	}
	for _, table := range tables {
		// A previous table's cascade (foreign keys) may have added it.
		var known bool
		if err := db.Get(&known, `SELECT EXISTS (SELECT 1 FROM citus_tables WHERE table_name = $1::regclass)`, "public."+table); err != nil {
			return fmt.Errorf("citus: check %s: %w", table, err)
		}
		if known {
			continue
		}
		if _, err := db.Exec(`SELECT citus_add_local_table_to_metadata($1::regclass, cascade_via_foreign_keys => true)`, "public."+table); err != nil {
			return fmt.Errorf("citus: add %s to the metadata: %w", table, err)
		}
	}
	return nil
}
