# GoFlow — Citus-Backed Distributed Storage (Design Scope)

Status: **complete (2026-10-20)** — reference tables, the per-instance
colocation group (jobs and timers included, section 5), local tables in
the metadata, full regression on a real cluster. Phases 1–2 shipped
2026-07-05. This is a separate architectural axis from
`event-sourced-kafka-design.md` — that document is about the *write path*
(how a command reaches Postgres); this one is about *storage itself* (how
many Postgres nodes the data lives on). They're complementary, not
alternatives: the long-term direction is a Kafka-partitioned write path
whose partition key also determines Citus shard placement, so that a given
process instance's command ownership and storage ownership line up — see
"Where this is headed" at the end.

## 1. Why this exists

The engine runs on one shared Postgres primary. That's the single real
scaling ceiling versus Zeebe's partitioned, no-shared-database architecture
(see `readme.md`'s Comparison section). Citus is a Postgres extension that
shards tables across multiple Postgres nodes while keeping a single logical
SQL interface — no application code needs to become "shard-aware" to issue
a normal query, unlike hand-rolled sharding.

## 2. Phase 1 — infrastructure + reference tables

Auditing the schema (every table, its FKs, and whether it carries a
`process_instance_id`) split it cleanly into two groups:

**Global config/lookup tables** — `process_definitions`, `deployments`,
`users`, `roles`, `user_roles`, `actions`, `role_actions`,
`process_permissions`, `zeebe_keys`, `dmn_decisions`, `forms`. No
`process_instance_id`, read on nearly every request (auth, RBAC, resolving
a process instance to its definition graph). These are the textbook case
for a **Citus reference table**: replicated in full to every node, and a
reference table behaves *identically* to a normal table for every existing
query pattern (joins, FKs, arbitrary `WHERE` clauses) — genuinely zero
query-code changes needed anywhere.

**Per-instance runtime tables** — `executions`, `tasks`, `incidents`, etc.
— all carry `process_instance_id` and look like natural **distributed
table** candidates. Phase 1 deliberately stopped short of these (see
Phase 2 below for what it actually took).

### What shipped

- **`docker-compose.citus.yml`** — a new, opt-in Compose *override* file.
  `docker compose -f docker-compose.yaml -f docker-compose.citus.yml up`
  brings up a `citus-coordinator` plus `citus-worker-1`/`citus-worker-2`
  (all `citusdata/citus:12.1`, wire-compatible with plain Postgres — no
  driver/connection-pooling changes needed anywhere in the Go app, which
  already uses exactly one `*sqlx.DB` pool everywhere including the
  partitioned write path). Plain `docker compose up` (no override) is
  completely untouched — the default path stays on the original single
  `postgres` service. The base file's `postgres` service does still start
  alongside Citus under the override (Compose merges services additively;
  cleanly excluding it would need version-dependent profile/depends_on
  tricks) — harmless, just unused once `goflow`'s `DB_HOST` points at
  `citus-coordinator` instead.
- **`database/00_citus_extension.sql`** — a new file in the existing
  `database/` folder, guarded by
  `IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'citus')`
  so it silently no-ops on the plain `postgres:16` image and only takes
  effect on the Citus image. **`database/citus-worker-init/00_extension.sql`**
  — a minimal, worker-only init script (just `CREATE EXTENSION citus;`);
  workers don't need the full app schema, since Citus pushes
  distributed/reference table DDL to workers itself at
  `create_reference_table()`/`create_distributed_table()` time.
- **`internal/citus/bootstrap.go`** — `EnsureReferenceTables(db, workers)`,
  mirroring `internal/eventlog.EnsureTopics`'s established pattern in this
  codebase: idempotent (checks `pg_dist_node` before `citus_add_node`,
  checks the `citus_tables` view before `create_reference_table`), safe to
  run on every boot/restart, fails fast with a clear error if
  `CITUS_ENABLED=true` but the citus extension isn't actually active
  (e.g. `DB_HOST` still points at a plain Postgres). Converts exactly the
  11 reference-table candidates listed above — nothing else.
- **`main.go`** — wired right after `database.NewPostgres(util)` (the
  earliest point the DB handle exists), gated on `CITUS_ENABLED` — unset
  (the default) is a complete no-op, matching `KAFKA_BROKERS`'s existing
  gating convention exactly.

## 3. Phase 2 — the per-instance colocation group

Asked to continue into the harder, higher-value part: sharding the actual
per-instance runtime tables. Research before writing code found the
originally-feared blocker ("add `process_instance_id` to every `UPDATE`/
`DELETE` WHERE clause across a dozen-plus call sites") was wrong in an
important way — **that's a performance optimization, not a correctness
requirement.** Citus supports `UPDATE`/`DELETE` on a distributed table
filtered by a non-distribution column; it just fans out to every shard
instead of routing to one. Left as explicit future optimization work, not
blocking this phase.

What actually blocked this phase, found only by running real code against
a real Citus cluster — three separate things reading the FK graph alone
would never have surfaced:

**Foreign keys.** Citus doesn't allow a plain local table to hold an FK
into a distributed table (either direction), and a self-referencing FK not
expressed on the shared distribution column (`process_instance_id`) can't
be verified as colocated even when both sides are distributed. 21 FK
constraints across the schema needed dropping — see
`database/archive/migrations-before-baseline/013_citus_phase2_drop_fks.sql` for the full list and
reasoning per constraint. Application code (`internal/repository`,
`internal/runtime`, `internal/service`) already maintains these
relationships correctly; nothing here was ever the *only* thing enforcing
integrity except one real gap (see "Real bugs found" below).

**Composite primary keys.** Not visible from reading FK constraints at
all — only surfaced when `create_distributed_table('executions', ...)`
actually failed with `cannot create constraint on "executions" ... 
Distributed relations cannot have UNIQUE, EXCLUDE, or PRIMARY KEY
constraints that do not include the partition column`. Every one of the 6
distributed non-root tables had a plain `id UUID PRIMARY KEY`, which
Citus rejects once the table is sharded by a *different* column
(`process_instance_id`) — uniqueness can only be enforced within a shard.
Fixed by widening each to a composite `PRIMARY KEY (process_instance_id, id)`
— safe because, by that point, nothing referenced any of these tables'
bare `id` column via FK anymore (the FK drops above already removed every
such reference).

**`FOR UPDATE` can't cross a distributed-table join.** The single biggest
surprise: distributing `executions` broke `FetchAndLockJobsTx` — the
fetch-and-lock query nearly every test (and every real external-task
worker) depends on — even though `jobs` itself stays local exactly as
planned. The query joined `jobs` and `executions` under `FOR UPDATE SKIP
LOCKED`, and Citus rejects any `FOR UPDATE`/`FOR SHARE` query touching a
distributed table unless it's routable to a single shard. Distributing
`executions` broke this regardless of `jobs`' own distribution status —
the two tables aren't as independent as the phase's scoping assumed. Fixed
by splitting the query: lock job rows via a `jobs`-only `SELECT ... FOR
UPDATE SKIP LOCKED` (fully local, unaffected by anything Citus does), then
a separate, unlocked follow-up `SELECT` against `executions` for
`current_element_id`. First regression attempt: 24 of 32 suites failed
until this was found and fixed.

### Real bugs found (not Citus-specific, but only surfaced by it)

Two genuine, pre-existing races were exposed by Citus's added
coordinator↔worker network latency widening race windows that were always
theoretically present but too narrow to ever trigger against a
single-process local Postgres in practice:

1. **`TerminateProcessInstance` had no status guard.** If an instance
   completes normally at almost the same moment an explicit terminate call
   arrives for it, the terminate path would still find the row (not yet
   archived), overwrite its correct `'completed'` status with
   `'terminated'`, and write termination audit-log rows — which, after
   this phase's FK drop on `audit_logs.process_instance_id`, would
   silently succeed as **permanently orphaned rows** instead of loudly
   failing an FK-violation check the way the old schema would have. Found
   via targeted testing (deliberately racing a terminate call against
   natural completion), not by the automated suite. Fixed by adding a
   status guard to `internal/service/process_lifecycle_service.go`'s
   `TerminateProcessInstance` — mirrors `ArchiveAndDeleteProcessInstance`'s
   own existing guard, refusing to terminate an instance already
   `'completed'`/`'terminated'`.
2. **Parallel/inclusive gateway join creation had a check-then-insert
   race.** `ExecuteJoin` in both `parallel_gateway_executor.go` and
   `executor_inclusive_gateway.go` checks whether a `gateway_state` row
   already exists for `(process_instance_id, gateway_id)`; if not, it
   creates one. Two branches arriving concurrently could both see "doesn't
   exist yet" and both attempt to create it, one hitting the unique
   constraint. Always theoretically possible; Citus's added latency made
   it reproduce reliably in this suite for the first time. Fixed by
   changing `CreateGatewayStateTx` to `INSERT ... ON CONFLICT
   (process_instance_id, gateway_id) DO NOTHING`, returning whether the
   insert actually happened; a caller that loses the race now re-fetches
   the row the winner created and falls through to the normal join-update
   path instead of erroring.

Also removed while touching this code: two confirmed-dead functions
(`ProcessInstanceRepository.Delete`, `DeleteAllHistoric` — zero callers
anywhere) whose comments assumed cascade behavior this phase partially
removes, and one more confirmed-dead function (`UpdateGatewayJoinTx`,
superseded by the two executors above) that locked `gateway_state` by `id`
alone — would have hit the same `FOR UPDATE` routing problem as
`FetchAndLockJobsTx` if it were ever actually called.

### Tables distributed (colocated on `process_instance_id`, `process_instances` itself on `id`)

`process_instances`, `executions`, `tasks`, `variables`, `gateway_state`,
`multi_instance_executions`, `incidents` — 7 tables, all sharing one
Citus colocation group.

### Tables deliberately still local

`jobs`, `timer_jobs` (the global `FOR UPDATE SKIP LOCKED` claim queries
need their own redesign — see §5), `audit_logs` (nullable
`process_instance_id` — some rows are pure user actions with no instance,
which Citus's non-null distribution-column requirement can't represent),
`event_subscriptions`, `boundary_events`, `event_subprocesses`,
`multi_instance_children`, `execution_variables` (no direct
`process_instance_id` column — only reachable via a join through
`executions`). Every one of these tables' FKs into the now-distributed
group were dropped; `internal/repository/historic_archive_repository.go`'s
`CleanupOrphanExecutionStateTx` (already existed, previously covering
`gateway_state`/`event_subprocesses`/`event_subscriptions`/
`boundary_events` — the only tables that never had `ON DELETE CASCADE` in
the first place) was extended to also explicitly delete `jobs`/
`timer_jobs`/`audit_logs` rows (whose cascade FK was just dropped) plus a
new execution-id-based cleanup step for `multi_instance_children`/
`execution_variables`, all called before `DeleteLiveInstanceTx` in the
same archive transaction.

## 4. Verification (all done, `dev` branch, 2026-07-05)

**Phase 1**: `pg_dist_node` shows both workers registered; `citus_tables`
shows all 11 reference tables converted; idempotent across app restarts.

**Phase 2**, two-stage discipline (so a schema/cascade bug is never
confused with a Citus-specific one):

- **Stage A — schema + Go changes only, verified on the plain (non-Citus)
  stack first.** Full 32-suite regression green. Targeted check: an
  instance with a real multi-instance service task, completed normally,
  plus a second instance raced (explicit terminate call fired the instant
  its one task completed) — zero orphaned rows across all 8
  explicitly-cleaned tables, and the raced instance correctly stayed
  `'completed'` (the status-guard fix verified directly, not just
  inferred).
- **Stage B — the Citus-backed stack.** `citus_tables` confirms all 7
  tables `citus_table_type = 'distributed'` sharing one `colocation_id`.
  First full-regression attempt: 24/32 suites failed (the `FOR UPDATE`
  join blocker). After that fix: 2/32 failed (the gateway-join race).
  After that fix: **32/32 green**, on a fresh stack built from scratch.
  Same targeted multi-instance/race check re-run against the coordinator
  — zero orphans.
- Re-ran the full regression on a **fresh plain (non-Citus) stack** one
  more time after all fixes landed, plus suite 03 (parallel gateway,
  skipped by `--no-timers` in the main run) directly, to confirm none of
  the query/race fixes changed behavior for the default deployment path.
  All green.

## 5. Jobs, timers and the rest of the coordinator tables (done, 2026-10-20)

The plan written here in July (a reference "claim index" table next to a
distributed payload table) was not needed. What was built instead:

- **`jobs`, `timer_jobs` and `scope_variables` are distributed**, colocated
  with `process_instances` like the seven tables of phase 2. The bootstrap
  re-keys `jobs`/`timer_jobs` to `(process_instance_id, id)` on Citus only
  (Citus requires the distribution column in the primary key); the plain
  Postgres schema keeps `id`.
- **Claims without `FOR UPDATE`** (`repository.SetDistributedClaims`). Citus
  refuses `SELECT … FOR UPDATE` on a distributed table unless the query
  targets one instance — confirmed in July. So a claim reads candidates
  without a lock (a multi-shard `SELECT`, which Citus accepts), then claims
  each with an `UPDATE … WHERE process_instance_id = $1 AND id = $2 AND
  status = 'pending' RETURNING …`: routed to one shard, and only successful
  while the row is still free. Two claimers can't get the same job; the
  loser moves on to its next candidate. Same for due timers
  (`is_triggered = false`). Plain Postgres keeps `FOR UPDATE SKIP LOCKED`.
- **Locking a job while it is completed or failed**
  (`GetJobWithExecutionTx`) first reads the job's instance, then locks with
  `FOR UPDATE OF j` filtered on that instance — a single-shard query Citus
  accepts. The job writes name the instance too, so they hit one shard.
- **Citus local tables.** Every table that is neither reference nor
  distributed (subscriptions, boundary events, audit, history…) is added
  to the metadata with `citus_add_local_table_to_metadata` (after
  registering the coordinator with `citus_set_coordinator_host`). A join
  between a distributed table and a table Citus doesn't know is refused
  ("direct joins between distributed and local tables are not
  supported"); with local tables in the metadata Citus plans them.
- **No trigger on the variables tables.** Citus won't distribute a table
  with triggers, and a `NOTIFY` fired on a worker would never reach the
  coordinator's `LISTEN`. On Citus the bootstrap drops the two triggers of
  migration 004 and the engine sends the `goflow_variables` notification
  itself after every variables write (`repository.SetAppVariableNotify`).
- **No `NOW()` where Citus refuses it**: in an `INSERT … ON CONFLICT DO
  UPDATE SET` on a distributed table, or compared to a column in an
  `UPDATE`. The time is passed from Go instead.

The full integration regression runs on a coordinator + 2 workers
(`COMPOSE_FILE="docker-compose.yaml;docker-compose.citus.yml"`).

## 6. Performance follow-up — route single-shard instead of scatter-gather (done, 2026-07-05)

Phase 2 made queries against the six per-instance tables *correct* even
when filtered by row ID alone (not `process_instance_id`), but such
queries ran as a scatter-gather across every shard instead of routing to
one. An exhaustive inventory (not manual sampling) found 59 call sites
filtering by `id` alone across `EngineRepository`, `TaskRepository`,
`ExternalTaskRepository`, `IncidentRepository`,
`event_subscription_repository`, and `service.ResumeTimerExecution`:

- **18 were dead code** (zero callers anywhere) — deleted outright.
- **~15 needed zero new parameters** — the query was already inline
  inside a method holding a struct with `ProcessInstanceID` in scope;
  just extended the `WHERE` clause.
- **~15 needed a new `processInstanceID` parameter on a shared method,
  but every real caller already had the value** (mostly executors in
  `internal/runtime`, already holding `exec.ProcessInstanceID`) — added
  the parameter, threaded it through every caller, verified via
  `go build`'s exact "not enough arguments" errors rather than manual
  grep.
- **~8 are genuine "resolve from an opaque external ID" entry points**
  with no cheap fix (`FindTaskByID`/`GetTaskByID` called with just a
  URL-param task ID, the first `GetActiveExecution` call in a request
  chain, `IncidentRepository.GetByID`, `partition.LookupIncidentOwner`)
  — left as accepted scatter-gather; they're low-frequency "resolve an ID
  from outside" calls, not hot-path loops, and fixing them would need a
  secondary id→process_instance_id lookup index, out of scope here.

**A real regression found by the full regression suite, not by
inspection**: two of the shared `gateway_state` lookup queries in
`ParallelGatewayExecutor.ExecuteJoin` / `InclusiveGatewayExecutor.ExecuteJoin`
selected `id, expected_incoming, received_incoming, status, joined_flows`
but never `process_instance_id`. Whenever the *second* (non-first) branch
to reach a join fetched the row through that query, sqlx left
`gatewayState.ProcessInstanceID` at its Go zero-value (`00000000-...`)
since the column wasn't in the result set. The new compound
`WHERE process_instance_id = $3 AND id = $4` on the subsequent
`received_incoming`/`status='completed'` updates then matched zero rows —
Postgres doesn't error on a zero-row `UPDATE`, so this failed completely
silently. Net effect: any parallel/inclusive gateway join with 2+ branches
looked like it joined (children marked completed, parent's node advanced)
but the follow-up "resume and process the next node" step never fired,
because the code that decides whether to re-trigger it re-reads
`gateway_state` from the DB and saw the join as still incomplete. Instances
got permanently stuck immediately after the gateway, with no error logged
anywhere. Fixed by adding `process_instance_id` to both SELECTs in both
files. Caught by the plain-stack regression (3 suites failing on
gateway-fork scenarios with 10-16s timeouts), root-caused with a temporary
`RowsAffected()` debug print rather than by inspection alone, then
verified fixed with the debug logging removed.

**Verification**: full 32-suite regression green on both the plain and
Citus-backed stacks (fresh stacks, `--no-timers`). `EXPLAIN` on the Citus
coordinator confirmed the actual point of the work: the `executions`,
`tasks`, and `gateway_state` compound-WHERE updates all produce
`Custom Scan (Citus Adaptive)` plans with **`Task Count: 1`** (a single
worker, `Index Scan` on the compound index), versus **`Task Count: 32`**
(one task shown of 32, `Seq Scan`/`Bitmap Heap Scan` filtering by `id`
alone) for the pre-fix id-only queries.
