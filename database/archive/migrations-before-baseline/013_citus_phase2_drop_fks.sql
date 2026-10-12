-- 013_citus_phase2_drop_fks.sql
--
-- Citus phase 2 (see architecture/citus-distributed-storage-design.md):
-- drops every foreign key, and widens every primary key, that would block
-- converting process_instances, executions, tasks, variables,
-- gateway_state, multi_instance_executions, and incidents into a colocated
-- Citus distributed-table group. Citus does not allow a plain local table
-- to hold an FK into a distributed table (either direction), a
-- self-referencing FK not expressed on the shared distribution column
-- (process_instance_id) can't be verified as colocated even when both
-- sides are distributed, and — found only by actually running
-- create_distributed_table against a real Citus node, not from reading
-- FK constraints alone — Citus also rejects any UNIQUE/PRIMARY KEY
-- constraint that doesn't include the distribution column, since
-- uniqueness can only be enforced within a single shard. Every one of
-- these 6 tables had a plain `id UUID PRIMARY KEY`, which needs widening
-- to a composite (process_instance_id, id) key.
--
-- Zero behavior change on a plain (non-Citus) Postgres database — these
-- constraints are dropped whether or not Citus is ever actually enabled;
-- referential integrity for the relationships below is already maintained
-- by application code (internal/repository, internal/runtime,
-- internal/service), and CleanupOrphanExecutionStateTx (extended in this
-- same change) explicitly deletes rows that used to rely on
-- ON DELETE CASCADE from process_instances.
--
-- Kept, unchanged: the process_instance_id FK on executions, tasks,
-- variables, gateway_state, incidents, and multi_instance_executions —
-- these remain valid, colocation-verifiable FKs once both sides are
-- distributed on process_instance_id, and continue to cascade correctly.

ALTER TABLE process_instances
    DROP CONSTRAINT IF EXISTS process_instances_parent_instance_id_fkey;

ALTER TABLE executions
    DROP CONSTRAINT IF EXISTS executions_parent_execution_id_fkey,
    DROP CONSTRAINT IF EXISTS executions_gateway_join_id_fkey;

ALTER TABLE event_subprocesses
    DROP CONSTRAINT IF EXISTS event_subprocesses_parent_execution_id_fkey,
    DROP CONSTRAINT IF EXISTS event_subprocesses_process_instance_id_fkey;

ALTER TABLE event_subscriptions
    DROP CONSTRAINT IF EXISTS event_subscriptions_execution_id_fkey,
    DROP CONSTRAINT IF EXISTS event_subscriptions_process_instance_id_fkey;

ALTER TABLE boundary_events
    DROP CONSTRAINT IF EXISTS boundary_events_execution_id_fkey,
    DROP CONSTRAINT IF EXISTS boundary_events_process_instance_id_fkey;

ALTER TABLE tasks
    DROP CONSTRAINT IF EXISTS tasks_execution_id_fkey;

ALTER TABLE jobs
    DROP CONSTRAINT IF EXISTS jobs_execution_id_fkey,
    DROP CONSTRAINT IF EXISTS jobs_process_instance_id_fkey;

ALTER TABLE timer_jobs
    DROP CONSTRAINT IF EXISTS timer_jobs_execution_id_fkey,
    DROP CONSTRAINT IF EXISTS timer_jobs_process_instance_id_fkey;

ALTER TABLE audit_logs
    DROP CONSTRAINT IF EXISTS audit_logs_process_instance_id_fkey,
    DROP CONSTRAINT IF EXISTS audit_logs_task_id_fkey;

ALTER TABLE incidents
    DROP CONSTRAINT IF EXISTS incidents_job_id_fkey;

ALTER TABLE multi_instance_executions
    DROP CONSTRAINT IF EXISTS multi_instance_executions_execution_id_fkey;

ALTER TABLE multi_instance_children
    DROP CONSTRAINT IF EXISTS multi_instance_children_parent_execution_id_fkey,
    DROP CONSTRAINT IF EXISTS multi_instance_children_child_execution_id_fkey;

ALTER TABLE execution_variables
    DROP CONSTRAINT IF EXISTS execution_variables_execution_id_fkey;

-- Widen each affected table's primary key to include the distribution
-- column. Safe to run on a plain Postgres database too (no behavior
-- change — a composite key is still unique and NOT NULL exactly as
-- before, just now also keyed on process_instance_id).
ALTER TABLE gateway_state
    DROP CONSTRAINT IF EXISTS gateway_state_pkey,
    ADD PRIMARY KEY (process_instance_id, id);

ALTER TABLE executions
    DROP CONSTRAINT IF EXISTS executions_pkey,
    ADD PRIMARY KEY (process_instance_id, id);

ALTER TABLE tasks
    DROP CONSTRAINT IF EXISTS tasks_pkey,
    ADD PRIMARY KEY (process_instance_id, id);

ALTER TABLE variables
    DROP CONSTRAINT IF EXISTS variables_pkey,
    ADD PRIMARY KEY (process_instance_id, id);

ALTER TABLE incidents
    DROP CONSTRAINT IF EXISTS incidents_pkey,
    ADD PRIMARY KEY (process_instance_id, id);

ALTER TABLE multi_instance_executions
    DROP CONSTRAINT IF EXISTS multi_instance_executions_pkey,
    ADD PRIMARY KEY (process_instance_id, id);
