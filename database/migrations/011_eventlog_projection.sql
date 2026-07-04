-- migrations/011_eventlog_projection.sql
--
-- Phase 0 "shadow mode" of the event-sourced engine scoping (see
-- architecture/event-sourced-kafka-design.md) publishes domain events to a
-- Redpanda topic alongside the existing Postgres-transactional write path,
-- which remains completely unchanged and is still the only source of truth.
-- These two tables exist purely to prove that claim in practice: a
-- background consumer (internal/eventlog.Consumer) folds the consumed event
-- stream into them, independently of the real engine tables, so a human or
-- a test can diff a projected row against the real REST API's view of the
-- same instance and confirm they agree.
--
-- Nothing else in the app reads or writes these tables — they are not a
-- read-model replacement, not consulted by any real request path, and not
-- guaranteed to be complete (only the transitions Phase 0 publishes events
-- for are folded in; see the event coverage table in the design doc).

CREATE TABLE IF NOT EXISTS eventlog_projected_instances (
    id UUID PRIMARY KEY,                    -- same id as the real process_instances row
    process_definition_id UUID NULL,
    process_key TEXT NULL,
    version INT NULL,
    status TEXT NOT NULL,                   -- 'running' | 'completed' | 'terminated'
    started_at TIMESTAMPTZ NULL,
    ended_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS eventlog_projected_jobs (
    id UUID PRIMARY KEY,                    -- same id as the real jobs row
    process_instance_id UUID NOT NULL,
    execution_id UUID NULL,
    job_type TEXT NULL,
    status TEXT NOT NULL,                   -- 'created' | 'completed' | 'failed'
    error_message TEXT NULL,
    retries_remaining INT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_eventlog_projected_jobs_process_instance_id
ON eventlog_projected_jobs(process_instance_id);
