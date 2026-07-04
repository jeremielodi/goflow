-- 012_process_write_path.sql
--
-- Phase 1 of the event-sourced engine (see
-- architecture/event-sourced-kafka-design.md): a per-process-definition
-- flag gating which write path a process definition's instances use.
-- Defaults to 'legacy' for every existing and future deployment, so this
-- is a zero-behavior-change migration on its own — a definition only moves
-- to 'partitioned' via an explicit admin call
-- (PATCH /engine-rest/process-definition/:id/write-path), which itself
-- rejects the flip unless the definition's graph is exactly
-- start -> one external service task -> end (this first vertical slice's
-- supported shape).
ALTER TABLE process_definitions
    ADD COLUMN IF NOT EXISTS write_path TEXT NOT NULL DEFAULT 'legacy'
        CHECK (write_path IN ('legacy', 'partitioned'));
