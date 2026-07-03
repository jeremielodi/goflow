-- migrations/008_jobs_composite_index.sql
--
-- FetchAndLockJobsTx (internal/repository/external_task_repository.go) is
-- the hottest query in the system — called by every REST fetch-and-lock
-- call, the v2 job controller, and gRPC ActivateJobs's poll loop — and
-- filters WHERE job_type = $1 AND status = 'pending' AND (locked_by IS
-- NULL OR locked_until < NOW()). Only independent single-column indexes on
-- job_type and status existed before this, forcing Postgres into a bitmap
-- AND instead of a single index scan built for this exact predicate.
--
-- Purely additive: no existing index dropped, and this is a partial index
-- (WHERE status = 'pending') since that's the only status this query ever
-- filters on, keeping it small regardless of how many completed/failed
-- jobs accumulate.

CREATE INDEX IF NOT EXISTS idx_jobs_type_status_pending
ON jobs(job_type, status)
WHERE status = 'pending';
