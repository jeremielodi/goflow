-- migrations/010_historic_process_instance_time_indexes.sql
--
-- The new analytics endpoint (GET /engine-rest/analytics/process-stats)
-- computes duration percentiles filtered/grouped by started_at, and
-- throughput-over-time buckets by started_at/ended_at — historic_process_
-- instances only had indexes on process_definition_key/tenant_id before
-- this, so both queries would need a sequential scan as the table grows.
-- Purely additive.

CREATE INDEX IF NOT EXISTS idx_historic_process_instances_started_at
ON historic_process_instances(started_at);

CREATE INDEX IF NOT EXISTS idx_historic_process_instances_ended_at
ON historic_process_instances(ended_at);

-- Composite for the common "stats for one process key over a date range"
-- filter; also serves as a prefix index for plain process_definition_key
-- lookups (the existing single-column index on it is kept regardless,
-- since other call sites like FindAllProcessInstances rely on it alone).
CREATE INDEX IF NOT EXISTS idx_historic_process_instances_key_started_at
ON historic_process_instances(process_definition_key, started_at);
