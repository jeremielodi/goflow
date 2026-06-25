-- migrations/001_optimize_postgres.sql

-- Increase max connections (requires restart)
ALTER SYSTEM SET max_connections = 200;

-- Optimize shared buffers (25% of RAM for dedicated server, adjust accordingly)
ALTER SYSTEM SET shared_buffers = '4GB';

-- Optimize work memory
ALTER SYSTEM SET work_mem = '16MB';

-- Optimize maintenance work memory
ALTER SYSTEM SET maintenance_work_mem = '256MB';

-- Enable parallel query execution
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
ALTER SYSTEM SET max_parallel_workers = 8;
ALTER SYSTEM SET parallel_setup_cost = 1000;
ALTER SYSTEM SET parallel_tuple_cost = 0.1;

-- Optimize checkpoint settings
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET min_wal_size = '1GB';

-- Optimize query planner
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_cache_size = '8GB';

-- Enable connection pooling settings
ALTER SYSTEM SET idle_in_transaction_session_timeout = '60s';
ALTER SYSTEM SET lock_timeout = '30s';
ALTER SYSTEM SET statement_timeout = '30s';

-- Apply settings (requires restart)
-- SELECT pg_reload_conf();