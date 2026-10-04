-- Citus worker nodes only need the extension activated — they don't need
-- the app schema (01_schema.sql/02_config.sql): Citus pushes distributed
-- and reference table DDL to workers itself when create_reference_table()/
-- create_distributed_table() run against the coordinator. See
-- docker-compose.citus.yml and architecture/citus-distributed-storage-design.md.
CREATE EXTENSION IF NOT EXISTS citus;
