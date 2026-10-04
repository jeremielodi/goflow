-- Activates the Citus extension when running against a Citus-capable
-- Postgres image (citusdata/citus). Safe no-op on plain postgres:16 (used
-- by default docker-compose.yaml) since the extension isn't listed in
-- pg_available_extensions there — this file is always mounted alongside
-- 01_schema.sql on both images, and only ever takes effect on Citus.
-- See docker-compose.citus.yml and architecture/citus-distributed-storage-design.md.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'citus') THEN
    CREATE EXTENSION IF NOT EXISTS citus;
  END IF;
END $$;
