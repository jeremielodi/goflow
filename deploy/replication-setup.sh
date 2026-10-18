#!/bin/bash
# Creates the replication role on the primary if it doesn't exist yet
# (idempotent: runs at every start of docker-compose.scale.yaml).
set -e
export PGPASSWORD="$POSTGRES_PASSWORD"
until pg_isready -h postgres -p 5432 -q; do sleep 1; done
psql -h postgres -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 \
	-v pwd="${REPLICATION_PASSWORD:-replicator}" <<'EOSQL'
SELECT format('CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD %L', :'pwd')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') \gexec
EOSQL
