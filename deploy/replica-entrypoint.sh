#!/bin/bash
# Read replica: on first start, copies the primary (pg_basebackup, which
# also writes the standby configuration), then runs as a hot standby that
# streams the primary's changes and answers read-only queries.
set -e

if [ ! -s "$PGDATA/PG_VERSION" ]; then
	mkdir -p "$PGDATA"
	chown postgres:postgres "$PGDATA"
	chmod 700 "$PGDATA"
	until gosu postgres pg_isready -h "${PRIMARY_HOST:-postgres}" -p 5432 -q; do
		sleep 1
	done
	export PGPASSWORD="${REPLICATION_PASSWORD:-replicator}"
	gosu postgres pg_basebackup -h "${PRIMARY_HOST:-postgres}" -p 5432 -U replicator \
		-D "$PGDATA" -R -X stream
fi

# max_connections must not be lower than the primary's.
exec gosu postgres postgres -c hot_standby=on -c max_connections="${POSTGRES_MAX_CONNECTIONS:-200}"
