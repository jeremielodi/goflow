#!/bin/bash
# One-shot setup of a new Patroni cluster for GoFlow, through HAProxy (so
# on the current primary): creates the goflow role and database if they
# don't exist, and loads database/01_schema.sql and 02_config.sql (the
# base permissions) into an empty database.
# Idempotent: safe at every start.
set -e
export PGPASSWORD="$PG_SUPERUSER_PASSWORD"
until pg_isready -h "${PG_HOST:-haproxy}" -p "${PG_PORT:-5000}" -q; do sleep 2; done
psql_su() { psql -h "${PG_HOST:-haproxy}" -p "${PG_PORT:-5000}" -U postgres -v ON_ERROR_STOP=1 "$@"; }

psql_su -d postgres -v pwd="$GOFLOW_PASSWORD" <<'EOSQL'
SELECT format('CREATE ROLE goflow WITH LOGIN PASSWORD %L', :'pwd')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'goflow') \gexec
SELECT 'CREATE DATABASE goflow OWNER goflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'goflow') \gexec
EOSQL

# GOFLOW_SKIP_SCHEMA=true: role and database only, to restore an existing
# database into (see the README, Postgres high availability).
if [ "${GOFLOW_SKIP_SCHEMA:-false}" != "true" ] &&
	[ "$(psql_su -d goflow -Atc "SELECT to_regclass('public.process_definitions') IS NULL")" = "t" ]; then
	echo "Loading the GoFlow schema"
	PGPASSWORD="$GOFLOW_PASSWORD" psql -h "${PG_HOST:-haproxy}" -p "${PG_PORT:-5000}" -U goflow -d goflow \
		-v ON_ERROR_STOP=1 -q -f /schema/01_schema.sql -f /schema/02_config.sql
fi
echo "GoFlow database ready"
