#!/usr/bin/env sh
# Vérifie qu'une sauvegarde se restaure, sans toucher à la base de prod :
# restaure dans une base temporaire (goflow_restore_check) du même serveur
# Postgres, compte les lignes de quelques tables, puis la supprime.
#
#   ./scripts/test-restore.sh                       # dernière sauvegarde de ./backups
#   ./scripts/test-restore.sh backups/goflow_20261017_030000.dump
set -eu

CONTAINER="${POSTGRES_CONTAINER:-goflow-postgres}"
DB_USER="${POSTGRES_USER:-goflow}"
BACKUP_DIR="${BACKUP_DIR:-./backups}"
CHECK_DB="goflow_restore_check"

file="${1:-$(ls -t "$BACKUP_DIR"/goflow_*.dump 2>/dev/null | head -1)}"
if [ -z "$file" ] || [ ! -f "$file" ]; then
  echo "aucune sauvegarde trouvée (dossier $BACKUP_DIR)" >&2
  exit 1
fi

psql() { docker exec -i "$CONTAINER" psql -U "$DB_USER" -v ON_ERROR_STOP=1 "$@"; }

echo "restauration de $file dans $CHECK_DB…"
psql -d postgres -c "DROP DATABASE IF EXISTS $CHECK_DB" > /dev/null
psql -d postgres -c "CREATE DATABASE $CHECK_DB" > /dev/null
trap 'psql -d postgres -c "DROP DATABASE IF EXISTS '"$CHECK_DB"'" > /dev/null' EXIT

docker exec -i "$CONTAINER" pg_restore -U "$DB_USER" -d "$CHECK_DB" --no-owner < "$file"

psql -d "$CHECK_DB" -tA -c "
  SELECT 'process_definitions', count(*) FROM process_definitions
  UNION ALL SELECT 'process_instances', count(*) FROM process_instances
  UNION ALL SELECT 'historic_process_instances', count(*) FROM historic_process_instances
  UNION ALL SELECT 'users', count(*) FROM users
  UNION ALL SELECT 'schema_migrations', count(*) FROM schema_migrations"
echo "OK : la sauvegarde se restaure."
