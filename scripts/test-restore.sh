#!/usr/bin/env sh
# Vérifie qu'une sauvegarde se restaure, sans toucher à la base de prod :
# restaure dans un Postgres jetable (conteneur postgres:16 supprimé à la
# fin), compte les lignes de quelques tables. Marche pareil pour une base
# autonome et pour le cluster haute disponibilité.
#
#   ./scripts/test-restore.sh                       # dernière sauvegarde de ./backups
#   ./scripts/test-restore.sh backups/goflow_20261017_030000.dump
set -eu

BACKUP_DIR="${BACKUP_DIR:-./backups}"
CHECK="goflow-restore-check-$$"

file="${1:-$(ls -t "$BACKUP_DIR"/goflow_*.dump 2>/dev/null | head -1)}"
if [ -z "$file" ] || [ ! -f "$file" ]; then
  echo "aucune sauvegarde trouvée (dossier $BACKUP_DIR)" >&2
  exit 1
fi

echo "restauration de $file dans un Postgres jetable…"
docker run -d --rm --name "$CHECK" -e POSTGRES_USER=goflow -e POSTGRES_PASSWORD=check \
  -e POSTGRES_DB=goflow postgres:16 > /dev/null
trap 'docker rm -f "$CHECK" > /dev/null 2>&1' EXIT

# Ready once the image's init (which restarts Postgres) is over.
tries=0
until docker exec "$CHECK" pg_isready -U goflow -d goflow -h 127.0.0.1 -q 2>/dev/null &&
      docker exec "$CHECK" psql -U goflow -d goflow -h 127.0.0.1 -tAc "SELECT 1" > /dev/null 2>&1; do
  tries=$((tries + 1))
  [ "$tries" -gt 60 ] && { echo "le Postgres jetable ne démarre pas" >&2; exit 1; }
  sleep 1
done

docker exec -i "$CHECK" pg_restore -U goflow -d goflow --no-owner --exit-on-error < "$file"

docker exec "$CHECK" psql -U goflow -d goflow -v ON_ERROR_STOP=1 -tA -c "
  SELECT 'process_definitions', count(*) FROM process_definitions
  UNION ALL SELECT 'process_instances', count(*) FROM process_instances
  UNION ALL SELECT 'historic_process_instances', count(*) FROM historic_process_instances
  UNION ALL SELECT 'users', count(*) FROM users
  UNION ALL SELECT 'schema_migrations', count(*) FROM schema_migrations"
echo "OK : la sauvegarde se restaure."
