#!/usr/bin/env sh
# Sauvegarde de la base GoFlow (Postgres dockerisé) avec rotation.
#
#   ./scripts/backup-postgres.sh                 # sauvegarde dans ./backups
#   BACKUP_DIR=/srv/backups BACKUP_KEEP_DAYS=30 ./scripts/backup-postgres.sh
#
# Cluster haute disponibilité (deploy/ha) : passer par HAProxy, qui mène au
# primaire du moment, quel qu'il soit :
#
#   PGHOST=haproxy PGPORT=5000 PGPASSWORD=… ./scripts/backup-postgres.sh
#
# pg_dump tourne alors dans un conteneur postgres:16 jetable, sur le réseau
# Docker de GoFlow (DOCKER_NETWORK, camunda-like_goflow-network par défaut).
#
# Cron quotidien (3h du matin) :
#   0 3 * * * cd /root/apps/goflow && ./scripts/backup-postgres.sh >> backups/backup.log 2>&1
#
# Copier ensuite les fichiers hors du serveur (rsync, rclone, stockage
# objet…) : une sauvegarde sur le même disque ne protège pas d'une panne de
# disque. Vérifier régulièrement qu'elle se restaure : scripts/test-restore.sh.
set -eu

CONTAINER="${POSTGRES_CONTAINER:-goflow-postgres}"
DB_USER="${POSTGRES_USER:-goflow}"
DB_NAME="${POSTGRES_DB:-goflow}"
BACKUP_DIR="${BACKUP_DIR:-./backups}"
KEEP_DAYS="${BACKUP_KEEP_DAYS:-14}"
NETWORK="${DOCKER_NETWORK:-camunda-like_goflow-network}"

# pg_dump / pg_restore : dans le conteneur Postgres, ou, avec PGHOST, dans un
# conteneur jetable qui joint PGHOST:PGPORT par le réseau.
pg() {
  if [ -n "${PGHOST:-}" ]; then
    docker run --rm -i --network "$NETWORK" -e PGPASSWORD="${PGPASSWORD:-}" \
      postgres:16 "$@" -h "$PGHOST" -p "${PGPORT:-5432}" -U "$DB_USER"
  else
    docker exec -i "$CONTAINER" "$@" -U "$DB_USER"
  fi
}

mkdir -p "$BACKUP_DIR"
stamp=$(date +%Y%m%d_%H%M%S)
file="$BACKUP_DIR/goflow_${stamp}.dump"
tmp="$file.partial"

source="$CONTAINER"
[ -n "${PGHOST:-}" ] && source="$PGHOST:${PGPORT:-5432}"
echo "$(date '+%F %T') sauvegarde de $DB_NAME ($source) vers $file"

# Format custom (-Fc) : compressé, restaurable table par table avec pg_restore.
pg pg_dump -d "$DB_NAME" -Fc > "$tmp"

# Un dump illisible ne doit jamais remplacer les bons : on vérifie avant de
# le garder (pg_restore --list ne se connecte à aucune base).
if ! docker run --rm -i postgres:16 pg_restore --list < "$tmp" > /dev/null; then
  echo "ERREUR : le dump $tmp est illisible, conservé pour analyse" >&2
  exit 1
fi
mv "$tmp" "$file"
echo "$(date '+%F %T') OK : $(du -h "$file" | cut -f1)"

# Rotation : supprime les sauvegardes de plus de KEEP_DAYS jours.
find "$BACKUP_DIR" -name 'goflow_*.dump' -type f -mtime +"$KEEP_DAYS" -print -delete
