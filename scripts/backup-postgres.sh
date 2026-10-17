#!/usr/bin/env sh
# Sauvegarde de la base GoFlow (Postgres dockerisé) avec rotation.
#
#   ./scripts/backup-postgres.sh                 # sauvegarde dans ./backups
#   BACKUP_DIR=/srv/backups BACKUP_KEEP_DAYS=30 ./scripts/backup-postgres.sh
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

mkdir -p "$BACKUP_DIR"
stamp=$(date +%Y%m%d_%H%M%S)
file="$BACKUP_DIR/goflow_${stamp}.dump"
tmp="$file.partial"

echo "$(date '+%F %T') sauvegarde de $DB_NAME ($CONTAINER) vers $file"

# Format custom (-Fc) : compressé, restaurable table par table avec pg_restore.
docker exec "$CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" -Fc > "$tmp"

# Un dump illisible ne doit jamais remplacer les bons : on vérifie avant de
# le garder.
if ! docker exec -i "$CONTAINER" pg_restore --list < "$tmp" > /dev/null; then
  echo "ERREUR : le dump $tmp est illisible, conservé pour analyse" >&2
  exit 1
fi
mv "$tmp" "$file"
echo "$(date '+%F %T') OK : $(du -h "$file" | cut -f1)"

# Rotation : supprime les sauvegardes de plus de KEEP_DAYS jours.
find "$BACKUP_DIR" -name 'goflow_*.dump' -type f -mtime +"$KEEP_DAYS" -print -delete
