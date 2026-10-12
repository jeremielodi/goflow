# Migrations

Les migrations sont des fichiers SQL **exécutés à la main** sur le serveur.
L'application ne modifie jamais le schéma toute seule.

## Point de départ

- `database/01_schema.sql` est le schéma complet. Une base neuve créée avec
  ce fichier n'a besoin d'aucune migration.
- `000_baseline.sql` marque une base existante (la prod) comme étant à jour
  avec ce schéma. Les anciennes migrations 002 à 014, déjà incluses dans
  `01_schema.sql`, sont archivées dans
  `database/archive/migrations-before-baseline/`.
- La table `schema_migrations` garde la liste des migrations appliquées :

  ```sql
  SELECT * FROM schema_migrations ORDER BY version;
  ```

### Mise en place sur la prod (une seule fois)

```bash
# 1. Vérifier que la base contient tout (lecture seule) : toutes les lignes doivent être OK
psql -h <hôte> -U <user> -d <base> -f check_baseline.sql

# 2. Enregistrer la baseline
psql -h <hôte> -U <user> -d <base> -v ON_ERROR_STOP=1 -1 -f 000_baseline.sql
```

Si une ligne affiche `MANQUANT`, rejouer d'abord la migration archivée
correspondante (elles sont toutes idempotentes), puis relancer la
vérification.

## Écrire une nouvelle migration

1. Créer `NNN_description_courte.sql` avec le numéro suivant (`001_...`,
   `002_...`).
2. Rendre le fichier idempotent autant que possible (`IF NOT EXISTS`,
   `IF EXISTS`), pour qu'une exécution en double ne casse rien.
3. Terminer le fichier par son enregistrement :

   ```sql
   INSERT INTO public.schema_migrations (version, description)
   VALUES ('001', 'description courte')
   ON CONFLICT (version) DO NOTHING;
   ```

4. Reporter le même changement dans `database/01_schema.sql` (et l'INSERT
   dans `schema_migrations` à la fin du fichier), pour qu'une base neuve
   soit identique à une base migrée.

## Appliquer une migration

```bash
# Toujours faire une sauvegarde avant
pg_dump -h <hôte> -U <user> -Fc <base> > backup_$(date +%Y%m%d_%H%M).dump

# -1 : tout le fichier dans une seule transaction (tout ou rien)
# ON_ERROR_STOP : s'arrête à la première erreur
psql -h <hôte> -U <user> -d <base> -v ON_ERROR_STOP=1 -1 -f 001_description_courte.sql
```

Exception : `CREATE INDEX CONCURRENTLY` ne peut pas tourner dans une
transaction. Une migration qui l'utilise s'exécute sans `-1`, et doit être
seule dans son fichier.

## Réglages serveur

`database/tuning/postgresql-tuning.sql` n'est pas une migration : il règle
le serveur Postgres (`ALTER SYSTEM`, mémoire, timeouts) et doit être adapté
à la machine avant d'être appliqué. Il ne s'exécute pas dans une
transaction (pas de `-1`).
