-- 000_baseline.sql — point de départ du suivi des migrations.
--
-- À exécuter UNE FOIS sur une base existante (la prod), après avoir vérifié
-- avec check_baseline.sql que toutes les lignes sont OK. Une base neuve
-- créée depuis database/01_schema.sql contient déjà cette table et cette
-- ligne : inutile de lancer ce fichier dessus (il est sans effet de toute
-- façon).
--
--   psql -h <hôte> -U <user> -d <base> -v ON_ERROR_STOP=1 -1 -f 000_baseline.sql

CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version     TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO public.schema_migrations (version, description)
VALUES ('000', 'baseline — schéma de database/01_schema.sql (anciennes migrations 002-014 incluses)')
ON CONFLICT (version) DO NOTHING;
