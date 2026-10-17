-- 002_scope_variables.sql — variables locales aux portées (comme Camunda 8).
--
-- Les variables d'une instance vivent dans des portées imbriquées : l'instance
-- elle-même (table variables, inchangée), un sous-processus, une itération ou
-- le corps d'une multi-instance, une activité avec des mappings d'entrée.
-- Chaque portée locale est portée par une exécution (scope_id). Une lecture
-- part de l'exécution et remonte jusqu'à l'instance ; une écriture met à jour
-- la portée la plus proche qui a déjà la variable, sinon l'instance.
--
--   psql -h <hôte> -U <user> -d <base> -v ON_ERROR_STOP=1 -1 -f 002_scope_variables.sql

CREATE TABLE IF NOT EXISTS public.scope_variables (
    process_instance_id UUID NOT NULL,
    scope_id            UUID NOT NULL,      -- exécution qui porte la portée
    data                JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (process_instance_id, scope_id)
);

INSERT INTO public.schema_migrations (version, description)
VALUES ('002', 'scope_variables : variables locales aux portées')
ON CONFLICT (version) DO NOTHING;
