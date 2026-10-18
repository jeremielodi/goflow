-- 003_incident_execution.sql — exécution concernée par un incident.
--
-- Comme Camunda 8 : une erreur pendant l'exécution d'un élément (condition,
-- script, collection multi-instance, processus appelé introuvable, timer
-- invalide…) crée un incident sur cet élément au lieu de bloquer le jeton en
-- silence ; résoudre l'incident ré-exécute l'élément. execution_id dit quelle
-- exécution reprendre.
--
--   psql -h <hôte> -U <user> -d <base> -v ON_ERROR_STOP=1 -1 -f 003_incident_execution.sql

ALTER TABLE public.incidents ADD COLUMN IF NOT EXISTS execution_id UUID NULL;

INSERT INTO public.schema_migrations (version, description)
VALUES ('003', 'incidents.execution_id : exécution à reprendre à la résolution')
ON CONFLICT (version) DO NOTHING;
