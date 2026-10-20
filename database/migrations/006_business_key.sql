-- 006_business_key.sql — business key des instances (Camunda 7).
--
-- Une application Camunda 7 démarre souvent une instance avec un
-- businessKey (numéro de commande, de dossier…) puis la retrouve par
-- ce businessKey. Colonne facultative : les instances existantes restent
-- sans business key, rien n'est modifié.
--
--   psql -h <hôte> -U <user> -d <base> -v ON_ERROR_STOP=1 -1 -f 006_business_key.sql

ALTER TABLE public.process_instances ADD COLUMN IF NOT EXISTS business_key TEXT NULL;
CREATE INDEX IF NOT EXISTS idx_process_instances_business_key ON public.process_instances (business_key);

ALTER TABLE public.historic_process_instances ADD COLUMN IF NOT EXISTS business_key TEXT NULL;
CREATE INDEX IF NOT EXISTS idx_historic_process_instances_business_key ON public.historic_process_instances (business_key);

INSERT INTO public.schema_migrations (version, description)
VALUES ('006', 'business key des instances (Camunda 7)')
ON CONFLICT (version) DO NOTHING;
