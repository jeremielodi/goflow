-- 004_variables_notify.sql — signale chaque écriture de variables.
--
-- Les événements conditionnels sont réévalués dès qu'une variable de leur
-- instance change (comme Camunda 7), au lieu d'attendre le balayage
-- périodique : chaque écriture dans variables ou scope_variables envoie un
-- NOTIFY goflow_variables avec l'id de l'instance, que le moteur écoute.
--
--   psql -h <hôte> -U <user> -d <base> -v ON_ERROR_STOP=1 -1 -f 004_variables_notify.sql

CREATE OR REPLACE FUNCTION public.goflow_notify_variables() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('goflow_variables', NEW.process_instance_id::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS variables_notify ON public.variables;
CREATE TRIGGER variables_notify
AFTER INSERT OR UPDATE ON public.variables
FOR EACH ROW EXECUTE FUNCTION public.goflow_notify_variables();

DROP TRIGGER IF EXISTS scope_variables_notify ON public.scope_variables;
CREATE TRIGGER scope_variables_notify
AFTER INSERT OR UPDATE ON public.scope_variables
FOR EACH ROW EXECUTE FUNCTION public.goflow_notify_variables();

INSERT INTO public.schema_migrations (version, description)
VALUES ('004', 'goflow_variables : NOTIFY à chaque écriture de variables')
ON CONFLICT (version) DO NOTHING;
