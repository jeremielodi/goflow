-- 005_hot_path_indexes.sql — index manquants sur les tables du moteur.
--
-- Ces tables sont lues et nettoyées par instance ou par exécution à chaque
-- étape (attente d'un message, d'un timer, fin de job, archivage). Sans ces
-- index, chaque requête parcourt la table entière : invisible tant qu'il y
-- a peu d'instances vivantes, coûteux dès qu'il y en a des milliers.
--
-- Créer un index bloque les écritures sur sa table le temps de le
-- construire. Ces tables ne contiennent que l'état vivant (les instances
-- terminées sont archivées), c'est donc court. Sur une très grosse table,
-- on peut lancer chaque CREATE INDEX à la main avec CONCURRENTLY (sans -1).
--
--   psql -h <hôte> -U <user> -d <base> -v ON_ERROR_STOP=1 -1 -f 005_hot_path_indexes.sql

CREATE INDEX IF NOT EXISTS idx_jobs_process_instance_id ON public.jobs (process_instance_id);
CREATE INDEX IF NOT EXISTS idx_jobs_execution_id ON public.jobs (execution_id);

CREATE INDEX IF NOT EXISTS idx_timer_jobs_process_instance_id ON public.timer_jobs (process_instance_id);
CREATE INDEX IF NOT EXISTS idx_timer_jobs_execution_id ON public.timer_jobs (execution_id);

CREATE INDEX IF NOT EXISTS idx_event_subscriptions_process_instance_id ON public.event_subscriptions (process_instance_id);
CREATE INDEX IF NOT EXISTS idx_event_subscriptions_execution_id ON public.event_subscriptions (execution_id) WHERE consumed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_event_subprocesses_process_instance_id ON public.event_subprocesses (process_instance_id);

CREATE INDEX IF NOT EXISTS idx_multi_instance_children_child ON public.multi_instance_children (child_execution_id);

CREATE INDEX IF NOT EXISTS idx_published_messages_expires_at ON public.published_messages (expires_at);

INSERT INTO public.schema_migrations (version, description)
VALUES ('005', 'index manquants sur les tables du moteur (jobs, timers, abonnements…)')
ON CONFLICT (version) DO NOTHING;
