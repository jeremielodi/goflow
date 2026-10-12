-- check_baseline.sql — lecture seule, ne modifie rien.
--
-- Vérifie qu'une base existante contient bien tout ce que faisaient les
-- anciennes migrations 002 à 014 (archivées dans
-- database/archive/migrations-before-baseline/). C'est la condition pour
-- la déclarer « baseline » avec 000_baseline.sql.
--
--   psql -h <hôte> -U <user> -d <base> -f check_baseline.sql
--
-- Chaque ligne doit afficher OK. Une ligne MANQUANT indique la migration
-- archivée à rejouer d'abord (elles sont toutes idempotentes).

SELECT migration, verification,
       CASE WHEN ok THEN 'OK' ELSE 'MANQUANT' END AS statut
FROM (VALUES
  ('002', 'table execution_variables',
     to_regclass('public.execution_variables') IS NOT NULL),
  ('003', 'colonne gateway_state.status',
     EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'gateway_state' AND column_name = 'status')),
  ('004', 'colonne jobs.error_code',
     EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'jobs' AND column_name = 'error_code')),
  ('004', 'table incidents',
     to_regclass('public.incidents') IS NOT NULL),
  ('004', 'table dmn_decisions',
     to_regclass('public.dmn_decisions') IS NOT NULL),
  ('004', 'colonne process_instances.parent_instance_id',
     EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'process_instances' AND column_name = 'parent_instance_id')),
  ('004', 'colonne event_subscriptions.target_element_id',
     EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'event_subscriptions' AND column_name = 'target_element_id')),
  ('004', 'colonne tasks.priority',
     EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'tasks' AND column_name = 'priority')),
  ('004', 'colonne users.tenant_id',
     EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'tenant_id')),
  ('004', 'colonne users.oidc_subject',
     EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'oidc_subject')),
  ('005', 'tables historic_* (process, activity, tasks, variables)',
     to_regclass('public.historic_process_instances') IS NOT NULL
     AND to_regclass('public.historic_activity_instances') IS NOT NULL
     AND to_regclass('public.historic_tasks') IS NOT NULL
     AND to_regclass('public.historic_variables') IS NOT NULL),
  ('006', 'table process_permissions',
     to_regclass('public.process_permissions') IS NOT NULL),
  ('007', 'table zeebe_keys',
     to_regclass('public.zeebe_keys') IS NOT NULL),
  ('008', 'index idx_jobs_type_status_pending',
     to_regclass('public.idx_jobs_type_status_pending') IS NOT NULL),
  ('009', 'table historic_incidents',
     to_regclass('public.historic_incidents') IS NOT NULL),
  ('010', 'index idx_historic_process_instances_started_at',
     to_regclass('public.idx_historic_process_instances_started_at') IS NOT NULL),
  ('011', 'tables eventlog_projected_*',
     to_regclass('public.eventlog_projected_instances') IS NOT NULL
     AND to_regclass('public.eventlog_projected_jobs') IS NOT NULL),
  ('012', 'colonne process_definitions.write_path',
     EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'process_definitions' AND column_name = 'write_path')),
  ('013', 'clé primaire composite (process_instance_id, id) sur executions',
     (SELECT count(*) FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
       WHERE i.indrelid = 'public.executions'::regclass AND i.indisprimary
         AND a.attname = 'process_instance_id') = 1),
  ('013', 'FK executions_parent_execution_id_fkey supprimée',
     NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'executions_parent_execution_id_fkey')),
  ('014', 'code de permission CAN_MANGE_USER corrigé',
     NOT EXISTS (SELECT 1 FROM public.actions WHERE code IN ('CAN_MANGE_USER', 'CAN_MANAGE_USERS')))
) AS checks(migration, verification, ok)
ORDER BY migration;
