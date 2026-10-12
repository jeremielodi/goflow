-- Migration 005: Historic archive (dedicated tables, decoupled from live tables)
--
-- Once a process instance completes or terminates, its data is copied to
-- these tables and removed from the live tables (process_instances/tasks/
-- executions/variables) — see internal/service/archive_service.go. Live
-- tables then only ever hold currently running instances, closer to
-- Camunda 7's ACT_RU_* (runtime) vs ACT_HI_* (history) split.
--
-- All statements are idempotent (IF NOT EXISTS) so this is safe to run
-- more than once or against a database that already has some of these.

CREATE TABLE IF NOT EXISTS public.historic_process_instances (
    id UUID PRIMARY KEY,
    process_definition_id UUID NOT NULL,
    process_definition_key TEXT NOT NULL,
    process_definition_version INTEGER NULL,
    tenant_id TEXT NULL,
    started_by TEXT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    duration_millis BIGINT NULL,
    state TEXT NOT NULL,
    delete_reason TEXT NULL,
    archived_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_historic_process_instances_definition_key
ON public.historic_process_instances(process_definition_key);

CREATE INDEX IF NOT EXISTS idx_historic_process_instances_tenant_id
ON public.historic_process_instances(tenant_id);

CREATE TABLE IF NOT EXISTS public.historic_activity_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL
        REFERENCES public.historic_process_instances(id)
        ON DELETE CASCADE,
    execution_id UUID NULL,
    action TEXT NOT NULL,
    element_id TEXT NULL,
    element_name TEXT NULL,
    task_id UUID NULL,
    detail TEXT NULL,
    occurred_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_historic_activity_instances_process_instance_id
ON public.historic_activity_instances(process_instance_id);

CREATE TABLE IF NOT EXISTS public.historic_tasks (
    id UUID PRIMARY KEY,
    process_instance_id UUID NOT NULL
        REFERENCES public.historic_process_instances(id)
        ON DELETE CASCADE,
    task_definition_key TEXT NOT NULL,
    task_name TEXT NULL,
    assignee TEXT NULL,
    status TEXT NOT NULL,
    priority INTEGER NULL,
    created_at TIMESTAMP NOT NULL,
    claimed_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_historic_tasks_process_instance_id
ON public.historic_tasks(process_instance_id);

CREATE TABLE IF NOT EXISTS public.historic_variables (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL
        REFERENCES public.historic_process_instances(id)
        ON DELETE CASCADE,
    data JSONB NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_historic_variables_process_instance_id
ON public.historic_variables(process_instance_id);
