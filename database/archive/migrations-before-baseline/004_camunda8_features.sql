-- Migration 004: Camunda 8 parity features (incidents, call activity,
-- event-based gateway, DMN, task priority, forms, multi-tenancy, OIDC)
--
-- Captures everything added to database/01_schema.sql while working through
-- the Camunda 8 parity roadmap, for databases that were initialized before
-- that work started (i.e. anything at or before migration 003). All
-- statements are idempotent (IF NOT EXISTS) so this is safe to run more
-- than once or against a database that already has some of these.

ALTER TABLE public.jobs
    ADD COLUMN IF NOT EXISTS error_code TEXT NULL;   -- BPMN error code thrown by worker (for error boundary events)

-- =========================================================
-- INCIDENTS
-- Created when a job/task fails permanently with no boundary event
-- =========================================================
CREATE TABLE IF NOT EXISTS public.incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL
        REFERENCES public.process_instances(id)
        ON DELETE CASCADE,
    job_id UUID NULL
        REFERENCES public.jobs(id)
        ON DELETE CASCADE,
    incident_type TEXT NOT NULL,  -- failedExternalTask, failedJob
    activity_id TEXT NOT NULL,    -- BPMN element ID where failure occurred
    error_message TEXT NULL,
    error_code TEXT NULL,
    state TEXT NOT NULL DEFAULT 'open',  -- open, resolved, deleted
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_incidents_process_instance_id
ON public.incidents(process_instance_id);

CREATE INDEX IF NOT EXISTS idx_incidents_state
ON public.incidents(state);

-- =========================================================
-- CALL ACTIVITY: parent process tracking
-- =========================================================
ALTER TABLE public.process_instances
    ADD COLUMN IF NOT EXISTS parent_instance_id UUID NULL REFERENCES public.process_instances(id),
    ADD COLUMN IF NOT EXISTS parent_execution_id UUID NULL;
-- Note: no FK on parent_execution_id to avoid circular reference with executions

-- =========================================================
-- EVENT-BASED GATEWAY: target element tracking per subscription
-- =========================================================
ALTER TABLE public.event_subscriptions
    ADD COLUMN IF NOT EXISTS target_element_id TEXT NULL;
-- Stores the BPMN node to advance to when this subscription fires (EBG routing)

-- =========================================================
-- DMN DECISIONS
-- Stores parsed DMN decision tables, deployed alongside BPMN
-- resources and evaluated by Business Rule Tasks (zeebe:calledDecision)
-- or directly via POST /v2/decisions/:key/evaluation.
-- =========================================================
CREATE TABLE IF NOT EXISTS public.dmn_decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID NOT NULL
        REFERENCES public.deployments(id)
        ON DELETE CASCADE,
    decision_key TEXT NOT NULL, -- DMN <decision id="...">
    decision_name TEXT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT true,
    dmn_xml TEXT NOT NULL,
    parsed_table JSONB NULL, -- Parsed decision table cache (inputs/outputs/rules)
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(decision_key, version)
);

CREATE INDEX IF NOT EXISTS idx_dmn_decisions_decision_key
ON public.dmn_decisions(decision_key);

CREATE INDEX IF NOT EXISTS idx_dmn_decisions_deployment_id
ON public.dmn_decisions(deployment_id);

-- =========================================================
-- TASK PRIORITY
-- Camunda 8 zeebe:priorityDefinition support (0-100, default 50)
-- =========================================================
ALTER TABLE public.tasks
    ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 50;

-- =========================================================
-- FORMS
-- Stores linked form definitions (.form JSON resources deployed
-- alongside BPMN/DMN files, referenced from a userTask via
-- zeebe:formDefinition formId="..."), versioned like process
-- definitions and DMN decisions.
-- =========================================================
CREATE TABLE IF NOT EXISTS public.forms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID NOT NULL
        REFERENCES public.deployments(id)
        ON DELETE CASCADE,
    form_id TEXT NOT NULL, -- .form JSON "id" field, referenced by zeebe:formDefinition formId
    version INTEGER NOT NULL DEFAULT 1,
    schema JSONB NOT NULL, -- Raw form JSON schema
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(form_id, version)
);

CREATE INDEX IF NOT EXISTS idx_forms_form_id
ON public.forms(form_id);

-- =========================================================
-- MULTI-TENANCY
-- Each user belongs to at most one tenant (NULL = superuser /
-- untenanted, sees everything — matches existing superuser semantics).
-- Process instances are stamped with the caller's tenant at creation
-- time, so list/search/get endpoints can filter by the caller's tenant
-- without a join.
-- =========================================================
ALTER TABLE public.users
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NULL;

ALTER TABLE public.process_instances
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_users_tenant_id ON public.users(tenant_id);
CREATE INDEX IF NOT EXISTS idx_process_instances_tenant_id ON public.process_instances(tenant_id);

-- =========================================================
-- OIDC IDENTITY
-- Lets a user authenticate via an external OIDC provider (Keycloak,
-- Auth0, etc.) instead of (or in addition to) a local password. The
-- local password_hash stays usable as a standalone fallback.
-- =========================================================
ALTER TABLE public.users
    ADD COLUMN IF NOT EXISTS oidc_subject TEXT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_oidc_subject ON public.users(oidc_subject) WHERE oidc_subject IS NOT NULL;
