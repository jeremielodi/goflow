-- =========================================================
-- EXTENSIONS
-- =========================================================

-- UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =========================================================
-- ENUM TYPES
-- =========================================================

-- Process instance lifecycle status
CREATE TYPE process_instance_status AS ENUM (
    'running',
    'suspended',
    'completed',
    'terminated'
);

-- Execution (Token) status
CREATE TYPE execution_status AS ENUM (
    'active',
    'waiting',
    'completed',
    'terminated'
);

-- User task lifecycle status
CREATE TYPE task_status AS ENUM (
    'created',
    'claimed',
    'completed',
    'cancelled'
);

-- Deployment status
CREATE TYPE deployment_status AS ENUM (
    'active',
    'inactive'
);

-- =========================================================
-- USERS
-- Represents application users
-- =========================================================
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Unique user identifier
    email TEXT UNIQUE NOT NULL, -- User's email address (login)
    full_name TEXT, -- User's full display name
    password_hash TEXT NOT NULL, -- Hashed password for authentication
    is_active BOOLEAN NOT NULL DEFAULT true, -- Whether user account is active
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), -- Account creation timestamp
    updated_at TIMESTAMP NOT NULL DEFAULT NOW() -- Last profile update timestamp
);

CREATE INDEX idx_users_email
ON users(email);


CREATE TABLE IF NOT EXISTS roles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  label varchar(50) NOT NULL UNIQUE,
  isDefault INT DEFAULT 0
)WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;


CREATE TABLE IF NOT EXISTS user_roles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL,
  roles_id UUID NOT NULL,
  FOREIGN KEY (roles_id) REFERENCES public.roles (id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (user_id) REFERENCES public.users (id) ON DELETE CASCADE ON UPDATE CASCADE
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;




CREATE TABLE public.actions (
  id INT NOT NULL,
  code VARCHAR(40),
  description VARCHAR(100) NOT NULL,
  CONSTRAINT action_pkey PRIMARY KEY (id)
)WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE public.role_actions(
  uuid UUID NOT NULL,
  roles_id UUID NOT NULL,
  action_id INT NOT NULL,
  CONSTRAINT role_action_pkey PRIMARY KEY (uuid),
  CONSTRAINT role_actions__action FOREIGN KEY (action_id) REFERENCES public.actions (id) ON UPDATE CASCADE,
  CONSTRAINT role_actions__role FOREIGN KEY (roles_id) REFERENCES public.roles (id) ON UPDATE CASCADE  ON DELETE CASCADE
)WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;


-- =========================================================
-- DEPLOYMENTS
-- One deployment can contain one or multiple BPMN files
-- Similar to Camunda deployments
-- =========================================================
CREATE TABLE deployments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Unique deployment identifier
    name TEXT NOT NULL, -- Human-readable deployment name
    deployed_by UUID NULL REFERENCES users(id), -- User who deployed (nullable)
    status deployment_status NOT NULL DEFAULT 'active', -- Current deployment status
    tenant_id TEXT NULL, -- Multi-tenancy identifier
    created_at TIMESTAMP NOT NULL DEFAULT NOW() -- Deployment creation timestamp
);

-- =========================================================
-- PROCESS DEFINITIONS
-- Stores parsed BPMN process definitions
--
-- IMPORTANT:
-- BPMN XML should NEVER be parsed during execution.
-- Parse once during deployment and persist metadata.
-- =========================================================
CREATE TABLE process_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Unique process definition identifier
    deployment_id UUID NOT NULL -- References the deployment containing this definition
        REFERENCES deployments(id)
        ON DELETE CASCADE,
    process_key TEXT NOT NULL, -- Unique technical key for the process
    tenant_id TEXT NULL, -- Multi-tenancy identifier
    process_name TEXT NULL, -- Human-readable process name
    version INTEGER NOT NULL DEFAULT 1, -- Version number (increments on changes)
    engine_type TEXT NOT NULL DEFAULT 'unknown', -- Type of BPMN engine (for compatibility)
    is_active BOOLEAN NOT NULL DEFAULT true, -- Whether this version is active for new instances
    bpmn_xml TEXT NOT NULL, -- Original BPMN XML uploaded from Camunda Modeler
    parsed_graph JSONB NULL, -- Parsed process graph/cache with node structure
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), -- Definition creation timestamp
    -- Phase 1 (see architecture/event-sourced-kafka-design.md, migration
    -- 012_process_write_path.sql): 'legacy' (default, unchanged Postgres-
    -- transactional engine) or 'partitioned' (Kafka command-log write path,
    -- gated to a narrow BPMN shape by a deploy-time validator).
    write_path TEXT NOT NULL DEFAULT 'legacy' CHECK (write_path IN ('legacy', 'partitioned')),
    UNIQUE(process_key, version)
);

CREATE INDEX idx_process_definitions_process_key
ON process_definitions(process_key);

CREATE INDEX idx_process_definitions_deployment_id
ON process_definitions(deployment_id);

-- =========================================================
-- PROCESS INSTANCES
-- Runtime instance of a process definition
-- =========================================================
CREATE TABLE process_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Unique process instance identifier
    process_definition_id UUID NOT NULL -- References the process definition being executed
        REFERENCES process_definitions(id),
    status process_instance_status NOT NULL DEFAULT 'running', -- Current lifecycle state
    started_by UUID NULL -- User who started this instance
        REFERENCES users(id),
    transaction_scope_id UUID,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(), -- When the instance began
    ended_at TIMESTAMP NULL -- When the instance terminated/completed (NULL if running)
);

CREATE INDEX idx_process_instances_definition_id
ON process_instances(process_definition_id);

CREATE INDEX idx_process_instances_status
ON process_instances(status);




-- Need to track gateway state
-- Primary key is composite (Citus phase 2): Citus requires any
-- UNIQUE/PRIMARY KEY constraint on a distributed table to include the
-- distribution column (process_instance_id here) — a plain `id` PK alone
-- can't be enforced across shards. Nothing referenced gateway_state(id)
-- by itself once executions.gateway_join_id's FK was dropped (see above),
-- so this is safe. See architecture/citus-distributed-storage-design.md.
CREATE TABLE gateway_state (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL REFERENCES process_instances(id),
    gateway_id TEXT NOT NULL, -- BPMN element ID
    expected_incoming INTEGER NOT NULL, -- How many incoming flows expected
    received_incoming INTEGER DEFAULT 0, -- How many received
    joined_flows JSONB DEFAULT '[]', -- Which flows have joined
    status VARCHAR(50) NOT NULL DEFAULT 'waiting', -- waiting | completed
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    PRIMARY KEY (process_instance_id, id),
    UNIQUE(process_instance_id, gateway_id)
);



-- =========================================================
-- EXECUTIONS (TOKENS)
-- Core BPMN runtime execution state
--
-- Each execution represents a token moving
-- through the process graph.
--
-- Parallel gateways later create multiple executions.
-- =========================================================
-- Primary key is composite (Citus phase 2, see the gateway_state comment
-- above for why): nothing referenced executions(id) alone once
-- parent_execution_id/gateway_join_id/tasks.execution_id/jobs.execution_id/
-- etc.'s FKs were dropped, so this is safe.
CREATE TABLE executions (
    id UUID NOT NULL DEFAULT gen_random_uuid(), -- Unique execution token identifier
    process_instance_id UUID NOT NULL -- Parent process instance
        REFERENCES process_instances(id)
        ON DELETE CASCADE,
    current_element_id TEXT NOT NULL, -- BPMN node ID currently being executed
    -- parent_execution_id and gateway_join_id have no FK (Citus phase 2):
    -- both would reference executions(id)/gateway_state(id) by a column
    -- other than process_instance_id, which Citus can't verify as
    -- colocated even though both tables share the same colocation group —
    -- see architecture/citus-distributed-storage-design.md.
    parent_execution_id UUID NULL, -- Supports future parallel gateways (hierarchy)
    status execution_status NOT NULL DEFAULT 'active', -- Current execution state
    is_active BOOLEAN NOT NULL DEFAULT true, -- Whether this token is still active
    path_id TEXT,
    gateway_join_id UUID,
    branch_variables JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), -- When execution started
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(), -- Last state change timestamp
    PRIMARY KEY (process_instance_id, id)
);

CREATE INDEX idx_executions_process_instance_id
ON executions(process_instance_id);

CREATE INDEX idx_executions_current_element_id
ON executions(current_element_id);

CREATE INDEX idx_executions_is_active
ON executions(is_active);

CREATE INDEX idx_executions_parent_gateway 
ON executions(parent_execution_id, gateway_join_id) 
WHERE is_active = true;


-- Event subprocess tracking
-- process_instance_id/parent_execution_id have no FK (Citus phase 2):
-- this table stays local while process_instances/executions become
-- distributed, and a local table can't hold an FK into a distributed one
-- either direction — see architecture/citus-distributed-storage-design.md.
-- Rows are explicitly cleaned up by CleanupOrphanExecutionStateTx before
-- a process_instances delete, same as before this change.
CREATE TABLE event_subprocesses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL,
    parent_execution_id UUID,
    event_type TEXT NOT NULL, -- message, timer, error, etc.
    event_key TEXT, -- Message name or error code
    is_interrupting BOOLEAN DEFAULT true,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    triggered_at TIMESTAMP
);

-- Event subscriptions
-- process_instance_id/execution_id have no FK (Citus phase 2) — same
-- reasoning as event_subprocesses above.
CREATE TABLE event_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL,
    execution_id UUID,
    event_type TEXT NOT NULL, -- message, signal, error
    event_name TEXT NOT NULL, -- Specific event identifier
    correlation_key TEXT, -- For message correlation
    correlation_id UUID,
    message_payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    consumed_at TIMESTAMP
);

CREATE INDEX idx_event_subscriptions_lookup 
ON event_subscriptions(event_type, event_name, correlation_key) 
WHERE consumed_at IS NULL;


CREATE TABLE message_buffer (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_name TEXT NOT NULL,
    correlation_key TEXT,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,
    processing_error TEXT
);

CREATE INDEX idx_message_buffer_correlation 
ON message_buffer(message_name, correlation_key) 
WHERE processed_at IS NULL;

-- Boundary event attachments
-- process_instance_id/execution_id have no FK (Citus phase 2) — same
-- reasoning as event_subprocesses above.
CREATE TABLE boundary_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL,
    attached_to_id TEXT NOT NULL, -- BPMN element ID this attaches to
    execution_id UUID,
    event_type TEXT NOT NULL, -- timer, error, message, signal
    event_config JSONB, -- Timer duration, error code, etc.
    is_interrupting BOOLEAN DEFAULT true,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    triggered_at TIMESTAMP
);

CREATE INDEX idx_boundary_events_active 
ON boundary_events(process_instance_id, attached_to_id) 
WHERE is_active = true;

-- =========================================================
-- TASKS
-- Human workflow/tasklist layer
-- =========================================================
-- Primary key is composite (Citus phase 2, see the gateway_state comment
-- above for why): nothing referenced tasks(id) alone once
-- audit_logs.task_id's FK was dropped, so this is safe.
CREATE TABLE tasks (
    id UUID NOT NULL DEFAULT gen_random_uuid(), -- Unique task identifier
    process_instance_id UUID NOT NULL -- Parent process instance
        REFERENCES process_instances(id)
        ON DELETE CASCADE,
    execution_id UUID NULL, -- Associated execution token
    -- No FK on execution_id (Citus phase 2): tasks and executions are
    -- colocated, but this FK isn't on process_instance_id so Citus can't
    -- verify it — see architecture/citus-distributed-storage-design.md.
    -- process_instance_id's cascade FK above still fully cleans up tasks.
    task_definition_key TEXT NOT NULL, -- BPMN userTask element ID
    task_name TEXT NULL, -- BPMN task name (human-readable)
    assignee TEXT NULL, -- User assigned to this task
    candidate_group TEXT NULL, -- Group that can claim this task
    status task_status NOT NULL DEFAULT 'created', -- Task lifecycle state
    form_data JSONB NULL, -- Dynamic form values submitted by users
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), -- When task was created
    updated_at  TIMESTAMP NOT NULL DEFAULT NULL,
    claimed_at TIMESTAMP NULL, -- When user claimed the task
    completed_at TIMESTAMP NULL, -- When task was completed
    PRIMARY KEY (process_instance_id, id)
);

CREATE INDEX idx_tasks_process_instance_id
ON tasks(process_instance_id);

CREATE INDEX idx_tasks_execution_id
ON tasks(execution_id);

CREATE INDEX idx_tasks_assignee
ON tasks(assignee);

CREATE INDEX idx_tasks_status
ON tasks(status);

-- =========================================================
-- VARIABLES
-- Stores process variables
--
-- Flexible JSONB structure similar to Camunda variables.
-- =========================================================
-- Primary key is composite (Citus phase 2, see the gateway_state comment
-- above for why); the existing unique_process_instance_variables
-- constraint below already includes process_instance_id, so it needs no
-- change.
CREATE TABLE variables (
    id UUID NOT NULL DEFAULT gen_random_uuid(), -- Unique variable set identifier
    process_instance_id UUID NOT NULL -- Which process instance owns these variables
        REFERENCES process_instances(id)
        ON DELETE CASCADE,
    data JSONB NOT NULL, -- JSON object containing all process variables
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(), -- Last variable modification
    PRIMARY KEY (process_instance_id, id)
);

CREATE INDEX idx_variables_process_instance_id
ON variables(process_instance_id);

ALTER TABLE public.variables 
ADD CONSTRAINT unique_process_instance_variables 
UNIQUE (process_instance_id);

CREATE INDEX idx_variables_data
ON variables
USING GIN(data);

-- =========================================================
-- SERVICE TASK JOBS
-- Async job queue for workers
--
-- Workflow engine creates jobs.
-- Workers poll and complete jobs.
-- =========================================================
-- process_instance_id/execution_id have no FK (Citus phase 2): jobs stays
-- local (see architecture/citus-distributed-storage-design.md — the
-- global FOR UPDATE SKIP LOCKED claim queries need their own redesign
-- before this table can be distributed), while process_instances and
-- executions become distributed — a local table can't hold an FK into a
-- distributed one. Rows are explicitly cleaned up by
-- CleanupOrphanExecutionStateTx before a process_instances delete.
CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Unique job identifier
    process_instance_id UUID NOT NULL, -- Parent process instance
    execution_id UUID NULL, -- Associated execution token
    job_type TEXT NOT NULL, -- Type of service task (e.g., "http", "email")
    retries INTEGER NOT NULL DEFAULT 3, -- Number of remaining retry attempts
    status TEXT NOT NULL DEFAULT 'pending', -- Job state (pending/processing/completed/failed)
    payload JSONB NULL, -- Job-specific input data
    error_message TEXT NULL, -- Last error message if job failed
    error_code TEXT NULL,   -- BPMN error code thrown by worker (for error boundary events)
    locked_by TEXT NULL, -- Worker ID that locked this job
    locked_until TIMESTAMP NULL, -- When job lock expires
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), -- When job was created
    updated_at  TIMESTAMP NOT NULL DEFAULT NULL,
    completed_at TIMESTAMP NULL -- When job finished successfully
);

CREATE INDEX idx_jobs_status
ON jobs(status);

CREATE INDEX idx_jobs_type
ON jobs(job_type);

-- Composite partial index for the hot fetch-and-lock query (job_type +
-- status = 'pending') — see archive/migrations-before-baseline/008_jobs_composite_index.sql.
CREATE INDEX IF NOT EXISTS idx_jobs_type_status_pending
ON jobs(job_type, status)
WHERE status = 'pending';

-- =========================================================
-- TIMER JOBS
-- Used later for BPMN timers/events
-- =========================================================
-- process_instance_id/execution_id have no FK (Citus phase 2) — same
-- reasoning as jobs above.
CREATE TABLE timer_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Unique timer identifier
    process_instance_id UUID NOT NULL, -- Parent process instance
    execution_id UUID NULL, -- Associated execution token
    event_type TEXT NOT NULL, -- Type of timer (e.g., "intermediate", "boundary")
    due_at TIMESTAMP NOT NULL, -- When this timer should trigger
    payload JSONB NULL, -- Timer configuration (duration, cycle, etc.)
    is_triggered BOOLEAN NOT NULL DEFAULT false, -- Whether timer has fired
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), -- When timer was scheduled
    triggered_at TIMESTAMP NULL,
    cycle_count INT DEFAULT 0,
    total_cycles INT DEFAULT 1,
    repeat_interval VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_timer_jobs_due_triggered ON public.timer_jobs(due_at, is_triggered);

CREATE INDEX idx_timer_jobs_due_at
ON timer_jobs(due_at);

CREATE INDEX idx_timer_jobs_triggered
ON timer_jobs(is_triggered);

-- =========================================================
-- AUDIT LOGS
-- Enterprise-grade traceability
-- =========================================================
-- process_instance_id/task_id have no FK (Citus phase 2): audit_logs stays
-- local (nullable process_instance_id — some rows are pure user actions
-- with no instance at all, which Citus's distribution-column requirement
-- can't represent; see architecture/citus-distributed-storage-design.md)
-- while process_instances/tasks become distributed. user_id keeps its FK
-- since users is a Citus reference table (Phase 1) — local tables can
-- always reference a reference table safely. Rows are explicitly cleaned
-- up by CleanupOrphanExecutionStateTx before a process_instances delete.
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Unique audit entry identifier
    process_instance_id UUID NULL, -- Related process instance (if any)
    task_id UUID NULL, -- Related task (if any)
    user_id UUID NULL -- User who performed the action
        REFERENCES users(id),
    action TEXT NOT NULL, -- Type of action performed (e.g., "claim", "complete")
    old_data JSONB NULL, -- Previous state before action
    new_data JSONB NULL, -- New state after action
    created_at TIMESTAMP NOT NULL DEFAULT NOW() -- When action occurred
);

CREATE INDEX idx_audit_logs_process_instance_id
ON audit_logs(process_instance_id);

CREATE INDEX idx_audit_logs_task_id
ON audit_logs(task_id);

-- =========================================================
-- INCIDENTS
-- Created when a job/task fails permanently with no boundary event
-- =========================================================
-- Primary key is composite (Citus phase 2, see the gateway_state comment
-- above for why): nothing referenced incidents(id) alone (historic_incidents
-- reuses the same id value but has no FK to the live table).
CREATE TABLE incidents (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL
        REFERENCES process_instances(id)
        ON DELETE CASCADE,
    job_id UUID NULL,
    -- No FK on job_id (Citus phase 2): incidents becomes distributed while
    -- jobs stays local — see architecture/citus-distributed-storage-design.md.
    -- process_instance_id's cascade FK above still fully cleans up incidents.
    incident_type TEXT NOT NULL,  -- failedExternalTask, failedJob
    activity_id TEXT NOT NULL,    -- BPMN element ID where failure occurred
    error_message TEXT NULL,
    error_code TEXT NULL,
    state TEXT NOT NULL DEFAULT 'open',  -- open, resolved, deleted
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMP NULL,
    PRIMARY KEY (process_instance_id, id)
);

CREATE INDEX idx_incidents_process_instance_id
ON incidents(process_instance_id);

CREATE INDEX idx_incidents_state
ON incidents(state);


CREATE TABLE process_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Unique outbox message identifier
    process_instance_id UUID NOT NULL, -- Related process instance
    event_type TEXT NOT NULL, -- Type of event (e.g., "TaskCompleted", "GatewayTaken")
    payload JSONB NOT NULL, -- Full event details as JSON
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), -- When event was created
    published_at TIMESTAMP, -- When event was published (NULL = pending)
    version INTEGER DEFAULT 1 -- Optimistic locking version
);

CREATE INDEX idx_outbox_unpublished ON process_outbox(published_at) 
    WHERE published_at IS NULL;



-- Multi-instance executions table
-- execution_id has no FK (Citus phase 2): multi_instance_executions and
-- executions are colocated, but this FK isn't on process_instance_id so
-- Citus can't verify it — see architecture/citus-distributed-storage-design.md.
-- process_instance_id's cascade FK below still fully cleans up this table.
-- Primary key is composite (Citus phase 2, see the gateway_state comment
-- above for why): nothing referenced multi_instance_executions(id) alone.
CREATE TABLE IF NOT EXISTS public.multi_instance_executions (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL REFERENCES public.process_instances(id) ON DELETE CASCADE,
    execution_id UUID NOT NULL,
    activity_id VARCHAR(255) NOT NULL,
    is_sequential BOOLEAN NOT NULL DEFAULT false,
    total_count INT NOT NULL DEFAULT 0,
    completed_count INT NOT NULL DEFAULT 0,
    nr_of_active_instances INT NOT NULL DEFAULT 0,
    nr_of_completed_instances INT NOT NULL DEFAULT 0,
    loop_counter INT NOT NULL DEFAULT 0,
    completion_condition TEXT,
    element_variable VARCHAR(255),
    input_collection VARCHAR(255),
    output_collection VARCHAR(255),
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (process_instance_id, id)
);

-- Multi-instance children table
-- parent_execution_id/child_execution_id have no FK (Citus phase 2): this
-- table stays local (no process_instance_id column to colocate on) while
-- executions becomes distributed — see
-- architecture/citus-distributed-storage-design.md. Rows are explicitly
-- cleaned up (via a subquery against executions) before a
-- process_instances delete.
CREATE TABLE IF NOT EXISTS public.multi_instance_children (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_execution_id UUID NOT NULL,
    child_execution_id UUID NOT NULL,
    loop_index INT NOT NULL,
    element_value TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE INDEX idx_multi_instance_execs_process ON public.multi_instance_executions(process_instance_id);
CREATE INDEX idx_multi_instance_children_parent ON public.multi_instance_children(parent_execution_id);

-- Execution-scoped variables for parallel gateway branch isolation
-- execution_id has no FK (Citus phase 2) — same reasoning as
-- multi_instance_children above.
CREATE TABLE IF NOT EXISTS public.execution_variables (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id    UUID NOT NULL,
    variables       JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_execution_variables_exec ON public.execution_variables(execution_id);

-- =========================================================
-- COMMENTS
-- =========================================================

COMMENT ON TABLE deployments IS
'Groups BPMN resources uploaded together';

COMMENT ON TABLE process_definitions IS
'Stores deployed BPMN definitions and parsed graphs';

COMMENT ON TABLE executions IS
'Represents BPMN execution tokens moving through the process graph';

COMMENT ON TABLE tasks IS
'Human tasks waiting for user interaction';

COMMENT ON TABLE jobs IS
'Async service task jobs executed by workers';

COMMENT ON TABLE timer_jobs IS 'Stores BPMN timer jobs';

COMMENT ON TABLE variables IS 'Stores process variables as JSONB';

COMMENT ON TABLE audit_logs IS 'Tracks all important workflow actions';

-- =========================================================
-- CALL ACTIVITY: parent process tracking
-- =========================================================
ALTER TABLE process_instances
  ADD COLUMN IF NOT EXISTS parent_instance_id UUID NULL,
  ADD COLUMN IF NOT EXISTS parent_execution_id UUID NULL;
-- Note: no FK on parent_execution_id to avoid circular reference with executions.
-- parent_instance_id also has no FK (Citus phase 2, see
-- architecture/citus-distributed-storage-design.md) — a call activity's
-- parent and child are different process_instances rows with independent
-- distribution-column hashes, not guaranteed colocated, so this
-- self-reference can't be Citus-verified regardless of distribution.

-- =========================================================
-- EVENT-BASED GATEWAY: target element tracking per subscription
-- =========================================================
ALTER TABLE event_subscriptions
  ADD COLUMN IF NOT EXISTS target_element_id TEXT NULL;
-- Stores the BPMN node to advance to when this subscription fires (EBG routing)

-- =========================================================
-- DMN DECISIONS
-- Stores parsed DMN decision tables, deployed alongside BPMN
-- resources and evaluated by Business Rule Tasks (zeebe:calledDecision)
-- or directly via POST /v2/decisions/:key/evaluation.
-- =========================================================
CREATE TABLE IF NOT EXISTS dmn_decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID NOT NULL
        REFERENCES deployments(id)
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
ON dmn_decisions(decision_key);

CREATE INDEX IF NOT EXISTS idx_dmn_decisions_deployment_id
ON dmn_decisions(deployment_id);

-- =========================================================
-- TASK PRIORITY
-- Camunda 8 zeebe:priorityDefinition support (0-100, default 50)
-- =========================================================
ALTER TABLE tasks
  ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 50;

-- =========================================================
-- FORMS
-- Stores linked form definitions (.form JSON resources deployed
-- alongside BPMN/DMN files, referenced from a userTask via
-- zeebe:formDefinition formId="..."), versioned like process
-- definitions and DMN decisions.
-- =========================================================
CREATE TABLE IF NOT EXISTS forms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    deployment_id UUID NOT NULL
        REFERENCES deployments(id)
        ON DELETE CASCADE,
    form_id TEXT NOT NULL, -- .form JSON "id" field, referenced by zeebe:formDefinition formId
    version INTEGER NOT NULL DEFAULT 1,
    schema JSONB NOT NULL, -- Raw form JSON schema
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(form_id, version)
);

CREATE INDEX IF NOT EXISTS idx_forms_form_id
ON forms(form_id);

-- =========================================================
-- MULTI-TENANCY
-- Each user belongs to at most one tenant (NULL = superuser /
-- untenanted, sees everything — matches existing superuser semantics).
-- Process instances are stamped with their process definition's tenant
-- at creation time, so list/search/get endpoints can filter by the
-- caller's tenant without a join.
-- =========================================================
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS tenant_id TEXT NULL;

ALTER TABLE process_instances
  ADD COLUMN IF NOT EXISTS tenant_id TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_users_tenant_id ON users(tenant_id);
CREATE INDEX IF NOT EXISTS idx_process_instances_tenant_id ON process_instances(tenant_id);

-- =========================================================
-- OIDC IDENTITY
-- Lets a user authenticate via an external OIDC provider (Keycloak,
-- Auth0, etc.) instead of (or in addition to) a local password. The
-- local password_hash stays usable as a standalone fallback.
-- =========================================================
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS oidc_subject TEXT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_oidc_subject ON users(oidc_subject) WHERE oidc_subject IS NOT NULL;

-- =========================================================
-- HISTORIC ARCHIVE
-- Once a process instance completes or terminates, its data is copied
-- here and removed from the live tables (process_instances/tasks/
-- executions/variables), so the live tables only ever hold currently
-- running instances — closer to Camunda 7's ACT_RU_* (runtime) vs
-- ACT_HI_* (history) split. See internal/service/archive_service.go.
-- =========================================================
CREATE TABLE IF NOT EXISTS historic_process_instances (
    id UUID PRIMARY KEY, -- same id as the original process_instances row
    process_definition_id UUID NOT NULL,
    process_definition_key TEXT NOT NULL, -- denormalized, survives even if the definition is later removed
    process_definition_version INTEGER NULL,
    tenant_id TEXT NULL,
    started_by TEXT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    duration_millis BIGINT NULL,
    state TEXT NOT NULL, -- 'completed' | 'terminated'
    delete_reason TEXT NULL,
    archived_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_historic_process_instances_definition_key
ON historic_process_instances(process_definition_key);

CREATE INDEX IF NOT EXISTS idx_historic_process_instances_tenant_id
ON historic_process_instances(tenant_id);

-- Analytics query support — see archive/migrations-before-baseline/010_historic_process_instance_time_indexes.sql.
CREATE INDEX IF NOT EXISTS idx_historic_process_instances_started_at
ON historic_process_instances(started_at);

CREATE INDEX IF NOT EXISTS idx_historic_process_instances_ended_at
ON historic_process_instances(ended_at);

CREATE INDEX IF NOT EXISTS idx_historic_process_instances_key_started_at
ON historic_process_instances(process_definition_key, started_at);

-- The archived form of a token-history step (see TokenHistoryStep in
-- internal/service/token_history_service.go, which this mirrors exactly
-- so BuildTokenHistory's output can be inserted here unchanged).
CREATE TABLE IF NOT EXISTS historic_activity_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL
        REFERENCES historic_process_instances(id)
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
ON historic_activity_instances(process_instance_id);

CREATE TABLE IF NOT EXISTS historic_tasks (
    id UUID PRIMARY KEY, -- same id as the original tasks row
    process_instance_id UUID NOT NULL
        REFERENCES historic_process_instances(id)
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
ON historic_tasks(process_instance_id);

-- Final variable snapshot (Camunda 7's HistoricVariableInstance
-- semantics: current value at process end, not a full change log — the
-- change log, when needed, comes from historic_activity_instances'
-- VARIABLES_UPDATED/MERGED-derived detail text).
CREATE TABLE IF NOT EXISTS historic_variables (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    process_instance_id UUID NOT NULL
        REFERENCES historic_process_instances(id)
        ON DELETE CASCADE,
    data JSONB NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_historic_variables_process_instance_id
ON historic_variables(process_instance_id);

-- Archived incidents — see archive/migrations-before-baseline/009_historic_incidents.sql for why
-- this exists (incidents.process_instance_id has ON DELETE CASCADE, so
-- without this, every incident vanishes the moment its instance archives).
CREATE TABLE IF NOT EXISTS historic_incidents (
    id UUID PRIMARY KEY,
    process_instance_id UUID NOT NULL
        REFERENCES historic_process_instances(id)
        ON DELETE CASCADE,
    process_definition_key TEXT NOT NULL,
    job_id UUID NULL,
    incident_type TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    error_message TEXT NULL,
    error_code TEXT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP NULL,
    archived_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_historic_incidents_process_definition_key
ON historic_incidents(process_definition_key);

CREATE INDEX IF NOT EXISTS idx_historic_incidents_process_instance_id
ON historic_incidents(process_instance_id);

-- Per-resource ACL for process definitions, keyed by process_key (covers
-- every version under that key). A key with zero rows here is
-- unrestricted (open to any authenticated user, unchanged behavior).
-- resource_type is kept generic for a future 'deployment' type; only
-- 'process_definition' is enforced today.
CREATE TABLE IF NOT EXISTS process_permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_type TEXT NOT NULL DEFAULT 'process_definition',
    process_key TEXT NOT NULL,
    grantee_type TEXT NOT NULL CHECK (grantee_type IN ('user', 'role')),
    grantee_id UUID NOT NULL,
    permission TEXT NOT NULL CHECK (permission IN ('VIEW', 'START', 'MANAGE')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (process_key, grantee_type, grantee_id, permission)
);

-- Maps GoFlow's internal UUIDs to the int64 "keys" the Zeebe gRPC gateway
-- protocol requires (processDefinitionKey/processInstanceKey/job key are
-- all int64 on the wire) — see internal/zeebegrpc. Keys are assigned
-- lazily, the first time a resource is exposed over gRPC, and must
-- persist across restarts since a client holds onto a job key between
-- ActivateJobs and a later CompleteJob/FailJob call.
CREATE TABLE IF NOT EXISTS zeebe_keys (
    key BIGSERIAL PRIMARY KEY,
    resource_type TEXT NOT NULL,
    resource_id UUID NOT NULL,
    UNIQUE (resource_type, resource_id)
);

CREATE INDEX IF NOT EXISTS idx_process_permissions_key
ON process_permissions(process_key);

-- Phase 0 event-sourced engine scoping (shadow mode) — see
-- archive/migrations-before-baseline/011_eventlog_projection.sql and
-- architecture/event-sourced-kafka-design.md. Populated only by
-- internal/eventlog.Consumer's background projector, not read/written by
-- any real request path — exists purely to prove engine state is
-- reconstructable from the published event stream alone.
CREATE TABLE IF NOT EXISTS eventlog_projected_instances (
    id UUID PRIMARY KEY,
    process_definition_id UUID NULL,
    process_key TEXT NULL,
    version INT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NULL,
    ended_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS eventlog_projected_jobs (
    id UUID PRIMARY KEY,
    process_instance_id UUID NOT NULL,
    execution_id UUID NULL,
    job_type TEXT NULL,
    status TEXT NOT NULL,
    error_message TEXT NULL,
    retries_remaining INT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_eventlog_projected_jobs_process_instance_id
ON eventlog_projected_jobs(process_instance_id);

CREATE INDEX IF NOT EXISTS idx_process_permissions_grantee
ON process_permissions(grantee_type, grantee_id);

CREATE TABLE IF NOT EXISTS public.published_messages (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    correlation_key TEXT NOT NULL DEFAULT '',
    message_id      TEXT NULL,          -- identifiant fourni par l'émetteur, pour la déduplication
    variables       JSONB NOT NULL DEFAULT '{}'::jsonb,
    tenant_id       TEXT NULL,
    expires_at      TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_published_messages_lookup
ON public.published_messages (name, correlation_key, expires_at);

CREATE INDEX IF NOT EXISTS idx_published_messages_message_id
ON public.published_messages (name, message_id) WHERE message_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.published_message_correlations (
    published_message_id UUID NOT NULL REFERENCES public.published_messages(id) ON DELETE CASCADE,
    process_key         TEXT NOT NULL,      -- un message se corrèle au plus une fois par processus
    process_instance_id UUID NOT NULL,
    correlated_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (published_message_id, process_key)
);

-- Variables locales aux portées (migration 002).
CREATE TABLE IF NOT EXISTS public.scope_variables (
    process_instance_id UUID NOT NULL,
    scope_id            UUID NOT NULL,      -- exécution qui porte la portée
    data                JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (process_instance_id, scope_id)
);


-- Suivi des migrations (voir database/migrations/README.md). Une base neuve
-- part directement de la baseline 000.
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version     TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO public.schema_migrations (version, description)
VALUES ('000', 'baseline — schéma de database/01_schema.sql (anciennes migrations 002-014 incluses)')
ON CONFLICT (version) DO NOTHING;

INSERT INTO public.schema_migrations (version, description)
VALUES ('001', 'published_messages : messages publiés en attente de corrélation (TTL)')
ON CONFLICT (version) DO NOTHING;

INSERT INTO public.schema_migrations (version, description)
VALUES ('002', 'scope_variables : variables locales aux portées')
ON CONFLICT (version) DO NOTHING;