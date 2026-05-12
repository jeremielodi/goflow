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
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    email TEXT UNIQUE NOT NULL,

    full_name TEXT,

    password_hash TEXT NOT NULL,

    is_active BOOLEAN NOT NULL DEFAULT true,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_email
ON users(email);

-- =========================================================
-- DEPLOYMENTS
-- One deployment can contain one or multiple BPMN files
-- Similar to Camunda deployments
-- =========================================================
CREATE TABLE deployments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    name TEXT NOT NULL,

    deployed_by UUID NULL REFERENCES users(id),

    status deployment_status NOT NULL DEFAULT 'active',
    tenant_id TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
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
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    deployment_id UUID NOT NULL
        REFERENCES deployments(id)
        ON DELETE CASCADE,

    process_key TEXT NOT NULL,

    tenant_id TEXT NULL,
    
    process_name TEXT NULL,

    version INTEGER NOT NULL DEFAULT 1,

    engine_type TEXT NOT NULL DEFAULT 'unknown'

    is_active BOOLEAN NOT NULL DEFAULT true,

    -- Original BPMN XML uploaded from Camunda Modeler
    bpmn_xml TEXT NOT NULL,

    -- Parsed process graph/cache
    -- Example:
    -- {
    --   "nodes": {
    --      "StartEvent_1": {
    --          "type": "startEvent"
    --      }
    --   }
    -- }
    parsed_graph JSONB NULL,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

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
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    process_definition_id UUID NOT NULL
        REFERENCES process_definitions(id),

    status process_instance_status NOT NULL DEFAULT 'running',

    started_by UUID NULL
        REFERENCES users(id),

    started_at TIMESTAMP NOT NULL DEFAULT NOW(),

    ended_at TIMESTAMP NULL
);

CREATE INDEX idx_process_instances_definition_id
ON process_instances(process_definition_id);

CREATE INDEX idx_process_instances_status
ON process_instances(status);

-- =========================================================
-- EXECUTIONS (TOKENS)
-- Core BPMN runtime execution state
--
-- Each execution represents a token moving
-- through the process graph.
--
-- Parallel gateways later create multiple executions.
-- =========================================================
CREATE TABLE executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    process_instance_id UUID NOT NULL
        REFERENCES process_instances(id)
        ON DELETE CASCADE,

    -- BPMN node currently being executed
    current_element_id TEXT NOT NULL,

    -- Supports future parallel gateways
    parent_execution_id UUID NULL
        REFERENCES executions(id),

    status execution_status NOT NULL DEFAULT 'active',

    is_active BOOLEAN NOT NULL DEFAULT true,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_executions_process_instance_id
ON executions(process_instance_id);

CREATE INDEX idx_executions_current_element_id
ON executions(current_element_id);

CREATE INDEX idx_executions_is_active
ON executions(is_active);

-- =========================================================
-- TASKS
-- Human workflow/tasklist layer
-- =========================================================
CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    process_instance_id UUID NOT NULL
        REFERENCES process_instances(id)
        ON DELETE CASCADE,

    execution_id UUID NULL
        REFERENCES executions(id)
        ON DELETE SET NULL,

    -- BPMN userTask id
    task_definition_key TEXT NOT NULL,

    -- BPMN task name
    task_name TEXT NULL,

    assignee TEXT NULL,

    candidate_group TEXT NULL,

    status task_status NOT NULL DEFAULT 'created',

    -- Dynamic form values submitted by users
    form_data JSONB NULL,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    claimed_at TIMESTAMP NULL,

    completed_at TIMESTAMP NULL
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
CREATE TABLE variables (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    process_instance_id UUID NOT NULL
        REFERENCES process_instances(id)
        ON DELETE CASCADE,

    data JSONB NOT NULL,

    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_variables_process_instance_id
ON variables(process_instance_id);

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
CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    process_instance_id UUID NOT NULL
        REFERENCES process_instances(id)
        ON DELETE CASCADE,

    execution_id UUID NULL
        REFERENCES executions(id)
        ON DELETE SET NULL,

    job_type TEXT NOT NULL,

    retries INTEGER NOT NULL DEFAULT 3,

    status TEXT NOT NULL DEFAULT 'pending',

    payload JSONB NULL,

    error_message TEXT NULL,

    locked_by TEXT NULL,

    locked_until TIMESTAMP NULL,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    completed_at TIMESTAMP NULL
);

CREATE INDEX idx_jobs_status
ON jobs(status);

CREATE INDEX idx_jobs_type
ON jobs(job_type);

-- =========================================================
-- TIMER JOBS
-- Used later for BPMN timers/events
-- =========================================================
CREATE TABLE timer_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    process_instance_id UUID NOT NULL
        REFERENCES process_instances(id)
        ON DELETE CASCADE,

    execution_id UUID NULL
        REFERENCES executions(id)
        ON DELETE SET NULL,

    event_type TEXT NOT NULL,

    due_at TIMESTAMP NOT NULL,

    payload JSONB NULL,

    is_triggered BOOLEAN NOT NULL DEFAULT false,

    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_timer_jobs_due_at
ON timer_jobs(due_at);

CREATE INDEX idx_timer_jobs_triggered
ON timer_jobs(is_triggered);

-- =========================================================
-- AUDIT LOGS
-- Enterprise-grade traceability
-- =========================================================
CREATE TABLE audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    process_instance_id UUID NULL
        REFERENCES process_instances(id)
        ON DELETE CASCADE,

    task_id UUID NULL
        REFERENCES tasks(id)
        ON DELETE CASCADE,

    user_id UUID NULL
        REFERENCES users(id),

    action TEXT NOT NULL,

    old_data JSONB NULL,

    new_data JSONB NULL,

    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_logs_process_instance_id
ON audit_logs(process_instance_id);

CREATE INDEX idx_audit_logs_task_id
ON audit_logs(task_id);

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