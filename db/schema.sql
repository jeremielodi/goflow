
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

-- =========================================================
-- USERS TABLE
-- Represents system users who can start processes,
-- complete tasks, and be assigned work
-- =========================================================
CREATE TABLE users (
    id UUID PRIMARY KEY,

    email TEXT UNIQUE NOT NULL,

    full_name TEXT,

    password_hash TEXT NOT NULL, -- store hashed password only

    is_active BOOLEAN NOT NULL DEFAULT true,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =========================================================
-- PROCESS DEFINITIONS (optional but recommended baseline)
-- Stores deployed BPMN definitions
-- =========================================================
CREATE TABLE process_instances (
    id UUID PRIMARY KEY,

    definition_id UUID NOT NULL, -- BPMN process definition reference

    status process_instance_status NOT NULL DEFAULT 'running',

    started_by UUID NULL REFERENCES users(id),

    started_at TIMESTAMP NOT NULL DEFAULT NOW(),

    ended_at TIMESTAMP NULL
);

-- =========================================================
-- EXECUTIONS (TOKENS)
-- Core BPMN runtime engine state
-- Each row = one active path in the process graph
-- =========================================================
CREATE TABLE executions (
    id UUID PRIMARY KEY,

    process_instance_id UUID NOT NULL,

    current_element_id TEXT NOT NULL, -- BPMN node ID

    parent_execution_id UUID NULL, -- supports gateways & future parallelism

    status execution_status NOT NULL DEFAULT 'active',

    is_active BOOLEAN NOT NULL DEFAULT true,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =========================================================
-- USER TASKS
-- Human interaction layer (tasklist system)
-- =========================================================
CREATE TABLE tasks (
    id UUID PRIMARY KEY,

    process_instance_id UUID NOT NULL,

    task_definition_key TEXT NOT NULL, -- BPMN userTask ID

    assignee UUID NULL REFERENCES users(id),

    candidate_group TEXT NULL,

    status task_status NOT NULL DEFAULT 'created',

    form_data JSONB NULL, -- dynamic form payload

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    completed_at TIMESTAMP NULL
);

-- =========================================================
-- VARIABLES
-- Process-wide state (JSON document)
-- =========================================================
CREATE TABLE variables (
    id UUID PRIMARY KEY,

    process_instance_id UUID NOT NULL,

    data JSONB NOT NULL, -- full variable map

    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);