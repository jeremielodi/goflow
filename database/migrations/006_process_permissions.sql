-- Migration 006: Per-resource process permissions
--
-- Adds a resource-level ACL for process definitions, keyed by
-- process_key (covers every version deployed under that key). A key with
-- zero rows here is unrestricted — open to any authenticated user, same
-- as today. See internal/service/process_authorization_service.go.
--
-- resource_type is a plain TEXT column (not an enum) so a future
-- 'deployment' resource type can be added without another migration;
-- only 'process_definition' is enforced by the application today.
--
-- All statements are idempotent (IF NOT EXISTS) so this is safe to run
-- more than once or against a database that already has some of these.

CREATE TABLE IF NOT EXISTS public.process_permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_type TEXT NOT NULL DEFAULT 'process_definition',
    process_key TEXT NOT NULL,
    grantee_type TEXT NOT NULL CHECK (grantee_type IN ('user', 'role')),
    grantee_id UUID NOT NULL,
    permission TEXT NOT NULL CHECK (permission IN ('VIEW', 'START', 'MANAGE')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (process_key, grantee_type, grantee_id, permission)
);

CREATE INDEX IF NOT EXISTS idx_process_permissions_key
ON process_permissions(process_key);

CREATE INDEX IF NOT EXISTS idx_process_permissions_grantee
ON process_permissions(grantee_type, grantee_id);
