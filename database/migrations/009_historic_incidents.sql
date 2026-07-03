-- migrations/009_historic_incidents.sql
--
-- incidents.process_instance_id has ON DELETE CASCADE, and
-- ArchiveAndDeleteProcessInstance (internal/service/archive_service.go)
-- hard-deletes the live process_instances row once an instance finishes —
-- silently destroying every incident (including resolved ones) the moment
-- its instance archives, which happens promptly on completion/termination.
-- This blocks any accurate "incident rate by process key" reporting.
--
-- Fix: mirror historic_process_instances/historic_tasks — copy incidents
-- into a dedicated historic table (denormalizing process_definition_key,
-- same reasoning as historic_process_instances) before the live row (and
-- its cascade) is deleted. Archived in whatever state they're in
-- (including 'open' — a force-terminated instance can have an unresolved
-- incident, and that shouldn't vanish either).

CREATE TABLE IF NOT EXISTS historic_incidents (
    id UUID PRIMARY KEY,                    -- same id as the original incidents row
    process_instance_id UUID NOT NULL
        REFERENCES historic_process_instances(id)
        ON DELETE CASCADE,
    process_definition_key TEXT NOT NULL,   -- denormalized, survives instance/definition deletion
    job_id UUID NULL,
    incident_type TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    error_message TEXT NULL,
    error_code TEXT NULL,
    state TEXT NOT NULL,                    -- whatever state it was in at archive time
    created_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP NULL,
    archived_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_historic_incidents_process_definition_key
ON historic_incidents(process_definition_key);

CREATE INDEX IF NOT EXISTS idx_historic_incidents_process_instance_id
ON historic_incidents(process_instance_id);
