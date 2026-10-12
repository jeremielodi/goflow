-- Migration 007: Zeebe gRPC gateway key mapping
--
-- Maps GoFlow's internal UUIDs to the int64 "keys" Zeebe's gRPC gateway
-- protocol requires (processDefinitionKey/processInstanceKey/job key are
-- all int64 on the wire) — see internal/zeebegrpc. Keys are assigned
-- lazily the first time a resource is exposed over gRPC, and must persist
-- across restarts since a client holds a job key between ActivateJobs and
-- a later CompleteJob/FailJob call.
--
-- Idempotent (IF NOT EXISTS) — safe to run more than once or against a
-- database that already has this table.

CREATE TABLE IF NOT EXISTS public.zeebe_keys (
    key BIGSERIAL PRIMARY KEY,
    resource_type TEXT NOT NULL,
    resource_id UUID NOT NULL,
    UNIQUE (resource_type, resource_id)
);
