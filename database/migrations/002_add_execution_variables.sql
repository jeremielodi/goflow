-- Migration 002: Add execution_variables table for parallel gateway branch isolation
CREATE TABLE IF NOT EXISTS public.execution_variables (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id    UUID NOT NULL REFERENCES public.executions(id) ON DELETE CASCADE,
    variables       JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_execution_variables_exec ON public.execution_variables(execution_id);
