-- 001_published_messages.sql — messages publiés en attente de corrélation.
--
-- Comme Camunda 8 : un message publié avec une durée de vie (TTL) reste
-- disponible jusqu'à expiration et se corrèle dès qu'un abonnement de même
-- nom et même clé de corrélation s'ouvre — au plus une fois par processus
-- (process_key, toutes versions confondues ; published_message_correlations).
--
--   psql -h <hôte> -U <user> -d <base> -v ON_ERROR_STOP=1 -1 -f 001_published_messages.sql

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

INSERT INTO public.schema_migrations (version, description)
VALUES ('001', 'published_messages : messages publiés en attente de corrélation (TTL)')
ON CONFLICT (version) DO NOTHING;
