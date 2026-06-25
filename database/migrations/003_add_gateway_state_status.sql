-- Migration 003: Add status column to gateway_state table
ALTER TABLE public.gateway_state
    ADD COLUMN IF NOT EXISTS status VARCHAR(50) NOT NULL DEFAULT 'waiting';
