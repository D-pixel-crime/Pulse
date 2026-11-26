-- PostgreSQL Initial Schema for Pulse
-- Idempotent execution: safe to apply during database initialization

CREATE TABLE IF NOT EXISTS monitors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds >= 30 AND interval_seconds <= 86400),
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds >= 1 AND timeout_seconds <= 30),
    current_status VARCHAR(20) NOT NULL DEFAULT 'UNKNOWN' CHECK (current_status IN ('UNKNOWN', 'UP', 'DOWN')),
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    last_checked_at TIMESTAMPTZ NULL,
    last_success_at TIMESTAMPTZ NULL,
    next_check_at TIMESTAMPTZ NOT NULL,
    claim_token UUID NULL,
    claim_expires_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS check_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    monitor_id UUID NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success BOOLEAN NOT NULL,
    http_status INTEGER NULL,
    latency_ms BIGINT NOT NULL,
    error_message VARCHAR(1024) NULL
);

-- Index for claimable due-monitor lookup and deterministic ordering
CREATE INDEX IF NOT EXISTS idx_monitors_due_claim
ON monitors (next_check_at, claim_expires_at, id);

-- Index for bounded cursor pagination ordered newest first
CREATE INDEX IF NOT EXISTS idx_monitors_created_at_id
ON monitors (created_at DESC, id DESC);

-- Index for recent history lookup and stats aggregation by monitor
CREATE INDEX IF NOT EXISTS idx_check_results_monitor_checked_at 
ON check_results (monitor_id, checked_at DESC);

-- Index for retention scans and batch cleanup
CREATE INDEX IF NOT EXISTS idx_check_results_checked_at_id
ON check_results (checked_at, id);
