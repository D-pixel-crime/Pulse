-- Idempotent schema upgrade for existing local or hosted Pulse databases.
-- Adds temporary monitor claim/lease columns and supporting due-claim index
-- without dropping or resetting existing data.

ALTER TABLE monitors
    ADD COLUMN IF NOT EXISTS claim_token UUID NULL,
    ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_monitors_next_check_at
    ON monitors (next_check_at);

CREATE INDEX IF NOT EXISTS idx_monitors_due_claim
    ON monitors (next_check_at, claim_expires_at, id);
