-- Idempotent schema upgrade: index supporting bounded keyset pagination (created_at DESC, id DESC)
CREATE INDEX IF NOT EXISTS idx_monitors_created_at_id
    ON monitors (created_at DESC, id DESC);
