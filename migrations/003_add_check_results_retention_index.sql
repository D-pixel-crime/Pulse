-- Idempotent schema upgrade: index supporting retention scans (checked_at, id)
CREATE INDEX IF NOT EXISTS idx_check_results_checked_at_id
    ON check_results (checked_at, id);
