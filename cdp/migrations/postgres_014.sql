CREATE TABLE IF NOT EXISTS billing_collection_quality_history (
    check_id TEXT PRIMARY KEY,
    checked_at_utc TEXT NOT NULL,
    source_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'blocked')),
    failure_code TEXT,
    rule_id TEXT
);
CREATE INDEX IF NOT EXISTS billing_collection_quality_history_time
    ON billing_collection_quality_history (checked_at_utc, check_id);
