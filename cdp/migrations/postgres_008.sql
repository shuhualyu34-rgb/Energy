CREATE TABLE IF NOT EXISTS billing_collection_quality (
    source_key TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('ok', 'blocked')),
    checked_at_utc TEXT NOT NULL,
    failure_code TEXT
);
