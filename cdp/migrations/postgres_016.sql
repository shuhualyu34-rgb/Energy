CREATE TABLE IF NOT EXISTS billing_quality_audit_run (
    slot_utc TEXT PRIMARY KEY,
    checked_at_utc TEXT NOT NULL,
    results TEXT NOT NULL
);
