CREATE TABLE IF NOT EXISTS billing_stream_progress (
    stream TEXT PRIMARY KEY,
    max_event_time_utc TEXT NOT NULL
);
INSERT INTO billing_stream_progress (stream, max_event_time_utc)
SELECT 'billing-orders', MAX(window_start_utc)
FROM billing_stream_event
HAVING COUNT(*) > 0
ON CONFLICT (stream) DO NOTHING;

CREATE TABLE IF NOT EXISTS billing_stream_late (
    event_id TEXT PRIMARY KEY REFERENCES billing_stream_event(event_id),
    event_time_utc TEXT NOT NULL,
    watermark_utc TEXT NOT NULL,
    applied BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS billing_stream_quarantine (
    event_id TEXT PRIMARY KEY,
    payload_hash TEXT NOT NULL,
    event_time_utc TEXT NOT NULL,
    reason TEXT NOT NULL
);
