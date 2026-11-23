CREATE TABLE IF NOT EXISTS billing_stream_event (
    event_id TEXT PRIMARY KEY,
    payload_hash TEXT NOT NULL,
    station_id TEXT NOT NULL,
    status TEXT NOT NULL,
    window_start_utc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS billing_stream_window ON billing_stream_event
    (window_start_utc, station_id, status);
