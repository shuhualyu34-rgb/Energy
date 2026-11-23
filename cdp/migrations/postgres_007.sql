CREATE TABLE IF NOT EXISTS billing_spark_sink_progress (
    checkpoint_key TEXT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    completed_at_utc TEXT NOT NULL,
    outcomes TEXT NOT NULL
);
