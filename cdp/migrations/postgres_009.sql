CREATE TABLE IF NOT EXISTS billing_feature_availability (
    order_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    available_at_utc TEXT NOT NULL,
    PRIMARY KEY (order_id, version, updated_at_utc),
    FOREIGN KEY (order_id, version, updated_at_utc)
        REFERENCES dwd_billing_order_v1_history (order_id, version, updated_at_utc)
);
CREATE INDEX IF NOT EXISTS billing_feature_availability_time
    ON billing_feature_availability (available_at_utc);
