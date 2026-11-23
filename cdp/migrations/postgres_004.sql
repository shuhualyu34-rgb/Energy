CREATE TABLE IF NOT EXISTS billing_kafka_delivery (
    order_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    PRIMARY KEY (order_id, version, updated_at_utc),
    FOREIGN KEY (order_id, version, updated_at_utc)
        REFERENCES dwd_billing_order_v1_history (order_id, version, updated_at_utc)
);
