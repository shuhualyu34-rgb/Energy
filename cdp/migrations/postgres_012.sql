CREATE TABLE IF NOT EXISTS billing_kafka_recovery_delivery (
    cluster_id TEXT NOT NULL,
    recovery_topic TEXT NOT NULL,
    order_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    PRIMARY KEY (cluster_id, recovery_topic, order_id, version, updated_at_utc),
    FOREIGN KEY (order_id, version, updated_at_utc)
        REFERENCES billing_order_archive (order_id, version, updated_at_utc)
);
