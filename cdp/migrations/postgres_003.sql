CREATE TABLE IF NOT EXISTS dwd_billing_order_v1_history (
    order_id TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    updated_at_utc TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('CHARGING','COMPLETED','ABNORMAL_CLOSED','REFUNDED')),
    snapshot TEXT NOT NULL,
    tou_slices TEXT,
    PRIMARY KEY (order_id, version, updated_at_utc)
);
INSERT INTO dwd_billing_order_v1_history (order_id, version, updated_at_utc, status, snapshot)
SELECT order_id, version, updated_at_utc, status, snapshot
FROM dwd_billing_order_v1
WHERE true
ON CONFLICT (order_id, version, updated_at_utc) DO NOTHING;
