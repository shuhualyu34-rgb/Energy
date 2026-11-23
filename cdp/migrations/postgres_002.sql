CREATE TABLE dwd_billing_order_v1 (
    order_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL CHECK (version > 0),
    updated_at_utc TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('CHARGING','COMPLETED','ABNORMAL_CLOSED','REFUNDED')),
    snapshot TEXT NOT NULL
);
