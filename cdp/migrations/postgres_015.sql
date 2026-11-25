CREATE TABLE IF NOT EXISTS billing_collection_rejection (
    check_id TEXT PRIMARY KEY REFERENCES billing_collection_quality_history(check_id) ON DELETE CASCADE,
    order_ref TEXT NOT NULL,
    record_ref TEXT NOT NULL,
    declared_amount_cents BIGINT CHECK (declared_amount_cents >= 0)
);
