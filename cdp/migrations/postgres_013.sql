CREATE TABLE IF NOT EXISTS billing_collection_quality_rule (
    source_key TEXT PRIMARY KEY REFERENCES billing_collection_quality(source_key) ON DELETE CASCADE,
    rule_id TEXT NOT NULL
);
