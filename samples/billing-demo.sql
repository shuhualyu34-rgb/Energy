CREATE TABLE billing_order_snapshot (
    order_id TEXT NOT NULL PRIMARY KEY,
    updated_at TEXT NOT NULL,
    station_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    energy_wh INTEGER NOT NULL,
    electricity_fee_cents INTEGER NOT NULL,
    service_fee_cents INTEGER NOT NULL
);
CREATE INDEX billing_snapshot_cursor ON billing_order_snapshot (updated_at, order_id);
INSERT INTO billing_order_snapshot VALUES
    ('demo-order-001', '2026-01-14T16:31:00.000000+00:00', 'demo-station-001', 1,
     '2026-01-14T23:50:00+08:00', '2026-01-15T00:30:00+08:00', 10000, 800, 300);
