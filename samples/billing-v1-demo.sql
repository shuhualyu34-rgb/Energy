CREATE TABLE billing_v1_event (
    updated_at TEXT NOT NULL,
    order_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    PRIMARY KEY (updated_at, order_id, version)
);
INSERT INTO billing_v1_event VALUES (
    '2026-01-14T16:31:00.000000+00:00', 'ORD_20260115_SH001_0001', 1,
    '{"order_id":"ORD_20260115_SH001_0001","station_id":"ST_SH_001","gun_id":"GUN_SH001_01A","user_id_hmac":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","start_time":"2026-01-14T15:50:00Z","end_time":"2026-01-14T16:30:00Z","updated_at":"2026-01-14T16:31:00Z","status":"COMPLETED","version":1,"total_kwh":10,"total_amount_cents":1100,"elec_fee_cents":800,"service_fee_cents":300,"tou_slices":[{"tou_type":"FLAT","start_time":"2026-01-14T15:50:00Z","end_time":"2026-01-14T16:30:00Z","kwh":10,"elec_fee_cents":800,"service_fee_cents":300}]}'
);
INSERT INTO billing_v1_event VALUES (
    '2026-01-14T16:31:00.000000+00:00', 'ORD_20260115_SH001_0001', 2,
    (SELECT json_set(payload, '$.status', 'REFUNDED', '$.version', 2) FROM billing_v1_event WHERE version=1)
);
