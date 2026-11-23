CREATE TABLE dwd_order (
    order_id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    started_at_utc TIMESTAMPTZ NOT NULL,
    ended_at_utc TIMESTAMPTZ NOT NULL CHECK (ended_at_utc > started_at_utc),
    business_day DATE NOT NULL,
    energy_wh BIGINT NOT NULL CHECK (energy_wh >= 0),
    electricity_fee_cents BIGINT NOT NULL CHECK (electricity_fee_cents >= 0),
    service_fee_cents BIGINT NOT NULL CHECK (service_fee_cents >= 0),
    snapshot_hash TEXT NOT NULL,
    raw_hash TEXT NOT NULL
);
CREATE INDEX dwd_order_day_station ON dwd_order (business_day, station_id);
CREATE VIEW ads_station_daily AS
SELECT station_id, business_day, COUNT(*) AS order_count,
       SUM(energy_wh) AS energy_wh,
       SUM(electricity_fee_cents) AS electricity_fee_cents,
       SUM(service_fee_cents) AS service_fee_cents
FROM dwd_order
GROUP BY station_id, business_day;
