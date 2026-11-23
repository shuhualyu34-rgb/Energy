CREATE TABLE IF NOT EXISTS station_weather_forecast (
    station_id TEXT NOT NULL,
    issued_at_utc TEXT NOT NULL,
    valid_at_utc TEXT NOT NULL,
    available_at_utc TEXT,
    temperature_centi_c INTEGER NOT NULL,
    precipitation_micrometers INTEGER NOT NULL,
    PRIMARY KEY (station_id, valid_at_utc, issued_at_utc)
);
CREATE INDEX IF NOT EXISTS station_weather_forecast_target
    ON station_weather_forecast (valid_at_utc, station_id, issued_at_utc);
