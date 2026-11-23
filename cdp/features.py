"""Produce a point-in-time station charging feature snapshot."""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from .billing import utc
from .config import configured_store
from .store import Store
from .weather import forecasts_as_of

LOOKBACK = timedelta(hours=24)


def station_features(store: Store, as_of: datetime, *, with_weather: bool = False) -> dict:
    if as_of.utcoffset() is None:
        raise ValueError("--as-of must include a timezone")
    as_of = as_of.astimezone(timezone.utc)
    earliest = datetime.min.replace(tzinfo=timezone.utc)
    if as_of < earliest + LOOKBACK:
        raise ValueError("--as-of is too early for the 24-hour lookback")
    if as_of > datetime.now(timezone.utc):
        raise ValueError("--as-of must be at or before the current time")
    start = as_of - LOOKBACK
    cutoff = as_of.isoformat(timespec="microseconds")
    store.initialize()
    bind = store.bind
    with store.connect(readonly=True) as connection:
        eligible = connection.execute(f"""
            WITH ranked AS (
                SELECT h.status, h.snapshot,
                       ROW_NUMBER() OVER (
                           PARTITION BY h.order_id ORDER BY h.version DESC, h.updated_at_utc DESC
                       ) AS position
                FROM dwd_billing_order_v1_history h
                JOIN billing_feature_availability a
                    ON (a.order_id, a.version, a.updated_at_utc) =
                       (h.order_id, h.version, h.updated_at_utc)
                WHERE a.available_at_utc <= {bind} AND h.updated_at_utc <= {bind}
            )
            SELECT snapshot FROM ranked WHERE position = 1 AND status = 'COMPLETED'
        """, (cutoff, cutoff))

        station_counts = defaultdict(int)
        station_energy = defaultdict(int)
        for row in eligible:
            snapshot = json.loads(row["snapshot"])
            if start <= utc(snapshot["end_time"]) < as_of:
                station = snapshot["station_id"]
                station_counts[station] += 1
                station_energy[station] += snapshot["energy_wh"]

    result = {
        "feature_version": "station-charging-v1",
        "as_of_utc": cutoff,
        "lookback_start_utc": start.isoformat(timespec="microseconds"),
        "rows": [
            {"station_id": station, "completed_order_count_24h": station_counts[station],
             "completed_energy_wh_24h": station_energy[station]}
            for station in sorted(station_counts)
        ],
    }

    if with_weather:
        next_hour = as_of.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        target = next_hour.isoformat(timespec="microseconds")
        forecasts = forecasts_as_of(store, cutoff, target)
        result["feature_version"] = "station-charging-weather-v1"
        result["forecast_target_utc"] = target
        for row in result["rows"]:
            row["weather"] = forecasts.get(row["station_id"])
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True, type=datetime.fromisoformat)
    parser.add_argument("--with-weather", action="store_true")
    args = parser.parse_args()
    try:
        snapshot = station_features(configured_store(), args.as_of, with_weather=args.with_weather)
    except ValueError as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(snapshot, indent=2))


if __name__ == "__main__":
    main()
