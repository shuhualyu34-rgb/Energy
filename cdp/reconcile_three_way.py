"""Compare finance, operations and platform station totals from the same frozen reporting scope."""

import argparse
import hashlib
import json
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field, ValidationError, field_validator

from .reconcile import ReferenceDay
from .store import BUSINESS_ZONE

SOURCES = ("finance", "operations", "platform")
METRICS = ("order_count", "energy_wh", "electricity_fee_cents", "service_fee_cents")


class FrozenStationDay(ReferenceDay):
    source: Literal["finance", "operations", "platform"]
    timezone: Literal["Asia/Shanghai"]
    metric_version: str = Field(strict=True, pattern=r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$")
    frozen_at: AwareDatetime

    @field_validator("frozen_at", mode="before")
    @classmethod
    def parse_freeze_time(cls, value: object) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError("freeze time must be an ISO 8601 timestamp with a timezone")
        try:
            return value.astimezone(timezone.utc)
        except OverflowError:
            raise ValueError("freeze time is outside the supported range") from None


def compare_station_days(snapshots: dict[str, FrozenStationDay]) -> dict:
    if set(snapshots) != set(SOURCES):
        raise ValueError("finance, operations and platform snapshots are required")

    reference = snapshots["platform"]
    scope = (reference.business_day, reference.timezone, reference.metric_version, reference.frozen_at)
    rows_by_source = {}
    for source in SOURCES:
        snapshot = snapshots[source]
        if snapshot.source != source:
            raise ValueError(f"{source}: snapshot source does not match its argument")
        if (snapshot.business_day, snapshot.timezone, snapshot.metric_version, snapshot.frozen_at) != scope:
            raise ValueError(f"{source}: snapshot must match platform business day, timezone, metric version and freeze time")
        rows = {row.station_id: row for row in snapshot.rows}
        if len(rows) != len(snapshot.rows):
            raise ValueError(f"{source}: duplicate station_id")
        rows_by_source[source] = rows

    try:
        day_end = datetime.combine(reference.business_day + timedelta(days=1), time.min, tzinfo=BUSINESS_ZONE)
    except OverflowError:
        raise ValueError("business day is outside the supported range") from None
    if reference.frozen_at < day_end:
        raise ValueError("freeze time must be at or after the end of the Shanghai business day")

    station_ids = set().union(*rows_by_source.values())
    if not station_ids:
        raise ValueError("all three snapshots are empty")

    differences = []
    for station in sorted(station_ids):
        present = {source: rows[station] for source, rows in rows_by_source.items() if station in rows}
        missing = [source for source in SOURCES if source not in present]
        metrics = {}
        for metric in METRICS:
            values = {source: getattr(row, metric) for source, row in present.items()}
            if len(set(values.values())) > 1:
                metrics[metric] = values
        if missing or metrics:
            differences.append({"station_id": station, "missing_from": missing, "metrics": metrics})
    return {
        "business_day": reference.business_day.isoformat(),
        "timezone": reference.timezone,
        "metric_version": reference.metric_version,
        "frozen_at_utc": reference.frozen_at.isoformat(),
        "station_counts": {source: len(rows) for source, rows in rows_by_source.items()},
        "matched": not differences,
        "differences": differences,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for source in SOURCES:
        parser.add_argument(f"--{source}", type=Path, required=True)
    args = parser.parse_args()
    snapshots = {}
    digests = {}
    for source in SOURCES:
        try:
            raw = getattr(args, source).read_bytes()
            snapshots[source] = FrozenStationDay.model_validate_json(raw)
        except ValidationError:
            parser.exit(1, f"{source}: invalid frozen station snapshot\n")
        except OSError as error:
            parser.exit(1, f"{source}: {error}\n")
        digests[source] = hashlib.sha256(raw).hexdigest()
    try:
        report = compare_station_days(snapshots)
    except ValueError as error:
        parser.exit(1, f"{error}\n")
    report["snapshot_sha256"] = digests
    print(json.dumps(report, indent=2))
    return 0 if report["matched"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
