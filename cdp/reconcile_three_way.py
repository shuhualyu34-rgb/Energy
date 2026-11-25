"""Compare frozen finance, operations and platform snapshots by station day or order month."""

import argparse
import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, field_validator

from .contract import Key, Quantity
from .reconcile import ReferenceDay
from .store import BUSINESS_ZONE

SOURCES = ("finance", "operations", "platform")
METRICS = ("order_count", "energy_wh", "electricity_fee_cents", "service_fee_cents")


class FrozenScope(BaseModel):
    model_config = ConfigDict(extra="forbid")

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


class FrozenStationDay(ReferenceDay, FrozenScope):
    pass


SignedCents = Annotated[int, Field(strict=True, ge=-(2**53 - 1), le=2**53 - 1)]


class FrozenOrder(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: Key
    station_id: Key
    business_day: date
    energy_wh: Quantity
    electricity_fee_cents: SignedCents
    service_fee_cents: SignedCents


class FrozenOrderMonth(FrozenScope):
    business_month: Annotated[str, Field(strict=True, pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$")]
    rows: list[FrozenOrder]


def validate_scope(snapshots: dict, period: str) -> dict:
    if set(snapshots) != set(SOURCES):
        raise ValueError("finance, operations and platform snapshots are required")
    reference = snapshots["platform"]
    for source in SOURCES:
        snapshot = snapshots[source]
        if snapshot.source != source:
            raise ValueError(f"{source}: snapshot source does not match its argument")
        for field in (period, "timezone", "metric_version", "frozen_at"):
            if getattr(snapshot, field) != getattr(reference, field):
                raise ValueError(f"{source}: {field} must match platform")
    return {
        "timezone": reference.timezone,
        "metric_version": reference.metric_version,
        "frozen_at_utc": reference.frozen_at.isoformat(),
    }


def index_rows(snapshots: dict, key: str) -> dict:
    indexed = {}
    for source in SOURCES:
        rows = snapshots[source].rows
        indexed[source] = {getattr(row, key): row.model_dump(mode="json") for row in rows}
        if len(indexed[source]) != len(rows):
            raise ValueError(f"{source}: duplicate {key}")
    if not any(indexed.values()):
        raise ValueError("all three snapshots are empty")
    return indexed


def compare_rows(
    indexed: dict, key: str, fields: tuple[str, ...], *, include_missing_values: bool = False,
) -> list[dict]:
    differences = []
    for identifier in sorted(set().union(*indexed.values())):
        present = {source: rows[identifier] for source, rows in indexed.items() if identifier in rows}
        missing = [source for source in SOURCES if source not in present]
        metrics = {}
        for field in fields:
            values = {source: row[field] for source, row in present.items()}
            if len(set(values.values())) > 1 or (missing and include_missing_values):
                metrics[field] = values
        if missing or metrics:
            differences.append({key: identifier, "missing_from": missing, "metrics": metrics})
    return differences


def compare_station_days(snapshots: dict[str, FrozenStationDay]) -> dict:
    scope = validate_scope(snapshots, "business_day")
    reference = snapshots["platform"]
    try:
        day_end = datetime.combine(reference.business_day + timedelta(days=1), time.min, tzinfo=BUSINESS_ZONE)
    except OverflowError:
        raise ValueError("business day is outside the supported range") from None
    if reference.frozen_at < day_end:
        raise ValueError("freeze time must be at or after the end of the Shanghai business day")
    stations = index_rows(snapshots, "station_id")
    differences = compare_rows(stations, "station_id", METRICS)
    return {
        "business_day": reference.business_day.isoformat(),
        **scope,
        "station_counts": {source: len(rows) for source, rows in stations.items()},
        "matched": not differences,
        "differences": differences,
    }


def compare_order_months(snapshots: dict[str, FrozenOrderMonth]) -> dict:
    scope = validate_scope(snapshots, "business_month")
    reference = snapshots["platform"]
    month = reference.business_month
    try:
        start = date.fromisoformat(f"{month}-01")
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
    except (ValueError, OverflowError):
        raise ValueError("business month is outside the supported range") from None
    if reference.frozen_at < datetime.combine(end, time.min, tzinfo=BUSINESS_ZONE):
        raise ValueError("freeze time must be at or after the end of the Shanghai business month")
    orders = index_rows(snapshots, "order_id")
    first_day, next_month = start.isoformat(), end.isoformat()
    stations: dict[str, dict[str, dict[str, int]]] = {}
    for source, source_orders in orders.items():
        totals: dict[str, dict[str, int]] = {}
        for row in source_orders.values():
            if not first_day <= row["business_day"] < next_month:
                raise ValueError(f"{source}: order business_day is outside the frozen month")
            station = totals.setdefault(row["station_id"], dict.fromkeys(METRICS, 0))
            station["order_count"] += 1
            for metric in METRICS[1:]:
                station[metric] += row[metric]
        stations[source] = totals

    station_differences = compare_rows(stations, "station_id", METRICS, include_missing_values=True)
    order_differences = compare_rows(
        orders, "order_id", ("station_id", "business_day", *METRICS[1:]), include_missing_values=True,
    )
    return {
        "business_month": month,
        **scope,
        "station_counts": {source: len(rows) for source, rows in stations.items()},
        "order_counts": {source: len(rows) for source, rows in orders.items()},
        "matched": not station_differences and not order_differences,
        "station_differences": station_differences,
        "order_differences": order_differences,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for source in SOURCES:
        parser.add_argument(f"--{source}", type=Path, required=True)
    parser.add_argument("--monthly", action="store_true", help="Compare frozen monthly order rows")
    args = parser.parse_args()
    snapshot_model = FrozenOrderMonth if args.monthly else FrozenStationDay
    snapshots = {}
    digests = {}
    for source in SOURCES:
        try:
            raw = getattr(args, source).read_bytes()
            snapshots[source] = snapshot_model.model_validate_json(raw)
        except ValidationError:
            parser.exit(1, f"{source}: invalid frozen snapshot\n")
        except OSError as error:
            parser.exit(1, f"{source}: {error}\n")
        digests[source] = hashlib.sha256(raw).hexdigest()
    try:
        report = compare_order_months(snapshots) if args.monthly else compare_station_days(snapshots)
    except ValueError as error:
        parser.exit(1, f"{error}\n")
    report["snapshot_sha256"] = digests
    print(json.dumps(report, indent=2))
    return 0 if report["matched"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
