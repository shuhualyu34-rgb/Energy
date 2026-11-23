import argparse
import json
from datetime import date
from pathlib import Path

from pydantic import BaseModel, ConfigDict, ValidationError

from .config import configured_store
from .contract import Key, Quantity
from .store import Store


class StationTotals(BaseModel):
    model_config = ConfigDict(extra="forbid")

    station_id: Key
    order_count: Quantity
    energy_wh: Quantity
    electricity_fee_cents: Quantity
    service_fee_cents: Quantity


class ReferenceDay(BaseModel):
    model_config = ConfigDict(extra="forbid")

    business_day: date
    rows: list[StationTotals]


def reconcile(store: Store, reference: ReferenceDay) -> dict:
    expected = {row.station_id: row.model_dump() for row in reference.rows}
    if len(expected) != len(reference.rows):
        raise ValueError("duplicate station_id in reference")
    actual = {}
    for row in store.daily(reference.business_day):
        row.pop("business_day")
        actual[row["station_id"]] = row
    differences = []
    for station_id in sorted(expected.keys() | actual.keys()):
        source = expected.get(station_id)
        platform = actual.get(station_id)
        if source != platform:
            differences.append({"station_id": station_id, "source": source, "platform": platform})
    return {
        "business_day": reference.business_day.isoformat(),
        "source_stations": len(expected),
        "platform_stations": len(actual),
        "matched": not differences,
        "differences": differences,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare a full day's station totals against a reference.")
    parser.add_argument("reference", type=Path)
    args = parser.parse_args()
    try:
        reference = ReferenceDay.model_validate_json(args.reference.read_bytes())
        report = reconcile(configured_store(), reference)
    except ValidationError:
        parser.exit(1, "invalid reconciliation reference\n")
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(report, indent=2))
    return 0 if report["matched"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
