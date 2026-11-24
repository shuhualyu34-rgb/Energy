#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python -E - "$@" <<'PY'
import argparse
import hashlib
import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

from cdp.cli import export_archive
from cdp.reconcile import ReferenceDay, reconcile
from cdp.store import Store

parser = argparse.ArgumentParser(description="Rehearse 24 months of synthetic order replay.")
parser.add_argument("--large", action="store_true", help="generate one order per day for 120 stations")
args = parser.parse_args()

station_count = 120 if args.large else 2
start = date(2024, 1, 1)
stop = date(2026, 1, 1)
shanghai = ZoneInfo("Asia/Shanghai")


def daily_fixture(day, offset):
    rows = []
    orders = []
    end = datetime.combine(day, time(0, 30), tzinfo=shanghai)
    for station in range(1, station_count + 1):
        totals = {
            "station_id": f"demo-station-{station:03d}",
            "order_count": 1,
            "energy_wh": 10000 + offset * station,
            "electricity_fee_cents": 800 + offset,
            "service_fee_cents": 300 + station,
        }
        order = {key: value for key, value in totals.items() if key != "order_count"}
        order.update(
            schema_version="0.1",
            order_id=f"replay-{day.isoformat()}-{station}",
            version=1,
            started_at=(end - timedelta(minutes=40)).isoformat(),
            ended_at=end.isoformat(),
        )
        rows.append(totals)
        orders.append(json.dumps(order, sort_keys=True).encode())
    return ReferenceDay(business_day=day, rows=rows), orders


def checksum(store, references):
    digest = hashlib.sha256()
    for reference in references:
        report = reconcile(store, reference)
        assert report["matched"], report
        report["rows"] = store.daily(reference.business_day)
        digest.update(json.dumps(report, sort_keys=True).encode() + b"\n")
    return digest.hexdigest()


with TemporaryDirectory() as directory:
    root = Path(directory)
    source = Store(root / "source")
    references = []
    for offset in range((stop - start).days):
        reference, orders = daily_fixture(start + timedelta(days=offset), offset)
        references.append(reference)
        for raw in orders:
            assert source.ingest(raw) == "inserted"

    archive = root / "archive"
    export_archive(source, archive)
    files = sorted(archive.glob("*.json"))
    assert len(files) == station_count * (stop - start).days, len(files)
    snapshots = []
    for path in files:
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == path.stem, path.name
        snapshots.append(raw)

    expected = checksum(source, references)
    print(f"Source: {len(references)} days, {station_count} stations, "
          f"{len(snapshots)} orders, SHA-256 {expected}", flush=True)
    for run, batch in enumerate((snapshots, snapshots[::-1], snapshots[1:] + snapshots[:1]), start=1):
        with TemporaryDirectory(dir=root, prefix=f"replay-{run}-") as destination:
            replay = Store(Path(destination))
            for raw in batch:
                assert replay.ingest(raw) == "inserted"
            before_retry = checksum(replay, references)
            assert before_retry == expected, (run, before_retry, expected)
            for raw in batch:
                assert replay.ingest(raw) == "duplicate"
            after_retry = checksum(replay, references)
            assert after_retry == expected, (run, after_retry, expected)
            print(f"S7 replay {run}: {len(batch)} duplicates suppressed, SHA-256 {after_retry}", flush=True)
PY
