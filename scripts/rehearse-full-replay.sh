#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python -E - <<'PY'
import hashlib
import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

from cdp.cli import export_archive
from cdp.reconcile import ReferenceDay, reconcile
from cdp.store import Store

start = date(2024, 1, 1)
stop = date(2026, 1, 1)
shanghai = ZoneInfo("Asia/Shanghai")


def daily_fixture(day, offset):
    rows = []
    orders = []
    for station in (1, 2):
        totals = {
            "station_id": f"demo-station-{station:03d}",
            "order_count": 1,
            "energy_wh": 10000 + offset * station,
            "electricity_fee_cents": 800 + offset,
            "service_fee_cents": 300 + station,
        }
        end = datetime.combine(day, time(0, 30), tzinfo=shanghai)
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
    assert len(files) == 2 * len(references)
    snapshots = []
    for path in files:
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == path.stem, path.name
        snapshots.append(raw)

    expected = checksum(source, references)
    print(f"Source: {len(references)} days, {len(snapshots)} orders, SHA-256 {expected}")
    for run, batch in enumerate((snapshots, snapshots[::-1], snapshots[1:] + snapshots[:1]), start=1):
        replay = Store(root / f"replay-{run}")
        for raw in batch:
            assert replay.ingest(raw) == "inserted"
        for raw in batch:
            assert replay.ingest(raw) == "duplicate"
        actual = checksum(replay, references)
        assert actual == expected, (run, actual, expected)
        print(f"S7 replay {run}: {len(batch)} duplicates suppressed, SHA-256 {actual}")
PY
