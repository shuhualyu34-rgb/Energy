#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python -E - "$@" <<'PY'
import argparse
import json
import multiprocessing
import os
import signal
import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.collect import collect
from cdp.collection_state import export_state
from cdp.reconcile import ReferenceDay, reconcile
from cdp.store import Store

parser = argparse.ArgumentParser(description="Rehearse collector recovery after SIGKILL.")
parser.add_argument("--large", action="store_true", help="interrupt after 50,000 committed orders")
args = parser.parse_args()

total, kill_after, page_size, timeout = (50_120, 50_000, 512, 600) if args.large else (120, 50, 16, 30)
order_id_width = len(str(total))
day = date(2026, 1, 15)
cutoff = datetime.fromisoformat("2026-01-14T16:31:00+00:00")
source_id = "crash-demo"


def interrupted_collection(source, directory):
    class InterruptedStore(Store):
        committed = 0

        def ingest(self, raw):
            outcome = super().ingest(raw)
            if outcome == "inserted":
                self.committed += 1
                if self.committed == kill_after:
                    os.kill(os.getpid(), signal.SIGKILL)
            return outcome

    collect(source, InterruptedStore(directory), source_id, cutoff, page_size)


with TemporaryDirectory() as directory:
    root = Path(directory)
    source = root / "billing.sqlite"
    with closing(sqlite3.connect(source)) as db, db:
        db.executescript(Path("samples/billing-demo.sql").read_text())
        db.execute("""
            WITH RECURSIVE numbers(n) AS (
                VALUES (2)
                UNION ALL SELECT n + 1 FROM numbers WHERE n < ?
            )
            INSERT INTO billing_order_snapshot
            SELECT printf('demo-order-%0*d', ?, n), updated_at, station_id, version,
                   started_at, ended_at, energy_wh, electricity_fee_cents, service_fee_cents
            FROM numbers CROSS JOIN billing_order_snapshot WHERE order_id = 'demo-order-001'
        """, (total, order_id_width))

    sample = json.loads(Path("samples/order.json").read_bytes())
    reference = ReferenceDay.model_validate({
        "business_day": day,
        "rows": [{
            "station_id": sample["station_id"], "order_count": total,
            "energy_wh": total * sample["energy_wh"],
            "electricity_fee_cents": total * sample["electricity_fee_cents"],
            "service_fee_cents": total * sample["service_fee_cents"],
        }],
    })

    store = Store(root / "platform")
    assert collect(source, store, source_id, cutoff - timedelta(minutes=1), page_size)["outcomes"] == {}
    checkpoint = export_state(store.directory)
    process = multiprocessing.get_context("fork").Process(
        target=interrupted_collection, args=(source, store.directory),
    )
    process.start()
    try:
        process.join(timeout=timeout)
        if process.is_alive():
            raise TimeoutError(f"Collector did not reach the interrupt point within {timeout} seconds")
        assert process.exitcode == -signal.SIGKILL, process.exitcode
    finally:
        if process.is_alive():
            process.kill()
            process.join()

    restarted = Store(store.directory)
    assert restarted.daily(day)[0]["order_count"] == kill_after
    assert export_state(restarted.directory) == checkpoint
    recovered = collect(source, restarted, source_id, cutoff, page_size)
    assert recovered["outcomes"] == {"duplicate": kill_after, "inserted": total - kill_after}, recovered
    assert export_state(restarted.directory).snapshot.checkpoints[0].through_utc == recovered["through_utc"]
    report = reconcile(restarted, reference)
    assert report["matched"], report
    expected_order_ids = {sample["order_id"]}
    expected_order_ids.update(f"demo-order-{number:0{order_id_width}d}" for number in range(2, total + 1))
    for raw in restarted.archived_orders():
        expected_order_ids.remove(json.loads(raw)["order_id"])
    assert not expected_order_ids, f"{len(expected_order_ids)} expected orders missing from the archive"
    print(f"Verified {total} archived orders and their checksums.")
    print(f"S3: SIGKILL after {kill_after} commits preserved the previous checkpoint.")
    print(f"Restart recovered {total - kill_after} orders, suppressed {kill_after} duplicates, and matched the reference totals.")
PY
