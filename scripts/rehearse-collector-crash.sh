#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python -E - <<'PY'
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

total = 120
kill_after = 50
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

    collect(source, InterruptedStore(directory), source_id, cutoff, 16)


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
            SELECT printf('demo-order-%03d', n), updated_at, station_id, version,
                   started_at, ended_at, energy_wh, electricity_fee_cents, service_fee_cents
            FROM numbers CROSS JOIN billing_order_snapshot WHERE order_id = 'demo-order-001'
        """, (total,))

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
    assert collect(source, store, source_id, cutoff - timedelta(minutes=1), 16)["outcomes"] == {}
    checkpoint = export_state(store.directory)
    process = multiprocessing.get_context("fork").Process(
        target=interrupted_collection, args=(source, store.directory),
    )
    process.start()
    try:
        process.join(timeout=30)
        assert process.exitcode == -signal.SIGKILL, process.exitcode
    finally:
        if process.is_alive():
            process.kill()
            process.join()

    restarted = Store(store.directory)
    assert restarted.daily(day)[0]["order_count"] == kill_after
    assert export_state(restarted.directory) == checkpoint
    recovered = collect(source, restarted, source_id, cutoff, 16)
    assert recovered["outcomes"] == {"duplicate": kill_after, "inserted": total - kill_after}, recovered
    assert export_state(restarted.directory).snapshot.checkpoints[0].through_utc == recovered["through_utc"]
    report = reconcile(restarted, reference)
    assert report["matched"], report
    assert len(list(restarted.archived_orders())) == total
    print(f"S3: SIGKILL after {kill_after} commits preserved the previous checkpoint.")
    print(f"Restart recovered {total - kill_after} orders, suppressed {kill_after} duplicates, and matched the reference totals.")
PY
