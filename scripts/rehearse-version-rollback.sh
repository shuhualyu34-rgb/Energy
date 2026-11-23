#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python - <<'PY'
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.billing_ledger import billing_history, verify_archive
from cdp.collect import BILLING_V1, backfill, collect
from cdp.store import Store

order_id = "ORD_20260115_SH001_0001"
accepted_at = datetime.fromisoformat("2026-01-14T16:31:00+00:00")
replay_cutoff = accepted_at + timedelta(hours=2)


def current_order(store):
    with store.connect(readonly=True) as db:
        row = db.execute(
            "SELECT version, status, snapshot FROM dwd_billing_order_v1 WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        return dict(row)


with TemporaryDirectory() as directory:
    root = Path(directory)
    source = root / "billing.sqlite"
    with closing(sqlite3.connect(source)) as db:
        db.executescript(Path("samples/billing-v1-demo.sql").read_text())

    store = Store(root / "platform")
    result = collect(source, store, "rollback-demo", accepted_at, 1, source_format=BILLING_V1)
    assert result["outcomes"] == {"inserted": 1, "updated": 1}, result
    current = current_order(store)
    assert current["version"] == 2 and current["status"] == "REFUNDED", current
    history = billing_history(store, order_id)
    assert [(row["version"], row["status"]) for row in history] == [(1, "COMPLETED"), (2, "REFUNDED")]
    archive = verify_archive(store, order_id)
    assert [row["result"] for row in archive["versions"]] == ["verified", "verified"], archive

    restarted = Store(store.directory)
    replay = backfill(source, restarted, accepted_at, replay_cutoff, 1, source_format=BILLING_V1)
    assert replay["outcomes"] == {"stale": 1, "duplicate": 1}, replay
    assert current_order(restarted) == current
    assert billing_history(restarted, order_id) == history
    assert verify_archive(restarted, order_id) == archive
    print("S5: replay at the two-hour cutoff counted one stale version and one duplicate.")
    print("Current refund, both history rows and verified archive snapshots match the pre-replay state.")
PY
