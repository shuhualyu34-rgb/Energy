#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python -E - <<'PY'
import json
import sqlite3
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

from cdp.billing_event import BILLING_ARCHIVE
from cdp.billing_ledger import billing_history, verify_archive
from cdp.collect import BILLING_V1, collect
from cdp.collection_quality import CollectionRejected
from cdp.collection_state import export_state
from cdp.store import Store

shanghai = ZoneInfo("Asia/Shanghai")
start = datetime(2026, 1, 15, 21, 50, tzinfo=shanghai).astimezone(timezone.utc)
boundary = start + timedelta(minutes=10)
end = boundary + timedelta(minutes=30)
first_cutoff = end + timedelta(minutes=1)
second_cutoff = first_cutoff + timedelta(minutes=1)
order_id = "ORD_20260115_SH001_9001"

# The synthetic meter provides segment readings. Energy is not prorated by duration.
slices = [
    {
        "tou_type": period, "start_time": begin.isoformat(), "end_time": finish.isoformat(),
        "kwh": kwh, "elec_rate_cents": electric_rate, "service_rate_cents": service_rate,
        "elec_fee_cents": kwh * electric_rate, "service_fee_cents": kwh * service_rate,
    }
    for period, begin, finish, kwh, electric_rate, service_rate in (
        ("PEAK", start, boundary, 80, 135, 45),
        ("VALLEY", boundary, end, 150, 60, 30),
    )
]
electric_fee = sum(part["elec_fee_cents"] for part in slices)
service_fee = sum(part["service_fee_cents"] for part in slices)
order = {
    "order_id": order_id, "station_id": "ST_SH_001", "gun_id": "GUN_SH001_01A",
    "user_id_hmac": "a" * 64, "start_time": start.isoformat(), "end_time": end.isoformat(),
    "updated_at": first_cutoff.isoformat(), "status": "COMPLETED", "version": 1,
    "total_kwh": sum(part["kwh"] for part in slices),
    "elec_fee_cents": electric_fee, "service_fee_cents": service_fee,
    "total_amount_cents": electric_fee + service_fee, "tou_slices": slices,
}
invalid = deepcopy(order)
invalid.update(version=2, updated_at=second_cutoff.isoformat())
invalid["tou_slices"][1]["elec_fee_cents"] += 1


def insert_source(source, payload):
    source.execute("INSERT INTO billing_v1_event VALUES (?, ?, ?, ?)", (
        datetime.fromisoformat(payload["updated_at"]).isoformat(timespec="microseconds"),
        payload["order_id"], payload["version"], json.dumps(payload),
    ))


with TemporaryDirectory() as directory:
    root = Path(directory)
    source_path = root / "billing.sqlite"
    with closing(sqlite3.connect(source_path)) as source, source:
        source.executescript(Path("samples/billing-v1-demo.sql").read_text())
        source.execute("DELETE FROM billing_v1_event")
        insert_source(source, order)

    store = Store(root / "platform")
    options = {"source_format": BILLING_V1, "page_size": 1}
    report = collect(source_path, store, "tou-rehearsal", first_cutoff, **options)
    assert report["outcomes"] == {"inserted": 1}, report
    checkpoint = export_state(store.directory)
    history = billing_history(store, order_id)
    accepted, = history
    assert accepted["version"] == 1 and accepted["status"] == "COMPLETED", accepted
    snapshot = accepted["snapshot"]
    assert (snapshot["energy_wh"], snapshot["elec_fee_cents"],
            snapshot["service_fee_cents"], snapshot["total_amount_cents"]) == (230000, 19800, 8100, 27900), snapshot
    stored_slices = accepted["tou_slices"]
    assert [(part["tou_type"], part["energy_wh"], part["elec_fee_cents"], part["service_fee_cents"])
            for part in stored_slices] == [("PEAK", 80000, 10800, 3600), ("VALLEY", 150000, 9000, 4500)], stored_slices
    assert [part["start_time"] for part in stored_slices] == [start.isoformat(), boundary.isoformat()]
    assert [part["end_time"] for part in stored_slices] == [boundary.isoformat(), end.isoformat()]
    archive = verify_archive(store, order_id)
    assert [item["result"] for item in archive["versions"]] == ["verified"], archive
    archived = json.loads(store.read_archive(accepted["archive_sha256"], namespace=BILLING_ARCHIVE))
    assert archived["order"] == snapshot and archived["tou_slices"] == stored_slices, archived

    with closing(sqlite3.connect(source_path)) as source, source:
        insert_source(source, invalid)
    try:
        collect(source_path, Store(store.directory), "tou-rehearsal", second_cutoff, **options)
    except CollectionRejected as error:
        assert (error.code, error.rule_id) == ("contract_validation", "billing_tou_totals")
    else:
        raise AssertionError("Expected the one-cent TOU mismatch to block collection")
    assert export_state(store.directory) == checkpoint
    assert billing_history(store, order_id) == history
    assert verify_archive(store, order_id) == archive

    with closing(sqlite3.connect(source_path)) as source, source:
        source.execute("DELETE FROM billing_v1_event WHERE version = 2")
    replay = collect(source_path, Store(store.directory), "tou-rehearsal", second_cutoff, **options)
    assert replay["outcomes"] == {"duplicate": 1}, replay
    assert billing_history(store, order_id) == history
    print("S9: 230,000 Wh across peak and valley; 19,800 cents electricity and 8,100 cents service verified.")
    print("Archive matched the ledger; a one-cent mismatch blocked collection; replay did not create a second version.")
PY
