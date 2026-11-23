#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python -E - <<'PY'
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.billing_event import event_id
from cdp.store import Store
from cdp.stream import Observation, apply_late_events, billing_windows, observe

day = date(2026, 1, 15)
station = "ST_SH_001"


def delivery(number, end, *, version=1, status="COMPLETED"):
    order_id = f"ORD_20260115_SH001_{number:04d}"
    updated = f"2026-01-15T12:02:00.{version:06d}+00:00"
    payload = {
        "schema_version": "1.0",
        "event_id": event_id(order_id, version, updated),
        "order_id": order_id,
        "version": version,
        "updated_at": updated,
        "status": status,
        "order": {"station_id": station, "end_time": end},
    }
    return order_id.encode(), json.dumps(payload).encode()


def counts(store):
    return {
        (row["window_start_utc"], row["status"]): row["event_count"]
        for row in billing_windows(store, day)
    }


with TemporaryDirectory() as directory:
    store = Store(Path(directory))
    original = delivery(1, "2026-01-15T12:02:00Z")
    assert observe(store, *original) == Observation.APPLIED
    restarted = Store(Path(directory))
    for _ in range(100):
        assert observe(restarted, *original) == Observation.DUPLICATE
    expected = {("2026-01-15T12:00:00+00:00", "COMPLETED"): 1}
    assert counts(restarted) == expected
    print("S1: 100 redeliveries after reopening the store left the window count at one.")

    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    quarantined = delivery(2, future)
    assert observe(restarted, *quarantined) == Observation.QUARANTINED
    assert observe(restarted, *quarantined) == Observation.DUPLICATE
    assert counts(restarted) == expected

    at_watermark = delivery(3, "2026-01-15T10:02:00Z")
    assert observe(restarted, *at_watermark) == Observation.APPLIED
    expected[("2026-01-15T10:00:00+00:00", "COMPLETED")] = 1

    refund = delivery(1, "2026-01-15T12:02:00Z", version=2, status="REFUNDED")
    assert observe(restarted, *refund) == Observation.APPLIED
    assert observe(restarted, *refund) == Observation.DUPLICATE
    expected[("2026-01-15T12:00:00+00:00", "REFUNDED")] = 1

    late = delivery(4, "2026-01-15T09:02:00Z")
    assert observe(restarted, *late) == Observation.LATE
    assert counts(restarted) == expected
    assert apply_late_events(restarted, day) == 1
    assert apply_late_events(restarted, day) == 0
    assert observe(restarted, *late) == Observation.DUPLICATE
    expected[("2026-01-15T09:00:00+00:00", "COMPLETED")] = 1
    assert counts(restarted) == expected
    print("S2: two-hour boundary applied, future event quarantined, three-hour event backfilled once.")
    print("Refund version appears once in its own lifecycle status.")

    clock_store = Store(Path(directory) / "clock-drift")
    now = datetime.now(timezone.utc)
    baseline = now - timedelta(minutes=5)
    drifted = baseline + timedelta(hours=24)
    normal = baseline - timedelta(minutes=30)
    assert observe(clock_store, *delivery(10, baseline.isoformat())) == Observation.APPLIED
    anomaly = delivery(11, drifted.isoformat())
    with clock_store.connect(readonly=True) as db:
        before = db.execute("SELECT max_event_time_utc FROM billing_stream_progress").fetchone()[0]

    assert observe(clock_store, *anomaly) == Observation.QUARANTINED
    assert observe(clock_store, *anomaly) == Observation.DUPLICATE
    assert observe(clock_store, *delivery(12, normal.isoformat())) == Observation.APPLIED
    with clock_store.connect(readonly=True) as db:
        after = db.execute("SELECT max_event_time_utc FROM billing_stream_progress").fetchone()[0]
        quarantined = db.execute("""
            SELECT event_id, event_time_utc, reason
            FROM billing_stream_quarantine
        """).fetchall()
        window_events = db.execute("SELECT COUNT(*) FROM billing_stream_event").fetchone()[0]
        late_events = db.execute("SELECT COUNT(*) FROM billing_stream_late").fetchone()[0]

    assert before == after == baseline.isoformat(timespec="microseconds")
    assert [tuple(row) for row in quarantined] == [(
        json.loads(anomaly[1])["event_id"],
        drifted.isoformat(timespec="microseconds"),
        "future_event_time",
    )]
    assert window_events == 2 and late_events == 0
    print("S8: a 24-hour clock jump was isolated without advancing stream progress or marking a normal event late.")
PY
