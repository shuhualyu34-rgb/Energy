"""Run and retain platform quality audits at four Shanghai time slots per day."""

import argparse
import json
import signal
import sqlite3
import sys
from datetime import date, datetime, timezone
from threading import Event

import psycopg

from .collection_quality import quality_day_bounds
from .config import configured_store
from .quality_audit import quality_audit
from .store import BUSINESS_ZONE, Store


def audit_slot(when: datetime) -> str:
    local = when.astimezone(BUSINESS_ZONE)
    slot = local.replace(hour=local.hour // 6 * 6, minute=0, second=0, microsecond=0)
    return slot.astimezone(timezone.utc).isoformat(timespec="microseconds")


def run_scheduled_audit(store: Store, now: datetime) -> dict:
    slot = audit_slot(now)
    bind = store.bind
    with store.connect(readonly=True) as connection:
        existing = connection.execute(
            f"SELECT 1 FROM billing_quality_audit_run WHERE slot_utc = {bind}", (slot,),
        ).fetchone()
    if existing:
        return {"slot_utc": slot, "status": "already_recorded"}

    report = quality_audit(store)
    with store.connect() as connection:
        inserted = connection.execute(f"""
            INSERT INTO billing_quality_audit_run (slot_utc, checked_at_utc, results)
            VALUES ({bind}, {bind}, {bind})
            ON CONFLICT (slot_utc) DO NOTHING
        """, (slot, report["checked_at_utc"], json.dumps(report["results"]))).rowcount
    return {"slot_utc": slot, "status": "recorded" if inserted else "already_recorded"}


def scheduled_audit_history(store: Store, day: date) -> list[dict]:
    start, end = quality_day_bounds(day)
    bind = store.bind
    with store.connect(readonly=True) as connection:
        rows = connection.execute(f"""
            SELECT slot_utc, checked_at_utc, results
            FROM billing_quality_audit_run
            WHERE slot_utc >= {bind} AND slot_utc < {bind}
            ORDER BY slot_utc
        """, (start, end)).fetchall()
    return [dict(row) | {"results": json.loads(row["results"])} for row in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--follow", action="store_true", help="Keep running future audit slots")
    args = parser.parse_args()
    store = configured_store()
    store.initialize()

    if not args.follow:
        print(json.dumps(run_scheduled_audit(store, datetime.now(timezone.utc))))
        return 0

    stop = Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    completed_slot = None
    while not stop.is_set():
        now = datetime.now(timezone.utc)
        if audit_slot(now) != completed_slot:
            try:
                result = run_scheduled_audit(store, now)
                completed_slot = result["slot_utc"]
                print(json.dumps(result), flush=True)
            except (psycopg.Error, sqlite3.Error) as error:
                print(f"Quality audit could not run: {type(error).__name__}", file=sys.stderr, flush=True)
        stop.wait(30)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
