"""Compare source and recovered stream windows without writing either database."""

import argparse
import hashlib
import json
import os
import sqlite3
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import psycopg

from .billing import utc
from .config import configured_store
from .postgres import PostgresStore
from .store import Store
from .stream import ALLOWED_LATENESS, STREAM, day_bounds
from .stream_snapshot import EVENT_STATE_SELECT, QUARANTINE_SELECT, event_fingerprint_bytes

MAX_EXAMPLES = 20


def summarize_day(store: Store, day: date) -> tuple[str, int, int, Counter]:
    start, end = day_bounds(day)
    bind = store.bind
    digest = hashlib.sha256()
    windows = Counter()
    count = pending = 0
    with store.connect(readonly=True) as connection:
        rows = connection.execute(f"""
            {EVENT_STATE_SELECT}
            WHERE e.window_start_utc >= {bind} AND e.window_start_utc < {bind}
            ORDER BY e.event_id
        """, (start, end))
        for row in rows:
            digest.update(event_fingerprint_bytes(row))
            count += 1
            if row["pending"]:
                pending += 1
            else:
                windows[(row["window_start_utc"], row["station_id"], row["status"])] += 1
    return digest.hexdigest(), count, pending, windows


def summarize_stream_state(store: Store) -> dict:
    digest = hashlib.sha256()
    quarantine_count = 0
    with store.connect(readonly=True) as connection:
        progress = connection.execute(f"""
            SELECT max_event_time_utc FROM billing_stream_progress WHERE stream = {store.bind}
        """, (STREAM,)).fetchone()
        quarantined = connection.execute(QUARANTINE_SELECT)
        for row in quarantined:
            entry = (row["event_id"], row["payload_hash"], row["event_time_utc"], row["reason"])
            digest.update(json.dumps(entry, separators=(",", ":")).encode() + b"\n")
            quarantine_count += 1

    maximum = utc(progress["max_event_time_utc"]) if progress else None
    return {
        "max_event_time_utc": maximum.isoformat(timespec="microseconds") if maximum else None,
        "watermark_timestamp_seconds": maximum.timestamp() - ALLOWED_LATENESS.total_seconds() if maximum else None,
        "quarantined_events": quarantine_count,
        "quarantine_sha256": digest.hexdigest(),
    }



def compare_windows(source: Store, target: Store, first: date, last: date, *, with_stream_state: bool = False) -> dict:
    if last < first:
        raise ValueError("through-day must be on or after from-day")

    days = []
    examples = []
    source_total = target_total = difference_total = 0
    passed = True
    for offset in range((last - first).days + 1):
        day = first + timedelta(days=offset)
        source_digest, source_count, source_pending, source_windows = summarize_day(source, day)
        target_digest, target_count, target_pending, target_windows = summarize_day(target, day)
        difference_count = 0
        for window in sorted(source_windows.keys() | target_windows.keys()):
            source_hits, target_hits = source_windows[window], target_windows[window]
            if source_hits == target_hits:
                continue
            difference_count += 1
            if len(examples) < MAX_EXAMPLES:
                minute, station, status = window
                examples.append({
                    "window_start_utc": minute, "station_id": station, "status": status,
                    "source_count": source_hits, "target_count": target_hits,
                })

        event_match = source_digest == target_digest and source_count == target_count
        passed = passed and event_match and difference_count == 0 and not (source_pending or target_pending)
        source_total += source_count
        target_total += target_count
        difference_total += difference_count
        days.append({
            "day_utc": day.isoformat(), "source_events": source_count, "target_events": target_count,
            "source_pending_late": source_pending, "target_pending_late": target_pending,
            "event_fingerprint_match": event_match, "window_differences": difference_count,
        })

    report = {
        "result": ("match" if passed else "mismatch") if source_total or target_total else "no_data",
        "source_events": source_total, "target_events": target_total,
        "window_differences": difference_total, "window_examples": examples, "days": days,
    }
    if with_stream_state:
        source_state = summarize_stream_state(source)
        target_state = summarize_stream_state(target)
        watermark_match = source_state["max_event_time_utc"] == target_state["max_event_time_utc"]
        quarantine_match = (
            source_state["quarantined_events"] == target_state["quarantined_events"]
            and source_state["quarantine_sha256"] == target_state["quarantine_sha256"]
        )
        report["stream_state"] = {
            "watermark_match": watermark_match, "quarantine_match": quarantine_match,
            "source": source_state, "target": target_state,
        }
        if not watermark_match or not quarantine_match:
            report["result"] = "mismatch"
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-day", required=True, type=date.fromisoformat)
    parser.add_argument("--through-day", type=date.fromisoformat)
    parser.add_argument("--stream-state", action="store_true", help="Compare stream-wide watermark and quarantine")
    parser.add_argument("--target-data-dir", type=Path, help="SQLite target directory")
    args = parser.parse_args()
    target_dsn = os.getenv("CDP_RECOVERY_TARGET_DSN")
    if bool(args.target_data_dir) == bool(target_dsn):
        parser.error("configure one target: --target-data-dir or CDP_RECOVERY_TARGET_DSN")
    source = configured_store()
    target = PostgresStore(source.directory, target_dsn) if target_dsn else Store(args.target_data_dir)
    if (type(source) is PostgresStore and target_dsn == source.dsn
            or type(source) is Store and type(target) is Store and source.directory == target.directory):
        parser.error("source and target must be different databases")
    try:
        result = compare_windows(source, target, args.from_day, args.through_day or args.from_day,
                                 with_stream_state=args.stream_state)
    except (sqlite3.Error, psycopg.Error) as error:
        parser.exit(1, f"could not read recovery data ({type(error).__name__})\n")
    except ValueError as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(result, indent=2))
    return 0 if result["result"] == "match" else 2


if __name__ == "__main__":
    raise SystemExit(main())
