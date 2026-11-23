"""Export and restore verified stream watermark and quarantine metadata."""

import argparse
import hashlib
import hmac
import json
import sqlite3
from pathlib import Path
from typing import Annotated, Literal

import psycopg
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .billing import utc
from .config import configured_store
from .store import Store
from .stream import STREAM, STREAM_LOCK

EVENT_STATE_SELECT = """
    SELECT e.event_id, e.payload_hash, e.window_start_utc, e.station_id, e.status,
           CASE WHEN l.applied = FALSE THEN 1 ELSE 0 END AS pending
    FROM billing_stream_event AS e
    LEFT JOIN billing_stream_late AS l ON l.event_id = e.event_id
"""
QUARANTINE_SELECT = """
    SELECT event_id, payload_hash, event_time_utc, reason
    FROM billing_stream_quarantine ORDER BY event_id
"""
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class Quarantined(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    event_id: Digest
    payload_hash: Digest
    event_time_utc: str
    reason: Literal["future_event_time"]

    @field_validator("event_time_utc")
    @classmethod
    def valid_timestamp(cls, value: str) -> str:
        if utc(value).isoformat(timespec="microseconds") != value:
            raise ValueError("quarantine time must use canonical UTC")
        return value


class StreamState(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    format_version: Literal[1]
    active_event_count: Annotated[int, Field(ge=0)]
    active_sha256: Digest
    max_event_time_utc: str | None
    quarantine: list[Quarantined]

    @field_validator("max_event_time_utc")
    @classmethod
    def valid_frontier(cls, value: str | None) -> str | None:
        if value is not None and utc(value).isoformat(timespec="microseconds") != value:
            raise ValueError("stream frontier must use canonical UTC")
        return value


class StateBundle(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    snapshot: StreamState
    sha256: Digest


def event_fingerprint_bytes(row) -> bytes:
    entry = (
        row["event_id"], row["payload_hash"], row["window_start_utc"],
        row["station_id"], row["status"], row["pending"],
    )
    return json.dumps(entry, separators=(",", ":")).encode() + b"\n"


def active_fingerprint(connection) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(EVENT_STATE_SELECT + " ORDER BY e.event_id"):
        digest.update(event_fingerprint_bytes(row))
        count += 1
    return count, digest.hexdigest()


def frontier(connection, bind: str) -> str | None:
    row = connection.execute(f"""
        SELECT max_event_time_utc FROM billing_stream_progress WHERE stream = {bind}
    """, (STREAM,)).fetchone()
    return utc(row["max_event_time_utc"]).isoformat(timespec="microseconds") if row else None


def snapshot_checksum(snapshot: StreamState) -> str:
    raw = json.dumps(snapshot.model_dump(), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def export_stream_state(store: Store) -> StateBundle:
    with store.write_order(STREAM_LOCK) as connection:
        count, digest = active_fingerprint(connection)
        snapshot = StreamState(
            format_version=1, active_event_count=count, active_sha256=digest,
            max_event_time_utc=frontier(connection, store.bind),
            quarantine=[Quarantined.model_validate(dict(row)) for row in connection.execute(QUARANTINE_SELECT)],
        )
    return StateBundle(snapshot=snapshot, sha256=snapshot_checksum(snapshot))


def restore_stream_state(store: Store, raw: bytes) -> dict:
    bundle = StateBundle.model_validate_json(raw)
    snapshot = bundle.snapshot
    if not hmac.compare_digest(snapshot_checksum(snapshot), bundle.sha256):
        raise ValueError("stream snapshot checksum mismatch")
    expected_quarantine = {row.event_id: row.model_dump() for row in snapshot.quarantine}
    if len(expected_quarantine) != len(snapshot.quarantine):
        raise ValueError("stream snapshot contains duplicate quarantine IDs")
    if bool(snapshot.active_event_count) != bool(snapshot.max_event_time_utc):
        raise ValueError("stream snapshot has an inconsistent event count and frontier")

    b = store.bind
    with store.write_order(STREAM_LOCK) as connection:
        if active_fingerprint(connection) != (snapshot.active_event_count, snapshot.active_sha256):
            raise ValueError("target stream events or pending late status differ from the snapshot")
        existing = {row["event_id"]: dict(row) for row in connection.execute(QUARANTINE_SELECT)}
        if any(expected_quarantine.get(key) != row for key, row in existing.items()):
            raise ValueError("target quarantine differs from the snapshot")

        missing = [row for key, row in expected_quarantine.items() if key not in existing]
        for row in missing:
            if connection.execute(f"""
                SELECT 1 FROM billing_stream_event WHERE event_id = {b}
            """, (row["event_id"],)).fetchone():
                raise ValueError("quarantine event already exists as an active event")
            connection.execute(f"""
                INSERT INTO billing_stream_quarantine
                    (event_id, payload_hash, event_time_utc, reason)
                VALUES ({b}, {b}, {b}, {b})
            """, (row["event_id"], row["payload_hash"], row["event_time_utc"], row["reason"]))

        current = frontier(connection, b)
        updated = current != snapshot.max_event_time_utc
        if updated:
            if snapshot.max_event_time_utc is None:
                raise ValueError("target has a frontier without matching snapshot metadata")
            connection.execute(f"""
                INSERT INTO billing_stream_progress (stream, max_event_time_utc) VALUES ({b}, {b})
                ON CONFLICT (stream) DO UPDATE SET max_event_time_utc = excluded.max_event_time_utc
            """, (STREAM, snapshot.max_event_time_utc))

    return {"quarantine_restored": len(missing), "watermark_restored": updated}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("export")
    actions.add_parser("restore").add_argument("file", type=Path)
    args = parser.parse_args()
    store = configured_store()
    try:
        result = export_stream_state(store).model_dump() if args.action == "export" else restore_stream_state(
            store, args.file.read_bytes(),
        )
    except ValidationError:
        parser.exit(1, "invalid stream snapshot format\n")
    except (sqlite3.Error, psycopg.Error) as error:
        parser.exit(1, f"stream snapshot operation failed ({type(error).__name__})\n")
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
