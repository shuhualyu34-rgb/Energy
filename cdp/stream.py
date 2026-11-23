"""Persist idempotent billing event observations and query UTC windows."""

import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .billing import BillingContractError, utc
from .billing_event import event_id
from .contract import Key
from .store import Store


class EventOrder(BaseModel):
    station_id: Key
    end_time: str


class BillingEvent(BaseModel):
    model_config = ConfigDict(strict=True)

    schema_version: Literal["1.0"]
    event_id: str
    order_id: Key
    version: Annotated[int, Field(gt=0, le=2**63 - 1)]
    updated_at: str
    status: Literal["CHARGING", "COMPLETED", "ABNORMAL_CLOSED", "REFUNDED"]
    order: EventOrder


STREAM = "billing-orders"
STREAM_LOCK = "billing-stream-watermark"
ALLOWED_LATENESS = timedelta(hours=2)


class Observation(StrEnum):
    APPLIED = "applied"
    LATE = "late"
    QUARANTINED = "quarantined"
    DUPLICATE = "duplicate"


def observe(store: Store, key: bytes, payload: bytes) -> Observation:
    try:
        raw = json.loads(payload)
        event = BillingEvent.model_validate(raw)
    except (ValueError, ValidationError):
        raise BillingContractError("invalid billing event envelope") from None
    if key != event.order_id.encode() or event.event_id != event_id(
        event.order_id, event.version, event.updated_at,
    ):
        raise BillingContractError("invalid billing event identity")

    if utc(event.updated_at).isoformat(timespec="microseconds") != event.updated_at:
        raise BillingContractError("billing event update time must be canonical UTC")
    ended = utc(event.order.end_time)
    future = ended > datetime.now(timezone.utc) + timedelta(minutes=5)
    event_time = ended.isoformat(timespec="microseconds")
    window = ended.replace(minute=ended.minute // 5 * 5, second=0, microsecond=0)
    digest = hashlib.sha256(json.dumps(raw, sort_keys=True).encode()).hexdigest()
    return record_observation(store, event, event_time, window.isoformat(), digest, future)


def record_observation(store, event, event_time, window, digest, future) -> Observation:
    store.initialize()
    b = store.bind
    with store.write_order(STREAM_LOCK) as connection:
        existing = connection.execute(f"""
            SELECT payload_hash FROM billing_stream_event WHERE event_id = {b}
            UNION ALL
            SELECT payload_hash FROM billing_stream_quarantine WHERE event_id = {b}
        """, (event.event_id, event.event_id)).fetchone()
        if existing:
            if existing["payload_hash"] != digest:
                raise BillingContractError("conflicting payload for an observed event ID")
            return Observation.DUPLICATE

        if future:
            connection.execute(f"""
                INSERT INTO billing_stream_quarantine
                    (event_id, payload_hash, event_time_utc, reason)
                VALUES ({b}, {b}, {b}, 'future_event_time')
            """, (event.event_id, digest, event_time))
            return Observation.QUARANTINED

        progress = connection.execute(f"""
            SELECT max_event_time_utc FROM billing_stream_progress WHERE stream = {b}
        """, (STREAM,)).fetchone()
        connection.execute(f"""
            INSERT INTO billing_stream_event
                (event_id, payload_hash, station_id, status, window_start_utc)
            VALUES ({b}, {b}, {b}, {b}, {b})
        """, (event.event_id, digest, event.order.station_id, event.status, window))

        if progress:
            maximum = utc(progress["max_event_time_utc"])
            event_datetime = utc(event_time)
            if maximum - event_datetime > ALLOWED_LATENESS:
                watermark = maximum - ALLOWED_LATENESS
                connection.execute(f"""
                    INSERT INTO billing_stream_late (event_id, event_time_utc, watermark_utc)
                    VALUES ({b}, {b}, {b})
                """, (event.event_id, event_time, watermark.isoformat(timespec="microseconds")))
                return Observation.LATE

        connection.execute(f"""
            INSERT INTO billing_stream_progress (stream, max_event_time_utc)
            VALUES ({b}, {b})
            ON CONFLICT (stream) DO UPDATE SET max_event_time_utc = excluded.max_event_time_utc
            WHERE excluded.max_event_time_utc > billing_stream_progress.max_event_time_utc
        """, (STREAM, event_time))
        return Observation.APPLIED


def apply_late_events(store: Store, day: date) -> int:
    store.initialize()
    start, end = day_bounds(day)
    b = store.bind
    with store.write_order(STREAM_LOCK) as connection:
        result = connection.execute(f"""
            UPDATE billing_stream_late SET applied = TRUE WHERE applied = FALSE
              AND event_id IN (
                  SELECT event_id FROM billing_stream_event
                  WHERE window_start_utc >= {b} AND window_start_utc < {b}
              )
        """, (start, end))
        return result.rowcount


def day_bounds(day: date) -> tuple[str, str]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = (start + timedelta(days=1)).isoformat() if day != date.max else "9999-12-31T24"
    return start.isoformat(), end


def billing_windows(store: Store, day: date) -> list[dict]:
    start, end = day_bounds(day)
    with store.connect(readonly=True) as connection:
        rows = connection.execute(f"""
            SELECT window_start_utc, station_id, status, COUNT(*) AS event_count
            FROM billing_stream_event
            WHERE window_start_utc >= {store.bind} AND window_start_utc < {store.bind}
              AND NOT EXISTS (
                  SELECT 1 FROM billing_stream_late AS late
                  WHERE late.event_id = billing_stream_event.event_id AND late.applied = FALSE
              )
            GROUP BY window_start_utc, station_id, status
            ORDER BY window_start_utc, station_id, status
        """, (start, end)).fetchall()
    return [dict(row) for row in rows]
