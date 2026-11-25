"""Current status and history of incremental contract checks."""

from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4

from .quality_rules import QualityRule
from .store import BUSINESS_ZONE, Store


class CollectionRejected(ValueError):
    def __init__(self, code: str, rule_id: QualityRule | None = None):
        super().__init__("billing row rejected; collection cutoff unchanged")
        self.code = code
        self.rule_id = rule_id


def record_quality(
    store: Store, source_key: str, failure_code: str | None, rule_id: QualityRule | None = None,
) -> None:
    bind = store.bind
    status = "blocked" if failure_code else "ok"
    checked_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    rule = str(rule_id) if failure_code and rule_id else None
    with store.connect() as connection:
        connection.execute(f"""
            INSERT INTO billing_collection_quality (source_key, status, checked_at_utc, failure_code)
            VALUES ({bind}, {bind}, {bind}, {bind})
            ON CONFLICT (source_key) DO UPDATE SET status = excluded.status,
                checked_at_utc = excluded.checked_at_utc, failure_code = excluded.failure_code
        """, (source_key, status, checked_at, failure_code))
        if rule is not None:
            connection.execute(f"""
                INSERT INTO billing_collection_quality_rule (source_key, rule_id)
                VALUES ({bind}, {bind})
                ON CONFLICT (source_key) DO UPDATE SET rule_id = excluded.rule_id
            """, (source_key, rule))
        else:
            connection.execute(f"DELETE FROM billing_collection_quality_rule WHERE source_key = {bind}", (source_key,))
        connection.execute(f"""
            INSERT INTO billing_collection_quality_history
                (check_id, checked_at_utc, source_key, status, failure_code, rule_id)
            VALUES ({bind}, {bind}, {bind}, {bind}, {bind}, {bind})
        """, (uuid4().hex, checked_at, source_key, status, failure_code, rule))


def collection_quality(store: Store) -> list[dict]:
    with store.connect(readonly=True) as connection:
        rows = connection.execute("""
            SELECT quality.source_key, quality.status, quality.checked_at_utc, quality.failure_code, rule.rule_id
            FROM billing_collection_quality AS quality
            LEFT JOIN billing_collection_quality_rule AS rule ON rule.source_key = quality.source_key
            ORDER BY quality.source_key
        """).fetchall()
    return [dict(row) for row in rows]


def quality_day_bounds(day: date) -> tuple[str, str]:
    start = datetime.combine(day, time.min, tzinfo=BUSINESS_ZONE)
    end = start + timedelta(days=1)
    return (
        start.astimezone(timezone.utc).isoformat(timespec="microseconds"),
        end.astimezone(timezone.utc).isoformat(timespec="microseconds"),
    )


def quality_history(store: Store, day: date, limit: int, offset: int) -> list[dict]:
    start, end = quality_day_bounds(day)
    bind = store.bind
    with store.connect(readonly=True) as connection:
        rows = connection.execute(f"""
            SELECT check_id, checked_at_utc, source_key, status, failure_code, rule_id
            FROM billing_collection_quality_history
            WHERE checked_at_utc >= {bind} AND checked_at_utc < {bind}
            ORDER BY checked_at_utc, check_id
            LIMIT {bind} OFFSET {bind}
        """, (start, end, limit, offset)).fetchall()
    return [dict(row) for row in rows]


def daily_quality(store: Store, day: date) -> dict:
    start, end = quality_day_bounds(day)
    bind = store.bind
    with store.connect(readonly=True) as connection:
        rows = connection.execute(f"""
            SELECT source_key, status, failure_code, rule_id, COUNT(*) AS check_count
            FROM billing_collection_quality_history
            WHERE checked_at_utc >= {bind} AND checked_at_utc < {bind}
            GROUP BY source_key, status, failure_code, rule_id
            ORDER BY source_key, status, COALESCE(failure_code, ''), COALESCE(rule_id, '')
        """, (start, end)).fetchall()
    return {
        "business_day": day,
        "timezone": BUSINESS_ZONE.key,
        "total_checks": sum(row["check_count"] for row in rows),
        "blocked_checks": sum(row["check_count"] for row in rows if row["status"] == "blocked"),
        "rows": [dict(row) for row in rows],
    }
