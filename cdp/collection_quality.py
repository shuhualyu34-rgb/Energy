"""Current status and history of incremental contract checks."""

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4

from .quality_rules import QualityRule
from .store import BUSINESS_ZONE, Store


@dataclass(frozen=True)
class RejectedRecord:
    order_ref: str
    record_ref: str
    declared_amount_cents: int | None


class CollectionRejected(ValueError):
    def __init__(
        self, code: str, rule_id: QualityRule | None = None, record: RejectedRecord | None = None,
    ):
        super().__init__("billing row rejected; collection cutoff unchanged")
        self.code = code
        self.rule_id = rule_id
        self.record = record


def record_quality(
    store: Store, source_key: str, failure_code: str | None, rule_id: QualityRule | None = None,
    *, rejection: RejectedRecord | None = None,
) -> None:
    bind = store.bind
    status = "blocked" if failure_code else "ok"
    checked_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    check_id = uuid4().hex
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
        """, (check_id, checked_at, source_key, status, failure_code, rule))
        if failure_code and rejection is not None:
            connection.execute(f"""
                INSERT INTO billing_collection_rejection
                    (check_id, order_ref, record_ref, declared_amount_cents)
                VALUES ({bind}, {bind}, {bind}, {bind})
            """, (check_id, rejection.order_ref, rejection.record_ref, rejection.declared_amount_cents))


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
            SELECT history.check_id, checked_at_utc, source_key, status, failure_code, rule_id,
                   rejection.check_id IS NOT NULL AS has_rejection_evidence, declared_amount_cents
            FROM billing_collection_quality_history AS history
            LEFT JOIN billing_collection_rejection AS rejection ON rejection.check_id = history.check_id
            WHERE checked_at_utc >= {bind} AND checked_at_utc < {bind}
            ORDER BY checked_at_utc, history.check_id
            LIMIT {bind} OFFSET {bind}
        """, (start, end, limit, offset)).fetchall()
    return [dict(row) | {"has_rejection_evidence": bool(row["has_rejection_evidence"])} for row in rows]


def daily_quality(store: Store, day: date) -> dict:
    start, end = quality_day_bounds(day)
    bind = store.bind
    with store.connect(readonly=True) as connection:
        rows = connection.execute(f"""
            SELECT source_key, status, failure_code, rule_id, order_ref, record_ref,
                   declared_amount_cents, COUNT(*) AS check_count
            FROM billing_collection_quality_history AS history
            LEFT JOIN billing_collection_rejection AS rejection ON rejection.check_id = history.check_id
            WHERE checked_at_utc >= {bind} AND checked_at_utc < {bind}
            GROUP BY source_key, status, failure_code, rule_id, order_ref, record_ref, declared_amount_cents
            ORDER BY source_key, status, COALESCE(failure_code, ''), COALESCE(rule_id, '')
        """, (start, end)).fetchall()

    counts = Counter()
    records = set()
    amounts_by_order: dict[tuple[str, str], set[int | None]] = {}
    untracked = 0
    for row in rows:
        source, status = row["source_key"], row["status"]
        counts[source, status, row["failure_code"], row["rule_id"]] += row["check_count"]
        if row["order_ref"] is not None:
            records.add((source, row["record_ref"]))
            amounts_by_order.setdefault((source, row["order_ref"]), set()).add(row["declared_amount_cents"])
        elif status == "blocked":
            untracked += row["check_count"]

    known_amounts = [next(iter(amounts)) for amounts in amounts_by_order.values()
                     if len(amounts) == 1 and None not in amounts]
    return {
        "business_day": day,
        "timezone": BUSINESS_ZONE.key,
        "total_checks": sum(counts.values()),
        "blocked_checks": sum(count for key, count in counts.items() if key[1] == "blocked"),
        "rows": [
            {"source_key": source, "status": status, "failure_code": failure, "rule_id": rule, "check_count": count}
            for (source, status, failure, rule), count in counts.items()
        ],
        "quarantine": {
            "distinct_orders": len(amounts_by_order),
            "distinct_records": len(records),
            "orders_with_unknown_amount": len(amounts_by_order) - len(known_amounts),
            "known_declared_amount_cents": sum(known_amounts),
            "blocked_checks_without_evidence": untracked,
        },
    }
