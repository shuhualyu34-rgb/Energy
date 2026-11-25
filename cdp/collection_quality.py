"""Status of the most recent incremental contract check per source."""

from datetime import datetime, timezone

from .quality_rules import QualityRule
from .store import Store


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
    with store.connect() as connection:
        connection.execute(f"""
            INSERT INTO billing_collection_quality (source_key, status, checked_at_utc, failure_code)
            VALUES ({bind}, {bind}, {bind}, {bind})
            ON CONFLICT (source_key) DO UPDATE SET status = excluded.status,
                checked_at_utc = excluded.checked_at_utc, failure_code = excluded.failure_code
        """, (source_key, status, checked_at, failure_code))
        if failure_code and rule_id:
            connection.execute(f"""
                INSERT INTO billing_collection_quality_rule (source_key, rule_id)
                VALUES ({bind}, {bind})
                ON CONFLICT (source_key) DO UPDATE SET rule_id = excluded.rule_id
            """, (source_key, str(rule_id)))
        else:
            connection.execute(f"DELETE FROM billing_collection_quality_rule WHERE source_key = {bind}", (source_key,))


def collection_quality(store: Store) -> list[dict]:
    with store.connect(readonly=True) as connection:
        rows = connection.execute("""
            SELECT quality.source_key, quality.status, quality.checked_at_utc, quality.failure_code, rule.rule_id
            FROM billing_collection_quality AS quality
            LEFT JOIN billing_collection_quality_rule AS rule ON rule.source_key = quality.source_key
            ORDER BY quality.source_key
        """).fetchall()
    return [dict(row) for row in rows]
