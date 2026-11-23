"""Status of the most recent incremental contract check per source."""

from datetime import datetime, timezone

from .store import Store


class CollectionRejected(ValueError):
    def __init__(self, code: str):
        super().__init__("billing row rejected; collection cutoff unchanged")
        self.code = code


def record_quality(store: Store, source_key: str, failure_code: str | None) -> None:
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


def collection_quality(store: Store) -> list[dict]:
    with store.connect(readonly=True) as connection:
        rows = connection.execute("""
            SELECT source_key, status, checked_at_utc, failure_code
            FROM billing_collection_quality ORDER BY source_key
        """).fetchall()
    return [dict(row) for row in rows]
