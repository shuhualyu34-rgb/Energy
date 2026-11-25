"""Incremental order polling against a read-only billing snapshot."""

import argparse
import json
import sqlite3
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from .billing import BillingContractError
from .billing_ledger import ingest_billing
from .collection_quality import CollectionRejected, record_quality
from .collection_state import CHECKPOINT_FILE, CHECKPOINT_SCHEMA
from .config import configured_store
from .contract import Key, Order
from .quality_rules import QualityRule
from .store import Store, VersionConflict

ORDER_COLUMNS = tuple(sorted(Order.model_fields.keys() - {"schema_version"}))
ORDER_COLUMNS_SQL = ", ".join(ORDER_COLUMNS)
LOOKBACK = timedelta(minutes=15)
DEMO = "demo"
BILLING_V1 = "billing-v1"
SOURCE_QUERIES = {
    DEMO: ("billing_order_snapshot", ORDER_COLUMNS_SQL),
    BILLING_V1: ("billing_v1_event", "order_id, version, payload"),
}


def utc_key(value: datetime) -> str:
    if value.utcoffset() is None:
        raise ValueError("collection times must include a timezone")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def collect(
    source: Path,
    store: Store,
    source_id: str,
    through: datetime,
    page_size: int,
    *,
    source_format: str = DEMO,
) -> dict:
    upper_utc = utc_key(through)
    checkpoint_id = source_id if source_format == DEMO else f"{source_format}:{source_id}"

    store.directory.mkdir(parents=True, exist_ok=True)
    checkpoint = store.directory / CHECKPOINT_FILE
    source_uri = source.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(checkpoint, timeout=1)) as state, state:
        state.execute("BEGIN IMMEDIATE")
        state.execute(CHECKPOINT_SCHEMA)
        row = state.execute(
            "SELECT through_utc FROM collection_checkpoint WHERE source_id = ?",
            (checkpoint_id,),
        ).fetchone()
        previous = datetime.fromisoformat(row[0]) if row else None
        if previous is not None and through < previous:
            raise ValueError("cutoff precedes the last successful collection")
        epoch = datetime.min.replace(tzinfo=timezone.utc)
        lower = max(previous, epoch + LOOKBACK) - LOOKBACK if previous else epoch
        lower_utc = utc_key(lower)
        store.initialize()
        try:
            outcomes = poll(source_uri, store, lower_utc, upper_utc, page_size, source_format)
        except CollectionRejected as error:
            record_quality(store, checkpoint_id, error.code, error.rule_id)
            raise
        record_quality(store, checkpoint_id, None)
        state.execute("""
            INSERT INTO collection_checkpoint VALUES (?, ?)
            ON CONFLICT(source_id) DO UPDATE SET through_utc = excluded.through_utc
        """, (checkpoint_id, upper_utc))
    return {"source": source_id, "through_utc": upper_utc, "outcomes": dict(outcomes)}


def poll(
    source_uri: str, store: Store, lower: str, upper: str, page_size: int,
    source_format: str,
) -> Counter:
    if page_size < 1:
        raise ValueError("page size must be positive")
    table, columns = SOURCE_QUERIES[source_format]
    cursor = (lower, "", 0)
    outcomes = Counter()
    with closing(sqlite3.connect(source_uri, uri=True)) as source:
        source.row_factory = sqlite3.Row
        source.execute("BEGIN")
        while True:
            rows = source.execute(f"""
                SELECT updated_at, {columns}
                FROM {table}
                WHERE (updated_at, order_id, version) > (?, ?, ?) AND updated_at <= ?
                ORDER BY updated_at, order_id, version
                LIMIT ?
            """, (*cursor, upper, page_size)).fetchall()
            if not rows:
                return outcomes
            for row in rows:
                cursor = (row["updated_at"], row["order_id"], row["version"])
                try:
                    if source_format == BILLING_V1:
                        outcome = ingest_billing(store, row["payload"], source_position=cursor)
                    else:
                        snapshot = {column: row[column] for column in ORDER_COLUMNS}
                        snapshot["schema_version"] = "0.1"
                        outcome = store.ingest(json.dumps(snapshot, sort_keys=True).encode())
                except VersionConflict:
                    raise CollectionRejected("version_conflict", QualityRule.ORDER_VERSION_CONFLICT) from None
                except BillingContractError as error:
                    raise CollectionRejected("contract_validation", error.rule_id) from None
                except ValidationError:
                    raise CollectionRejected("contract_validation", QualityRule.DEMO_ORDER_CONTRACT) from None
                outcomes[outcome] += 1


def backfill(
    source: Path, store: Store, start: datetime, through: datetime, page_size: int,
    *, source_format: str = DEMO,
) -> dict:
    lower, upper = utc_key(start), utc_key(through)
    if lower > upper:
        raise ValueError("backfill start is later than its cutoff")
    store.initialize()
    source_uri = source.resolve().as_uri() + "?mode=ro"
    outcomes = poll(source_uri, store, lower, upper, page_size, source_format)
    return {"backfill_from_utc": lower, "through_utc": upper, "outcomes": dict(outcomes)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--format", choices=SOURCE_QUERIES, default=DEMO)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--source-id", help="Stable checkpoint identity for this source and destination")
    mode.add_argument("--backfill-from", type=datetime.fromisoformat)
    parser.add_argument("--through", type=datetime.fromisoformat, help="Required cutoff for a backfill")
    parser.add_argument("--page-size", type=int, default=500)
    args = parser.parse_args()

    if bool(args.backfill_from) != bool(args.through):
        parser.error("--backfill-from and --through must be used together")
    try:
        store = configured_store()
        if args.backfill_from:
            report = backfill(args.source, store, args.backfill_from, args.through, args.page_size, source_format=args.format)
        else:
            source_id = TypeAdapter(Key).validate_python(args.source_id)
            report = collect(args.source, store, source_id, datetime.now(timezone.utc), args.page_size, source_format=args.format)
    except ValidationError:
        parser.exit(1, "invalid source identity\n")
    except (sqlite3.Error, OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
