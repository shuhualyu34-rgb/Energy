"""Run V1 collection and bounded Kafka publication in order."""

import argparse
import json
import os
import signal
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

import psycopg
from kafka.errors import KafkaError
from pydantic import TypeAdapter, ValidationError

from .collect import BILLING_V1, collect
from .collection_quality import CollectionRejected
from .config import configured_store
from .contract import Key
from .publish import publish_to_kafka
from .store import Store


def run_once(
    store: Store, source: Path, source_id: str, bootstrap: str, page_size: int, publish_limit: int,
) -> dict:
    through = datetime.now(timezone.utc)
    report = collect(store=store, source=source, source_id=source_id, through=through,
                     page_size=page_size, source_format=BILLING_V1)
    published = publish_to_kafka(store, bootstrap, publish_limit)
    return {"status": "ok", "collection": report, "published": published}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=os.getenv("CDP_BILLING_SOURCE"))
    parser.add_argument("--source-id", default=os.getenv("CDP_BILLING_SOURCE_ID"))
    parser.add_argument("--bootstrap", default=os.getenv("CDP_KAFKA_BOOTSTRAP"))
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--publish-limit", type=int, default=100)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--follow", action="store_true")
    args = parser.parse_args()
    if not args.source or not args.bootstrap or min(args.page_size, args.publish_limit, args.interval) < 1:
        parser.error("source, bootstrap and positive limits are required")
    try:
        source_id = TypeAdapter(Key).validate_python(args.source_id)
    except ValidationError:
        parser.error("a valid source identity is required")

    store = configured_store()
    stop = Event()
    if args.follow:
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())

    while not stop.is_set():
        try:
            report = run_once(store, args.source, source_id, args.bootstrap, args.page_size, args.publish_limit)
        except CollectionRejected as error:
            report = {"status": "blocked", "reason": error.code}
        except KafkaError:
            report = {"status": "failed", "reason": "kafka_unavailable"}
        except (sqlite3.Error, psycopg.Error, OSError, ValueError):
            report = {"status": "failed", "reason": "collection_or_storage_error"}
        print(json.dumps(report, sort_keys=True), flush=True)
        if not args.follow:
            return 0 if report["status"] == "ok" else 1
        stop.wait(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
