"""Publish accepted billing history to Kafka with retry-safe delivery markers."""

import argparse
import json
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .billing_event import BILLING_ARCHIVE, TOPIC, billing_payload
from .config import configured_store
from .store import Store

Send = Callable[[bytes, bytes], tuple[int, int]]


def publish_pending(store: Store, send: Send, limit: int) -> int:
    if limit < 1:
        raise ValueError("limit must be positive")
    store.initialize()
    bind = store.bind
    with store.connect(readonly=True) as connection:
        batch = connection.execute(f"""
            SELECT h.order_id, h.version, h.updated_at_utc, h.status, h.snapshot, h.tou_slices, a.archive_sha256
            FROM dwd_billing_order_v1_history AS h
            LEFT JOIN billing_kafka_delivery AS d
                ON (d.order_id, d.version, d.updated_at_utc) =
                   (h.order_id, h.version, h.updated_at_utc)
            LEFT JOIN billing_order_archive AS a
                ON (a.order_id, a.version, a.updated_at_utc) =
                   (h.order_id, h.version, h.updated_at_utc)
            WHERE d.order_id IS NULL
            ORDER BY h.order_id, h.version, h.updated_at_utc
            LIMIT {bind}
        """, (limit,)).fetchall()

    published = 0
    for row in batch:
        position = (row["order_id"], row["version"], row["updated_at_utc"])
        with store.write_order(row["order_id"]) as connection:
            delivered = connection.execute(f"""
                SELECT 1 FROM billing_kafka_delivery
                WHERE (order_id, version, updated_at_utc) = ({bind}, {bind}, {bind})
            """, position).fetchone()
            if delivered:
                continue
            digest = row["archive_sha256"]
            payload = (
                store.read_archive(digest, namespace=BILLING_ARCHIVE) if digest
                else billing_payload(position, row["status"], row["snapshot"], row["tou_slices"])
            )
            partition, offset = send(row["order_id"].encode(), payload)
            connection.execute(f"""
                INSERT INTO billing_kafka_delivery
                    (order_id, version, updated_at_utc, kafka_partition, kafka_offset)
                VALUES ({bind}, {bind}, {bind}, {bind}, {bind})
            """, (*position, partition, offset))
        published += 1
    return published


@contextmanager
def kafka_sender(bootstrap: str, topic: str) -> Iterator[Send]:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","), enable_idempotence=True,
        acks="all", max_block_ms=10000,
    )
    try:
        def send(key: bytes, payload: bytes) -> tuple[int, int]:
            receipt = producer.send(topic, key=key, value=payload).get(timeout=15)
            return receipt.partition, receipt.offset

        yield send
    finally:
        producer.close(timeout=3)


def publish_to_kafka(store: Store, bootstrap: str, limit: int) -> int:
    with kafka_sender(bootstrap, TOPIC) as send:
        return publish_pending(store, send, limit)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.getenv("CDP_KAFKA_BOOTSTRAP"))
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if not args.bootstrap or args.limit < 1:
        parser.error("bootstrap and a positive limit are required")

    try:
        published = publish_to_kafka(configured_store(), args.bootstrap, args.limit)
    except (KafkaError, OSError, ValueError) as error:
        parser.exit(1, f"publication failed: {error}\n")
    print(json.dumps({"topic": TOPIC, "published": published}))


if __name__ == "__main__":
    main()
