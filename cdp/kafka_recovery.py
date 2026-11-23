"""Rebuild a dedicated Kafka recovery topic from verified V1 archives."""

import argparse
import json
import os

from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError
from pydantic import TypeAdapter, ValidationError

from .billing_event import BILLING_ARCHIVE, TOPIC
from .config import configured_store
from .contract import Key
from .publish import Send, kafka_sender
from .store import Store


def recovery_topic(run_id: str) -> str:
    try:
        valid = TypeAdapter(Key).validate_python(run_id)
    except ValidationError:
        raise ValueError("invalid Kafka recovery run ID") from None
    return f"{TOPIC}.recovery.{valid}"


def recovery_scope(run_id: str | None, target_id: str | None) -> tuple[str, str]:
    if not run_id or not target_id:
        raise ValueError("recovery requires both run and target IDs")
    topic = recovery_topic(run_id)
    try:
        target = TypeAdapter(Key).validate_python(target_id)
    except ValidationError:
        raise ValueError("invalid recovery target ID") from None
    return topic, f"{run_id}.{target}"



def verify_target(bootstrap: str, topic: str) -> str:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap.split(","), request_timeout_ms=10000)
    try:
        cluster = admin.describe_cluster().get("cluster_id")
        descriptions = admin.describe_topics([topic])
    finally:
        admin.close()
    if not isinstance(cluster, str) or not cluster:
        raise ValueError("Kafka cluster identity is unavailable")
    if len(descriptions) != 1 or descriptions[0].get("error_code") or not descriptions[0].get("partitions"):
        raise ValueError("create the dedicated recovery topic before replay")
    return cluster


def replay_archives(store: Store, cluster: str, run_id: str, send: Send, limit: int) -> dict:
    if limit < 1:
        raise ValueError("limit must be positive")
    topic = recovery_topic(run_id)
    store.initialize()
    b = store.bind
    scope = (cluster, topic)
    with store.connect(readonly=True) as connection:
        rows = connection.execute(f"""
            SELECT a.order_id, a.version, a.updated_at_utc, a.archive_sha256
            FROM billing_order_archive AS a
            LEFT JOIN billing_kafka_recovery_delivery AS d
                ON (a.order_id, a.version, a.updated_at_utc) =
                   (d.order_id, d.version, d.updated_at_utc)
                AND d.cluster_id = {b} AND d.recovery_topic = {b}
            WHERE d.order_id IS NULL
            ORDER BY a.order_id, a.version, a.updated_at_utc
            LIMIT {b}
        """, (*scope, limit)).fetchall()

    replayed = 0
    for row in rows:
        position = (row["order_id"], row["version"], row["updated_at_utc"])
        with store.write_order(row["order_id"]) as connection:
            delivered = connection.execute(f"""
                SELECT 1 FROM billing_kafka_recovery_delivery
                WHERE (cluster_id, recovery_topic, order_id, version, updated_at_utc) =
                      ({b}, {b}, {b}, {b}, {b})
            """, (*scope, *position)).fetchone()
            if delivered:
                continue
            payload = store.read_archive(row["archive_sha256"], namespace=BILLING_ARCHIVE)
            partition, offset = send(row["order_id"].encode(), payload)
            connection.execute(f"""
                INSERT INTO billing_kafka_recovery_delivery
                    (cluster_id, recovery_topic, order_id, version, updated_at_utc, kafka_partition, kafka_offset)
                VALUES ({b}, {b}, {b}, {b}, {b}, {b}, {b})
            """, (*scope, *position, partition, offset))
        replayed += 1

    with store.connect(readonly=True) as connection:
        counts = connection.execute(f"""
            SELECT
                (SELECT COUNT(*) FROM dwd_billing_order_v1_history) AS accepted,
                (SELECT COUNT(*) FROM billing_order_archive) AS archived,
                (SELECT COUNT(*) FROM billing_kafka_recovery_delivery
                    WHERE cluster_id = {b} AND recovery_topic = {b}) AS delivered
        """, scope).fetchone()
    return {
        "cluster_id": cluster, "topic": topic, "replayed": replayed,
        "remaining": counts["archived"] - counts["delivered"],
        "legacy_without_archive": counts["accepted"] - counts["archived"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--bootstrap", default=os.getenv("CDP_KAFKA_BOOTSTRAP"))
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if not args.bootstrap or args.limit < 1:
        parser.error("bootstrap and a positive limit are required")
    try:
        topic = recovery_topic(args.run_id)
        cluster = verify_target(args.bootstrap, topic)
        with kafka_sender(args.bootstrap, topic) as send:
            report = replay_archives(configured_store(), cluster, args.run_id, send, args.limit)
    except (KafkaError, OSError, ValueError) as error:
        parser.exit(1, f"archive replay failed: {error}\n")
    print(json.dumps(report, indent=2))
    return 2 if report["legacy_without_archive"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
