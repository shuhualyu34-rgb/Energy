"""Read a batch of billing events and commit offsets only after durable observation."""

import argparse
import json
import os
from collections import Counter
from datetime import date

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from .billing_event import TOPIC
from .config import configured_store
from .kafka_recovery import recovery_scope, verify_target
from .stream import Observation, apply_late_events, observe


NORMAL_GROUP = "cdp-billing-window-dev"
RECOVERY_GROUP_PREFIX = "cdp-billing-recovery."


def consumer_route(group: str | None, run_id: str | None, target_id: str | None) -> tuple[str, str]:
    if run_id is None and target_id is None:
        if group and group.startswith(RECOVERY_GROUP_PREFIX):
            raise ValueError("that consumer group prefix is reserved for recovery")
        return TOPIC, group or NORMAL_GROUP
    if group is not None:
        raise ValueError("recovery uses its own consumer group")
    topic, scope = recovery_scope(run_id, target_id)
    return topic, f"{RECOVERY_GROUP_PREFIX}{scope}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.getenv("CDP_KAFKA_BOOTSTRAP"))
    parser.add_argument("--group")
    parser.add_argument("--recovery-run-id")
    parser.add_argument("--recovery-target-id")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--apply-late-day", type=date.fromisoformat, help="Apply queued events for a UTC date")
    args = parser.parse_args()
    store = configured_store()
    if args.apply_late_day:
        if args.recovery_run_id is not None or args.recovery_target_id is not None:
            parser.error("late backfill cannot select a Kafka recovery route")
        print(json.dumps({"day_utc": args.apply_late_day.isoformat(),
                          "applied": apply_late_events(store, args.apply_late_day)}))
        return
    if not args.bootstrap or args.limit < 1:
        parser.error("bootstrap and a positive limit are required")

    try:
        topic, group = consumer_route(args.group, args.recovery_run_id, args.recovery_target_id)
    except ValueError as error:
        parser.error(str(error))

    store.initialize()
    processed = 0
    outcomes = Counter()
    try:
        if args.recovery_run_id:
            verify_target(args.bootstrap, topic)
        consumer = KafkaConsumer(
            topic, bootstrap_servers=args.bootstrap.split(","), group_id=group,
            enable_auto_commit=False, auto_offset_reset="earliest", consumer_timeout_ms=5000,
        )
        try:
            for record in consumer:
                result = observe(store, record.key, record.value)
                partition = TopicPartition(record.topic, record.partition)
                consumer.commit({partition: OffsetAndMetadata(record.offset + 1, "", -1)})
                outcomes[result] += 1
                processed += 1
                if processed == args.limit:
                    break
        finally:
            consumer.close(autocommit=False)
    except (KafkaError, ValueError) as error:
        parser.exit(1, f"consumption stopped: {error}\n")
    report = {
        "processed": processed,
        "inserted": outcomes[Observation.APPLIED],
        "duplicates": outcomes[Observation.DUPLICATE],
        "late": outcomes[Observation.LATE],
        "quarantined": outcomes[Observation.QUARANTINED],
    }
    if args.recovery_run_id:
        report.update(topic=topic, group=group)
    print(json.dumps(report))


if __name__ == "__main__":
    main()
