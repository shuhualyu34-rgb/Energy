"""Read Kafka with Spark Structured Streaming and write billing observations."""

import argparse
import json
import os
from collections import Counter

from kafka.errors import KafkaError
from pyspark.sql import SparkSession

from cdp.billing_event import TOPIC
from cdp.checkpoint import CheckpointInUse, checkpoint_lease, recovery_checkpoint
from cdp.config import configured_store
from cdp.kafka_recovery import recovery_scope, verify_target
from cdp.metrics import record_spark_sink
from cdp.stream import observe


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Drain currently available Kafka offsets")
    mode.add_argument("--follow", action="store_true", help="Poll every five seconds")
    parser.add_argument("--max-offsets", type=int, default=500)
    parser.add_argument("--recovery-run-id")
    parser.add_argument("--recovery-target-id")
    args = parser.parse_args()

    bootstrap = os.getenv("CDP_KAFKA_BOOTSTRAP")
    checkpoint = os.getenv("CDP_SPARK_CHECKPOINT")
    dsn = os.getenv("CDP_POSTGRES_DSN")
    if not bootstrap or not checkpoint or not dsn:
        parser.error("Kafka, PostgreSQL and a durable checkpoint location are required")
    if args.max_offsets < 1:
        parser.error("--max-offsets must be positive")

    topic = TOPIC
    if args.recovery_run_id is not None or args.recovery_target_id is not None:
        try:
            topic, _ = recovery_scope(args.recovery_run_id, args.recovery_target_id)
            cluster = verify_target(bootstrap, topic)
            checkpoint = recovery_checkpoint(checkpoint, cluster, args.recovery_run_id, args.recovery_target_id)
        except (ValueError, KafkaError) as error:
            parser.exit(1, f"cannot start recovery: {error}\n")

    try:
        with checkpoint_lease(dsn, checkpoint) as lease:
            store = configured_store()
            store.initialize()
            spark = (
                SparkSession.builder.appName("CDP-1 billing lifecycle")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("WARN")

            def write_batch(batch, batch_id: int) -> None:
                lease.execute("SELECT 1")
                outcomes = Counter()
                for row in batch.orderBy("partition", "offset").toLocalIterator():
                    outcome = observe(store, row.key, row.value)
                    outcomes[outcome] += 1
                record_spark_sink(store, checkpoint, batch_id, outcomes)
                print(json.dumps({"batch_id": batch_id, "outcomes": dict(outcomes)}, sort_keys=True), flush=True)

            try:
                writer = (
                    spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", bootstrap)
                    .option("subscribe", topic)
                    .option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "true")
                    .option("maxOffsetsPerTrigger", args.max_offsets)
                    .load()
                    .select("partition", "offset", "key", "value")
                    .writeStream.foreachBatch(write_batch)
                    .option("checkpointLocation", checkpoint)
                )
                query = (
                    writer.trigger(processingTime="5 seconds") if args.follow
                    else writer.trigger(availableNow=True)
                ).start()
                query.awaitTermination()
            finally:
                spark.stop()
    except CheckpointInUse as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
