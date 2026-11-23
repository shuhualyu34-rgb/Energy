"""Operational gauges for the CDP-1 sandbox."""

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone

from .billing import utc
from .store import Store
from .stream import ALLOWED_LATENESS, STREAM, Observation


def record_spark_sink(store: Store, checkpoint: str, batch_id: int, outcomes: Mapping[str, int]) -> None:
    key = hashlib.sha256(checkpoint.encode()).hexdigest()
    completed_at = datetime.now(timezone.utc).isoformat(timespec="microseconds")
    bind = store.bind
    values = ", ".join([bind] * 4)
    with store.connect() as connection:
        connection.execute(f"""
            INSERT INTO billing_spark_sink_progress
                (checkpoint_key, batch_id, completed_at_utc, outcomes)
            VALUES ({values})
            ON CONFLICT (checkpoint_key) DO UPDATE SET
                batch_id = excluded.batch_id, completed_at_utc = excluded.completed_at_utc,
                outcomes = excluded.outcomes
            WHERE excluded.batch_id >= billing_spark_sink_progress.batch_id
        """, (key, batch_id, completed_at, json.dumps(dict(outcomes), sort_keys=True)))


def metrics_text(store: Store) -> str:
    with store.connect(readonly=True) as connection:
        state = connection.execute(f"""
            SELECT
                (SELECT COUNT(*) FROM billing_collection_quality WHERE status = 'blocked') AS blocked_sources,
                (SELECT COUNT(*) FROM dwd_billing_order_v1_history) AS accepted_versions,
                (SELECT COUNT(*) FROM billing_kafka_delivery) AS acknowledged_versions,
                (SELECT COUNT(*) FROM billing_stream_event) AS observed_events,
                (SELECT COUNT(*) FROM billing_stream_late WHERE applied = FALSE) AS pending_late,
                (SELECT COUNT(*) FROM billing_stream_late WHERE applied = TRUE) AS applied_late,
                (SELECT COUNT(*) FROM billing_stream_quarantine) AS quarantined,
                (SELECT max_event_time_utc FROM billing_stream_progress
                    WHERE stream = {store.bind}) AS max_event_time_utc
        """, (STREAM,)).fetchone()
        spark = connection.execute("""
            SELECT batch_id, completed_at_utc, outcomes
            FROM billing_spark_sink_progress ORDER BY completed_at_utc DESC, checkpoint_key LIMIT 1
        """).fetchone()

    gauges = [
        ("cdp_collection_quality_blocked_sources", "Sources blocked by the latest incremental contract check.",
         state["blocked_sources"]),
        ("cdp_kafka_pending_events", "Accepted billing versions awaiting Kafka acknowledgement.",
         state["accepted_versions"] - state["acknowledged_versions"]),
        ("cdp_kafka_acknowledged_events", "Billing versions with an acknowledged Kafka record.",
         state["acknowledged_versions"]),
        ("cdp_stream_observed_events", "Distinct non-quarantined billing events observed in storage.",
         state["observed_events"]),
        ("cdp_stream_late_pending_events", "Late billing events waiting for backfill.", state["pending_late"]),
        ("cdp_stream_late_applied_events", "Late billing events applied by backfill.", state["applied_late"]),
        ("cdp_stream_quarantined_events", "Billing events quarantined for invalid event time.",
         state["quarantined"]),
    ]
    if state["max_event_time_utc"]:
        maximum = utc(state["max_event_time_utc"])
        gauges.append(("cdp_stream_watermark_timestamp_seconds", "Event-time watermark in Unix seconds.",
                       maximum.timestamp() - ALLOWED_LATENESS.total_seconds()))
    if spark:
        gauges.extend([
            ("cdp_spark_sink_last_batch_id", "Last completed Spark sink callback batch ID.", spark["batch_id"]),
            ("cdp_spark_sink_last_completed_timestamp_seconds",
             "Last Spark sink callback completion time in Unix seconds.",
             utc(spark["completed_at_utc"]).timestamp()),
        ])

    lines = []
    for name, help_text, value in gauges:
        lines.extend((f"# HELP {name} {help_text}", f"# TYPE {name} gauge", f"{name} {value}"))
    if spark:
        outcomes = json.loads(spark["outcomes"])
        name = "cdp_spark_sink_last_batch_events"
        lines.extend((f"# HELP {name} Events handled by the last Spark sink callback.",
                      f"# TYPE {name} gauge"))
        for outcome in Observation:
            lines.append(f'{name}{{outcome="{outcome}"}} {outcomes.get(outcome, 0)}')
    return "\n".join(lines) + "\n"
