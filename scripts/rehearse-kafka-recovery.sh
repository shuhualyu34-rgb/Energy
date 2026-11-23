#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ $# -gt 1 || ($# -eq 1 && "$1" != --spark) ]]; then
    printf '%s\n' 'Usage: rehearse-kafka-recovery.sh [--spark]' >&2
    exit 2
fi
spark_options=()
if [[ $# -eq 1 ]]; then
    spark_options=(-f compose.spark.yaml --profile spark)
fi

work_dir=$(mktemp -d /tmp/cdp-kafka-recovery.XXXXXXXX)
project="cdp-kafka-recovery-${work_dir##*.}"
project=${project,,}
export CDP_PORT=0

dc() {
    docker compose -p "$project" -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml "${spark_options[@]}" "$@"
}

cleanup() {
    local status=$?
    trap - EXIT
    if ((status != 0)); then dc ps -a >&2 || true; fi
    dc down --volumes >/dev/null 2>&1 || true
    rm -f "$work_dir/stream-state.json" "$work_dir/empty-acceptance.json" "$work_dir/original-spark.log" "$work_dir/first-spark.log" "$work_dir/resumed-spark.log"
    rmdir "$work_dir"
    exit "$status"
}
trap cleanup EXIT

dc up -d --no-build --wait --wait-timeout 90 api
dc exec -T api sh -eu <<'SH'
python -c 'import sqlite3; from pathlib import Path; db=sqlite3.connect("/data/replay-source.sqlite3"); db.executescript(Path("samples/billing-v1-demo.sql").read_text()); db.close()'
python -m cdp.collect /data/replay-source.sqlite3 --format billing-v1 --source-id kafka-recovery-test --page-size 1
python -m cdp.publish
SH
dc exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic billing.orders.v1.recovery.rehearsal --partitions 3 --replication-factor 1
for expected in '1:1' '1:0' '0:0'; do
    dc exec -T api python -m cdp.kafka_recovery --run-id rehearsal --limit 1 |
        python3 -c '
import json,sys
report=json.load(sys.stdin)
expected=sys.argv[1]
assert "{}:{}".format(report["replayed"], report["remaining"]) == expected, report
assert report["legacy_without_archive"] == 0, report
print(report)
' "$expected"
done

dc exec -T api python - <<'PY'
import json
from kafka import KafkaConsumer
from cdp.billing_event import TOPIC
from cdp.config import configured_store
from cdp.kafka_recovery import recovery_topic

def read(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers="kafka:9092", group_id=None,
                             auto_offset_reset="earliest", consumer_timeout_ms=5000)
    try:
        return [(row.key, row.value) for row in consumer]
    finally:
        consumer.close()

normal = read(TOPIC)
recovery = read(recovery_topic("rehearsal"))
assert normal == recovery and len(recovery) == 2, (normal, recovery)
assert [json.loads(payload)["version"] for _, payload in recovery] == [1, 2]
store = configured_store()
with store.connect(readonly=True) as connection:
    for table in ("billing_kafka_delivery", "billing_kafka_recovery_delivery"):
        rows = connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
        assert rows["count"] == 2
print("PASS: recovery topic contains exactly the same two archived events as the original topic.")
PY

dc exec -T api python -m cdp.consume --limit 2
dc exec -T api python -m cdp.consume --recovery-run-id rehearsal --recovery-target-id existing --limit 2 |
    python3 -c '
import json, sys
report = json.load(sys.stdin)
assert report["processed"] == report["duplicates"] == 2 and report["inserted"] == 0, report
print("Existing windows:", report)
'

dc exec -T postgres createdb -U cdp cdp_replayed
recovered() {
    dc exec -T -e CDP_POSTGRES_DSN=postgresql://cdp:cdp-local-only@postgres:5432/cdp_replayed api "$@"
}

compare_recovery() {
    dc exec -T -e CDP_RECOVERY_TARGET_DSN=postgresql://cdp:cdp-local-only@postgres:5432/cdp_replayed \
        api python -m cdp.recovery_check "$@"
}
recovered python -c 'from cdp.config import configured_store; configured_store().initialize()'
status=0
compare_recovery --from-day 2026-01-14 > "$work_dir/empty-acceptance.json" || status=$?
test "$status" = 2
python3 -c 'import json,sys; r=json.load(sys.stdin); assert r["result"]=="mismatch" and r["window_differences"]==2, r' < "$work_dir/empty-acceptance.json"

for expected in 1 1 0; do
    recovered python -m cdp.consume --recovery-run-id rehearsal --recovery-target-id rebuilt --limit 1 |
        python3 -c '
import json, sys
report = json.load(sys.stdin)
expected = int(sys.argv[1])
assert report["processed"] == report["inserted"] == expected and report["duplicates"] == 0, report
assert report["group"] == "cdp-billing-recovery.rehearsal.rebuilt", report
print("Rebuilt windows:", report)
' "$expected"
done

window_query='from datetime import date; import json; from cdp.config import configured_store; from cdp.stream import billing_windows; print(json.dumps(billing_windows(configured_store(), date(2026, 1, 14)), sort_keys=True))'
reference=$(dc exec -T api python -c "$window_query")
test "$reference" = "$(recovered python -c "$window_query")"
printf '%s\n' 'PASS: recovery consumption deduplicates existing state and rebuilds the same windows in an empty database.'

compare_recovery --from-day 2026-01-14 --through-day 2026-01-15 | python3 -c '
import json, sys
report = json.load(sys.stdin)
assert report["result"] == "match" and report["source_events"] == report["target_events"] == 2, report
print("Recovery acceptance:", report["result"], report["source_events"])
'
status=0
compare_recovery --from-day 2026-01-15 > "$work_dir/empty-acceptance.json" || status=$?
test "$status" = 2
python3 -c 'import json,sys; assert json.load(sys.stdin)["result"]=="no_data"' < "$work_dir/empty-acceptance.json"

quarantine_probe='
import json
from cdp.billing_event import event_id
from cdp.config import configured_store
from cdp.stream import Observation, observe
order_id = "ORD_20260115_SH001_9999"
updated = "2026-01-14T16:30:00.000000+00:00"
payload = {
    "schema_version": "1.0", "event_id": event_id(order_id, 1, updated),
    "order_id": order_id, "version": 1, "updated_at": updated, "status": "COMPLETED",
    "order": {"station_id": "ST_SH_001", "end_time": "2099-01-15T00:00:00Z"},
}
assert observe(configured_store(), order_id.encode(), json.dumps(payload).encode()) == Observation.QUARANTINED
'
dc exec -T api python -c "$quarantine_probe"
status=0
compare_recovery --from-day 2026-01-14 --stream-state > "$work_dir/empty-acceptance.json" || status=$?
test "$status" = 2
python3 -c '
import json,sys
r = json.load(sys.stdin)
assert r["result"] == "mismatch" and r["window_differences"] == 0, r
assert r["stream_state"]["watermark_match"] and not r["stream_state"]["quarantine_match"], r
print("Stream acceptance correctly refused a missing quarantine record.")
' < "$work_dir/empty-acceptance.json"
dc exec -T postgres psql -X -qAt -v ON_ERROR_STOP=1 -U cdp -d cdp_replayed -c \
    "UPDATE billing_stream_progress SET max_event_time_utc = '2026-01-14T16:29:00.000000+00:00'"
dc exec -T api python -m cdp.stream_snapshot export > "$work_dir/stream-state.json"
recovered sh -c 'cat > /tmp/cdp-stream-state.json' < "$work_dir/stream-state.json"
recovered python -m cdp.stream_snapshot restore /tmp/cdp-stream-state.json | python3 -c '
import json,sys
r = json.load(sys.stdin)
assert r == {"quarantine_restored":1,"watermark_restored":True}, r
print("Stream state restored:", r)
'
recovered python -m cdp.stream_snapshot restore /tmp/cdp-stream-state.json | python3 -c '
import json,sys
r = json.load(sys.stdin)
assert r == {"quarantine_restored":0,"watermark_restored":False}, r
'

compare_recovery --from-day 2026-01-14 --stream-state | python3 -c '
import json,sys
r = json.load(sys.stdin)
assert r["result"] == "match" and r["stream_state"]["watermark_match"] and r["stream_state"]["quarantine_match"], r
print("PASS: recovery acceptance matched the stream watermark and quarantine after reconciliation.")
'

if [[ $# -eq 1 ]]; then
    dc run -T --rm --no-deps spark --once > "$work_dir/original-spark.log" 2>&1 || {
        cat "$work_dir/original-spark.log" >&2
        exit 1
    }
    grep -F '{"batch_id": 0, "outcomes": {"duplicate": 2}}' "$work_dir/original-spark.log"

    dc exec -T postgres createdb -U cdp cdp_spark_replayed
    run_spark_recovery() {
        dc run -T --rm --no-deps \
            -e CDP_POSTGRES_DSN=postgresql://cdp:cdp-local-only@postgres:5432/cdp_spark_replayed \
            spark --once --recovery-run-id rehearsal --recovery-target-id spark-rebuilt
    }
    run_spark_recovery > "$work_dir/first-spark.log" 2>&1 || {
        cat "$work_dir/first-spark.log" >&2
        exit 1
    }
    grep -F '{"batch_id": 0, "outcomes": {"applied": 2}}' "$work_dir/first-spark.log"

    dc exec -T api python - <<'PYCODE'
from contextlib import closing
from datetime import datetime, timezone
import sqlite3

updated = datetime.now(timezone.utc).isoformat(timespec="microseconds")
with closing(sqlite3.connect("/data/replay-source.sqlite3")) as db, db:
    db.execute("""
        INSERT INTO billing_v1_event
        SELECT ?, order_id, 3, json_set(payload, '$.version', 3, '$.updated_at', ?)
        FROM billing_v1_event WHERE version=2
    """, (updated, updated))
PYCODE
    dc exec -T api python -m cdp.collect /data/replay-source.sqlite3 --format billing-v1 --source-id kafka-recovery-test
    dc exec -T api python -m cdp.kafka_recovery --run-id rehearsal --limit 1
    run_spark_recovery > "$work_dir/resumed-spark.log" 2>&1 || {
        cat "$work_dir/resumed-spark.log" >&2
        exit 1
    }
    grep -F '{"batch_id": 1, "outcomes": {"applied": 1}}' "$work_dir/resumed-spark.log"
    dc exec -T -e CDP_POSTGRES_DSN=postgresql://cdp:cdp-local-only@postgres:5432/cdp_spark_replayed api \
        python -c "$window_query" | python3 -c '
import json, sys
rows = json.load(sys.stdin)
assert {row["status"]: row["event_count"] for row in rows} == {"COMPLETED":1, "REFUNDED":2}, rows
print("Spark recovery windows:", rows)
'
    test "$reference" = "$(dc exec -T api python -c "$window_query")"
    test "$(dc exec -T postgres psql -X -qAt -U cdp -d cdp -c 'SELECT batch_id FROM billing_spark_sink_progress')" = 0
    printf '%s\n' 'PASS: Spark recovery used its own checkpoint, resumed one new event, and left source windows unchanged.'
fi
