#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

work_dir=$(mktemp -d /tmp/cdp-pipeline-latency.XXXXXXXX)
project="cdp-latency-${work_dir##*.}"
project=${project,,}
export CDP_PORT=0
export CDP_PIPELINE_INTERVAL_SECONDS=${CDP_PIPELINE_INTERVAL_SECONDS:-5}

dc() {
    docker compose -p "$project" -f compose.yaml -f compose.postgres.yaml -f compose.kafka.yaml \
        -f compose.pipeline.yaml -f compose.spark.yaml --profile pipeline "$@"
}

cleanup() {
    local status=$?
    trap - EXIT
    if ((status != 0)); then
        dc ps -a >&2 || true
        dc logs --tail 40 api pipeline spark-worker >&2 || true
    fi
    dc down --volumes >/dev/null 2>&1 || true
    rmdir "$work_dir" || true
    exit "$status"
}
trap cleanup EXIT

dc up -d --no-build --wait --wait-timeout 120 api
dc exec -T api python - <<'PY'
import sqlite3
from contextlib import closing
from pathlib import Path

with closing(sqlite3.connect("/data/billing-v1-pipeline.sqlite3")) as source, source:
    source.executescript(Path("samples/billing-v1-demo.sql").read_text())
    source.execute("DELETE FROM billing_v1_event")
PY
dc up -d --no-build --wait --wait-timeout 120 pipeline spark-worker

dc exec -T api python -u - <<'PY'
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from math import ceil
from time import monotonic, sleep
from urllib.request import urlopen

source_path = "/data/billing-v1-pipeline.sqlite3"
deadline_seconds = 300
latencies = []
expected_by_day = {}


def api(route):
    with urlopen(f"http://127.0.0.1:8000{route}", timeout=5) as result:
        return result.read()


def windows(day):
    response = json.loads(api(f"/v1/demo/billing-events/windows?day={day}"))
    assert response["window_minutes"] == 5, response
    return response["rows"]


for wave in range(3):
    started = monotonic()
    event_time = datetime.now(timezone.utc)
    day = event_time.date().isoformat()
    window = event_time.replace(minute=event_time.minute // 5 * 5, second=0, microsecond=0)
    stamp = event_time.isoformat(timespec="microseconds")
    expected = {}
    source_rows = []
    for number in range(wave * 10 + 1, wave * 10 + 11):
        station = f"ST_SH_{number:03d}"
        order_id = f"ORD_{event_time:%Y%m%d}_SH{number:03d}_{number:04d}"
        payload = dict(
            order_id=order_id, station_id=station, gun_id=f"GUN_SH{number:03d}_01A",
            user_id_hmac="a" * 64, start_time=(event_time - timedelta(minutes=2)).isoformat(),
            end_time=stamp, updated_at=stamp, status="COMPLETED", version=1, total_kwh=10,
            total_amount_cents=1100, elec_fee_cents=800, service_fee_cents=300,
        )
        source_rows.append((stamp, order_id, 1, json.dumps(payload)))
        expected[station] = {
            "station_id": station, "event_count": 1, "status": "COMPLETED",
            "window_start_utc": window.isoformat(),
        }
    with closing(sqlite3.connect(source_path, timeout=10)) as source, source:
        source.executemany("INSERT INTO billing_v1_event VALUES (?, ?, ?, ?)", source_rows)
    expected_by_day.setdefault(day, {}).update(expected)

    pending = set(expected)
    while pending:
        rows = windows(day)
        elapsed = monotonic() - started
        if elapsed > deadline_seconds:
            raise TimeoutError(f"Wave {wave + 1} not visible within {deadline_seconds}s: {sorted(pending)}")
        for row in rows:
            station = row["station_id"]
            if station in pending:
                assert row == expected[station], row
                pending.remove(station)
                latencies.append(elapsed)
        if pending:
            sleep(1)
    print(f"Wave {wave + 1}: all 10 new orders visible within {elapsed:.1f}s.")

for day, expected in expected_by_day.items():
    actual = windows(day)
    assert len(actual) == len(expected), actual
    assert {row["station_id"]: row for row in actual} == expected, actual

deadline = monotonic() + 30
while True:
    metrics = api("/metrics").decode().splitlines()
    completed = next((float(line.split()[1]) for line in metrics
                      if line.startswith("cdp_spark_sink_last_completed_timestamp_seconds ")), 0)
    if completed >= event_time.timestamp():
        break
    if monotonic() > deadline:
        raise TimeoutError("Spark did not report a completed callback after the final wave started")
    sleep(1)

for metric, value in {
    "cdp_kafka_acknowledged_events": len(latencies),
    "cdp_kafka_pending_events": 0,
    "cdp_stream_observed_events": len(latencies),
    "cdp_stream_late_pending_events": 0,
    "cdp_stream_quarantined_events": 0,
}.items():
    assert f"{metric} {value}" in metrics, metrics

p95 = sorted(latencies)[ceil(0.95 * len(latencies)) - 1]
assert len(latencies) == 30 and p95 <= deadline_seconds, latencies
print(f"PASS: {len(latencies)} generated orders, source to API visibility P95 {p95:.1f}s; max {max(latencies):.1f}s.")
PY
