#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
work_dir=$(mktemp -d /tmp/cdp-spark-recovery.XXXXXXXX)
project="cdp-recovery-${work_dir##*.}"
project=${project,,}
interrupted="${project}-interrupted"
resumed="${project}-resumed"
export CDP_PORT=0

dc() {
    docker compose -p "$project" -f compose.yaml -f compose.postgres.yaml \
        -f compose.kafka.yaml -f compose.spark.yaml "$@"
}

db() {
    dc exec -T postgres psql -X -qAt -v ON_ERROR_STOP=1 -U cdp -d cdp "$@"
}

cleanup() {
    local status=$?
    trap - EXIT
    if ((status != 0)); then
        dc ps -a >&2 || true
        docker logs --tail 40 "$interrupted" >&2 || true
        docker logs --tail 40 "$resumed" >&2 || true
    fi
    docker rm -f "$interrupted" "$resumed" >/dev/null 2>&1 || true
    dc --profile spark down --volumes >/dev/null 2>&1 || true
    rmdir "$work_dir" || true
    exit "$status"
}
trap cleanup EXIT

dc up -d --no-build --wait --wait-timeout 120
dc exec -T api sh -eu <<'SH'
python -c 'import sqlite3; from pathlib import Path; db=sqlite3.connect("/data/recovery-fixture.sqlite3"); db.executescript(Path("samples/billing-v1-demo.sql").read_text()); db.close()'
python -m cdp.collect /data/recovery-fixture.sqlite3 --format billing-v1 --source-id recovery --page-size 1
python -m cdp.publish
SH

db <<'SQL'
CREATE FUNCTION rehearsal_pause_second_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status = 'REFUNDED' THEN
        PERFORM pg_sleep(120);
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER rehearsal_pause_second_event BEFORE INSERT ON billing_stream_event
FOR EACH ROW EXECUTE FUNCTION rehearsal_pause_second_event();
SQL

dc run -d --no-deps --name "$interrupted" \
    -e 'CDP_POSTGRES_DSN=postgresql://cdp:cdp-local-only@postgres:5432/cdp?application_name=cdp-recovery-spark' \
    spark --once >/dev/null

# Wait for the second write to block after the first row has committed.
deadline=$((SECONDS + 90))
backend=""
while ((SECONDS < deadline)); do
    backend=$(db -c "SELECT pid FROM pg_stat_activity
        WHERE application_name = 'cdp-recovery-spark' AND wait_event = 'PgSleep'")
    if [[ "$backend" =~ ^[0-9]+$ ]]; then
        break
    fi
    [[ "$(docker inspect -f '{{.State.Running}}' "$interrupted")" == true ]]
    sleep 1
done
[[ "$backend" =~ ^[0-9]+$ ]]
[[ "$(db -c 'SELECT status FROM billing_stream_event')" == COMPLETED ]]
[[ "$(db -c 'SELECT COUNT(*) FROM billing_spark_sink_progress')" == 0 ]]
printf '%s\n' 'Before interruption: 1 event committed, second write paused, no completed Spark sink batch.'

docker kill --signal KILL "$interrupted" >/dev/null
[[ "$(docker inspect -f '{{.State.ExitCode}}' "$interrupted")" == 137 ]]
# PostgreSQL may still be sleeping after the detached driver is killed.
db -c "SELECT pg_terminate_backend($backend)
       FROM pg_stat_activity WHERE pid = $backend AND application_name = 'cdp-recovery-spark'" >/dev/null
db <<'SQL'
DROP TRIGGER rehearsal_pause_second_event ON billing_stream_event;
DROP FUNCTION rehearsal_pause_second_event();
SQL

dc run --no-deps --name "$resumed" spark --once
[[ "$(db -c 'SELECT COUNT(*) FROM billing_stream_event')" == 2 ]]
[[ "$(db -c 'SELECT batch_id FROM billing_spark_sink_progress')" == 0 ]]
dc exec -T api python - <<'PY'
import json
from urllib.request import urlopen

with urlopen("http://127.0.0.1:8000/v1/demo/billing-events/windows?day=2026-01-14", timeout=5) as response:
    rows = json.load(response)["rows"]
assert len(rows) == 2, rows
counts = {row["status"]: row["event_count"] for row in rows}
assert counts == {"COMPLETED": 1, "REFUNDED": 1}, rows
print(f"After restart: {counts}")
PY

result=$(db -c "SELECT outcomes::json ->> 'applied' || ',' || (outcomes::json ->> 'duplicate')
                FROM billing_spark_sink_progress")
[[ "$result" == 1,1 ]]
printf '%s\n' 'PASS: batch 0 replayed one committed event and applied one missing event.'
