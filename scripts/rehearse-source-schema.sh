#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

work_dir=$(mktemp -d /tmp/cdp-schema-rehearsal.XXXXXXXX)
project="cdp-schema-${work_dir##*.}"
project=${project,,}
export CDP_PORT=0 CDP_PROMETHEUS_PORT=0

dc() {
    docker compose -p "$project" -f compose.yaml -f compose.postgres.yaml -f compose.monitoring.yaml "$@"
}

cleanup() {
    local status=$?
    trap - EXIT
    if ((status != 0)); then
        dc ps -a >&2 || true
    fi
    dc down --volumes >/dev/null 2>&1 || true
    rmdir "$work_dir" || true
    exit "$status"
}
trap cleanup EXIT

dc up -d --no-build --wait --wait-timeout 90
dc exec -T api python - <<'PY'
import sqlite3
from contextlib import closing
from pathlib import Path

with closing(sqlite3.connect("/data/schema-fixture.sqlite3")) as db, db:
    db.executescript(Path("samples/billing-v1-demo.sql").read_text())
    db.execute("""
        UPDATE billing_v1_event SET payload = json_remove(
            json_set(payload, '$.pay_amount', json_extract(payload, '$.total_amount_cents')),
            '$.total_amount_cents'
        ) WHERE version = 2
    """)
PY

status=0
dc exec -T api python -m cdp.collect /data/schema-fixture.sqlite3 --format billing-v1 \
    --source-id schema-rehearsal --page-size 1 || status=$?
[[ "$status" == 1 ]]
api=$(dc port api 8000)
prom=$(dc port prometheus 9090)

wait_for_state() {
    python3 - "$api" "$prom" "$1" <<'PY'
import json
import sys
import time
from urllib.request import urlopen

api, prom, expected = sys.argv[1:]

def load(target, route):
    with urlopen(f"http://{target}{route}", timeout=5) as response:
        return json.load(response)

deadline = time.monotonic() + 100
while True:
    sources = load(api, "/v1/demo/collection-quality")["sources"]
    rules = load(prom, "/api/v1/rules")["data"]["groups"]
    alert = next(rule for group in rules for rule in group["rules"]
                 if rule["name"] == "CdpCollectionQualityBlocked")
    if sources[0]["status"] == expected and (
        (expected == "blocked" and alert["state"] == "firing")
        or (expected == "ok" and alert["state"] == "inactive")
    ):
        print(f"Collection quality: {expected}; Prometheus rule: {alert['state']}.")
        break
    if time.monotonic() > deadline:
        raise AssertionError((sources, alert["state"]))
    time.sleep(2)
PY
}

wait_for_state blocked
dc exec -T api python - <<'PY'
import sqlite3
from contextlib import closing

with closing(sqlite3.connect("/data/collection.sqlite3")) as state:
    assert state.execute("""
        SELECT COUNT(*) FROM sqlite_master
        WHERE type = 'table' AND name = 'collection_checkpoint'
    """).fetchone()[0] == 0
with closing(sqlite3.connect("/data/schema-fixture.sqlite3")) as db, db:
    db.execute("""
        UPDATE billing_v1_event SET payload = json_remove(
            json_set(payload, '$.total_amount_cents', json_extract(payload, '$.pay_amount')),
            '$.pay_amount'
        ) WHERE version = 2
    """)
PY
dc exec -T api python -m cdp.collect /data/schema-fixture.sqlite3 --format billing-v1 \
    --source-id schema-rehearsal --page-size 1 | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["outcomes"] == {"duplicate": 1, "updated": 1}, result
print(result["outcomes"])
'
wait_for_state ok
printf '%s\n' 'PASS: invalid source field blocked collection; retry recovered and alert cleared.'
