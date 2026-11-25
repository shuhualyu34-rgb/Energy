#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ "${1:-}" == "--local" && $# -eq 1 ]]; then
    python -E - <<'PYTHON'
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from cdp.billing_ledger import billing_history
from cdp.collect import BILLING_V1, collect
from cdp.collection_quality import CollectionRejected, collection_quality
from cdp.collection_state import export_state
from cdp.metrics import metrics_text
from cdp.store import Store

cutoff = datetime.fromisoformat("2026-01-14T16:31:00+00:00")
order_id = "ORD_20260115_SH001_0001"
source_id = "schema-rehearsal"

with TemporaryDirectory() as directory:
    root = Path(directory)
    source = root / "fixture.sqlite"
    with closing(sqlite3.connect(source)) as db, db:
        db.executescript(Path("samples/billing-v1-demo.sql").read_text())
        original = db.execute("SELECT payload FROM billing_v1_event WHERE version = 2").fetchone()[0]
        changed = json.loads(original)
        changed["pay_amount"] = changed.pop("total_amount_cents")
        db.execute("UPDATE billing_v1_event SET payload = ? WHERE version = 2", (json.dumps(changed),))

    store = Store(root / "platform")
    options = {"source_format": BILLING_V1, "page_size": 1}
    collect(source, store, source_id, cutoff - timedelta(minutes=1), **options)
    before = export_state(store.directory)
    try:
        collect(source, store, source_id, cutoff, **options)
    except CollectionRejected as error:
        assert (error.code, error.rule_id) == ("contract_validation", "billing_schema")
    else:
        raise AssertionError("schema change did not block collection")

    assert export_state(store.directory) == before
    row, = collection_quality(store)
    assert (row["status"], row["failure_code"]) == ("blocked", "contract_validation"), row
    assert "cdp_collection_quality_blocked_sources 1" in metrics_text(store).splitlines()
    assert [row["version"] for row in billing_history(store, order_id)] == [1]

    with closing(sqlite3.connect(source)) as db, db:
        db.execute("UPDATE billing_v1_event SET payload = ? WHERE version = 2", (original,))
    report = collect(source, Store(store.directory), source_id, cutoff, **options)
    assert report["outcomes"] == {"duplicate": 1, "updated": 1}, report
    assert export_state(store.directory).snapshot.checkpoints[0].through_utc == report["through_utc"]
    row, = collection_quality(store)
    assert (row["status"], row["failure_code"]) == ("ok", None), row
    assert "cdp_collection_quality_blocked_sources 0" in metrics_text(store).splitlines()
    history = billing_history(store, order_id)
    assert [row["status"] for row in history] == ["COMPLETED", "REFUNDED"]
    assert history[-1]["snapshot"]["total_amount_cents"] == json.loads(original)["total_amount_cents"]
    print("S4: renamed field blocked collection, preserved the checkpoint and raised the quality metric.")
    print("After repair: one duplicate suppressed, one version recovered, original amount verified, metric cleared.")
PYTHON
    exit
fi
if (( $# != 0 )); then
    printf 'Usage: %s [--local]\n' "${0##*/}" >&2
    exit 2
fi

work_dir=$(mktemp -d /tmp/cdp-schema-rehearsal.XXXXXXXX)
project="cdp-schema-${work_dir##*.}"
project=${project,,}
export CDP_PORT=0 CDP_PROMETHEUS_PORT=0
export CDP_S4_CONFIG_DIR="$work_dir"

dc() {
    docker compose -p "$project" -f compose.yaml -f compose.postgres.yaml -f compose.monitoring.yaml -f compose.s4.yaml "$@"
}

cleanup() {
    local status=$?
    trap - EXIT
    if ((status != 0)); then
        dc ps -a >&2 || true
        dc logs --tail 25 prometheus alertmanager receiver >&2 || true
    fi
    dc down --volumes >/dev/null 2>&1 || true
    rm -f "$work_dir"/{prometheus.yml,alertmanager.yml,receiver.py}
    rmdir "$work_dir" || true
    exit "$status"
}
trap cleanup EXIT

# Compose mounts local configs as files. Stage readable copies for non-root services.
install -m 644 monitoring/s4-alertmanager.yml "$work_dir/alertmanager.yml"
install -m 644 scripts/s4-receiver.py "$work_dir/receiver.py"
install -m 644 monitoring/prometheus.yml "$work_dir/prometheus.yml"
cat >> "$work_dir/prometheus.yml" <<'YAML'
alerting:
  alertmanagers:
    - static_configs:
        - targets: [alertmanager:9093]
YAML

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
started=$(python3 -c "from time import monotonic; print(monotonic())")
dc exec -T api python -m cdp.collect /data/schema-fixture.sqlite3 --format billing-v1 \
    --source-id schema-rehearsal --page-size 1 || status=$?
[[ "$status" == 1 ]]
api=$(dc port api 8000)
prom=$(dc port prometheus 9090)
receiver=$(dc port receiver 8080)

wait_for_state() {
    python3 - "$api" "$prom" "$receiver" "$1" "$2" <<'PY'
import json
import sys
import time
from urllib.request import ProxyHandler, build_opener

api, prom, receiver, expected, started = sys.argv[1:]
started = float(started)
alert_name = "CdpCollectionQualityBlocked"
rule_state, receipt_state = ("firing", "firing") if expected == "blocked" else ("inactive", "resolved")
expected_failure = ("contract_validation", "billing_schema") if expected == "blocked" else (None, None)
opener = build_opener(ProxyHandler({}))

def load(target, route):
    with opener.open(f"http://{target}{route}", timeout=5) as response:
        return json.load(response)

while True:
    sources = load(api, "/v1/demo/collection-quality")["sources"]
    rules = load(prom, "/api/v1/rules")["data"]["groups"]
    alert = next(rule for group in rules for rule in group["rules"]
                 if rule["name"] == alert_name)
    receipts = load(receiver, "/receipts")
    elapsed = time.monotonic() - started
    if elapsed > 300:
        raise AssertionError(f"No {receipt_state} notification within 300 s: {sources}, {alert['state']}, {receipts}")
    if (any(
            source["source_key"] == "billing-v1:schema-rehearsal"
            and source["status"] == expected
            and (source["failure_code"], source["rule_id"]) == expected_failure
            for source in sources
        )
            and alert["state"] == rule_state
            and {"name": alert_name, "status": receipt_state} in receipts):
        print(f"Collection quality: {expected}; webhook: {receipt_state} received within {elapsed:.1f} s.")
        break
    time.sleep(2)
PY
}

wait_for_state blocked "$started"
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
started=$(python3 -c "from time import monotonic; print(monotonic())")
dc exec -T api python -m cdp.collect /data/schema-fixture.sqlite3 --format billing-v1 \
    --source-id schema-rehearsal --page-size 1 | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["outcomes"] == {"duplicate": 1, "updated": 1}, result
print(result["outcomes"])
'
wait_for_state ok "$started"
printf '%s\n' 'PASS: invalid source field blocked collection; retry recovered and alert cleared.'
