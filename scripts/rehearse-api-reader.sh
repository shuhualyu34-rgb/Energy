#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
work_dir=$(mktemp -d /tmp/cdp-api-reader.XXXXXXXX)
project="cdp-api-reader-${work_dir##*.}"
project=${project,,}
export CDP_PORT=0
dc() { docker compose -p "$project" -f compose.yaml -f compose.postgres.yaml -f compose.db-access.yaml "$@"; }
cleanup() {
    local status=$?
    if ((status != 0)); then dc logs --tail 30 api db-access-init >&2 || true; fi
    dc down --volumes >/dev/null 2>&1 || true
    rmdir "$work_dir"
}
trap cleanup EXIT
dc up -d --no-build --wait --wait-timeout 90 api
dc run --rm -T --no-deps db-access-init python - <<'PY'
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from unittest.mock import patch
from cdp.billing_event import event_id
from cdp.billing_ledger import ingest_billing
from cdp.collection_quality import RejectedRecord, record_quality
from cdp.config import configured_store
from cdp.stream import observe

store = configured_store()
assert store.ingest(Path("samples/order.json").read_bytes()) == "inserted"
now = datetime.now(timezone.utc)
order_id = "ORD_20261123_SH030_3001"
payload = dict(order_id=order_id, station_id="ST_SH_030", gun_id="GUN_SH030_01A",
               user_id_hmac="a"*64, start_time=(now-timedelta(hours=2)).isoformat(),
               end_time=(now-timedelta(hours=1)).isoformat(),
               updated_at=(now-timedelta(minutes=30)).isoformat(), version=1, status="COMPLETED",
               total_kwh=10,total_amount_cents=1100,elec_fee_cents=800,service_fee_cents=300)
assert ingest_billing(store,json.dumps(payload).encode()) == "inserted"
updated = "2026-01-14T16:31:00.000000+00:00"
event = dict(schema_version="1.0", event_id=event_id(order_id,1,updated), order_id=order_id,
             version=1,updated_at=updated,status="COMPLETED",
             order=dict(station_id="ST_SH_030",end_time="2026-01-14T16:30:00Z"))
observe(store,order_id.encode(),json.dumps(event).encode())
with patch("cdp.collection_quality.datetime") as clock:
    checked_at = datetime(2026, 1, 15, 4, tzinfo=timezone.utc)
    clock.now.side_effect = [checked_at, checked_at + timedelta(seconds=1)]
    record_quality(store, "role-test", "contract_validation",
                   rejection=RejectedRecord("synthetic-key", "synthetic-record", 1100))
    record_quality(store, "role-test", None)
PY
dc exec -T api python - <<'PY'
import json
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.request import urlopen

import psycopg
from psycopg.errors import InsufficientPrivilege
from cdp.postgres import PostgresStore

assert not os.environ.get("CDP_POSTGRES_DSN")
reader = os.environ["CDP_API_POSTGRES_DSN"]
with PostgresStore(Path("/data"), reader, api_reader=True).connect() as connection:
    assert connection.execute("SHOW transaction_read_only").fetchone()["transaction_read_only"] == "on"
with psycopg.connect(reader) as connection:
    assert connection.execute("SELECT current_user").fetchone()[0] == "cdp_api_reader"
    try:
        connection.execute("UPDATE dwd_billing_order_v1_history SET status='REFUNDED'")
    except InsufficientPrivilege:
        connection.rollback()
    else:
        raise AssertionError("reader account could update the order history")
    try:
        connection.execute("CREATE TABLE public.reader_must_not_create (id integer)")
    except InsufficientPrivilege:
        connection.rollback()
    else:
        raise AssertionError("reader account could create a business table")
try:
    Path("/data/reader-must-not-write").touch()
except OSError:
    pass
else:
    raise AssertionError("API data volume was writable")

def load(path):
    with urlopen("http://127.0.0.1:8000" + path, timeout=5) as r:
        return r.read()

assert json.loads(load("/health/ready"))["status"] == "ready"
assert json.loads(load("/v1/demo/station-daily?day=2026-01-15"))["rows"][0]["order_count"] == 1
assert json.loads(load("/v1/demo/billing-events/windows?day=2026-01-14"))["rows"][0]["station_id"] == "ST_SH_030"
assert json.loads(load("/v1/demo/station-forecast"))["predictions"][0]["predicted_energy_wh"] == 10000
quality, = json.loads(load("/v1/demo/collection-quality"))["sources"]
assert quality["source_key"] == "role-test"
day = datetime.fromisoformat(quality["checked_at_utc"]).astimezone(ZoneInfo("Asia/Shanghai")).date()
report = json.loads(load(f"/v1/demo/collection-quality/daily?day={day}"))
assert (report["total_checks"], report["blocked_checks"]) == (2, 1), report
assert report["quarantine"] == {
    "distinct_orders": 1, "distinct_records": 1, "orders_with_unknown_amount": 0,
    "known_declared_amount_cents": 1100, "blocked_checks_without_evidence": 0,
}, report
blocked, recovered = json.loads(load(f"/v1/demo/collection-quality/history?day={day}&limit=2"))["rows"]
assert blocked["status"] == "blocked" and blocked["declared_amount_cents"] == 1100, blocked
assert recovered["source_key"] == "role-test" and recovered["status"] == "ok", recovered
rules = json.loads(load("/v1/demo/collection-quality/rules"))["rules"]
assert sum(rule["severity"] == "blocking" for rule in rules) == 14, rules
audit = json.loads(load("/v1/demo/collection-quality/audit"))["results"]
assert {row["rule_id"]: row["count"] for row in audit}["accepted_versions_pending_kafka"] == 1, audit
assert b"cdp_stream_observed_events 1" in load("/metrics")
try:
    PostgresStore(Path("/data"),"postgresql://cdp:cdp-local-only@postgres:5432/cdp").verify_api_reader()
except ValueError:
    pass
else:
    raise AssertionError("owner account passed the API reader check")
print("API routes verified with a PostgreSQL reader; business DML, schema DDL and archive writes refused.")
PY

set_quality_grant() {
    dc exec -T postgres psql -U cdp -d cdp -v ON_ERROR_STOP=1 -c "$1"
}

readiness_status() {
    dc exec -T api python - <<'PYTHON'
from urllib.error import HTTPError
from urllib.request import urlopen

try:
    with urlopen("http://127.0.0.1:8000/health/ready", timeout=5) as response:
        print(response.status)
except HTTPError as error:
    print(error.code)
PYTHON
}

set_quality_grant 'REVOKE SELECT ON public.billing_collection_quality FROM cdp_api_reader'
test "$(readiness_status)" = 503
set_quality_grant 'GRANT SELECT ON public.billing_collection_quality TO cdp_api_reader'
test "$(readiness_status)" = 200
echo "API readiness reported 503 after a required grant was revoked and 200 after it was restored."
