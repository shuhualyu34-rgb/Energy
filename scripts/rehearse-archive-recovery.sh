#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

work_dir=$(mktemp -d /tmp/cdp-archive-recovery.XXXXXXXX)
project="cdp-archive-recovery-${work_dir##*.}"
project=${project,,}
export CDP_PORT=0

dc() {
    docker compose -p "$project" -f compose.yaml -f compose.postgres.yaml -f compose.s3.yaml "$@"
}

cleanup() {
    local status=$?
    trap - EXIT
    if ((status != 0)); then dc ps -a >&2 || true; fi
    dc down --volumes >/dev/null 2>&1 || true
    rm -f "$work_dir/cdp.dump" "$work_dir/collection-checkpoint.json"
    rmdir "$work_dir"
    exit "$status"
}
trap cleanup EXIT

dc up -d --no-build --wait --wait-timeout 100
dc exec -T api python - <<'PY'
import os
import sqlite3
import time
from contextlib import closing
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

client = boto3.client("s3", endpoint_url=os.environ["CDP_S3_ENDPOINT_URL"],
                      config=Config(connect_timeout=2, read_timeout=3, retries={"total_max_attempts": 1}))
deadline = time.monotonic() + 60
while True:
    try:
        client.list_buckets()
        break
    except (BotoCoreError, ClientError):
        if time.monotonic() >= deadline:
            raise
        time.sleep(2)
client.create_bucket(Bucket=os.environ["CDP_S3_BUCKET"])

with closing(sqlite3.connect("/data/restore-fixture.sqlite3")) as db, db:
    db.executescript(Path("samples/billing-v1-demo.sql").read_text())
    db.execute("""
        INSERT INTO billing_v1_event
        SELECT updated_at, 'ORD_20260115_SH001_0002', version,
               json_set(payload, '$.order_id', 'ORD_20260115_SH001_0002')
        FROM billing_v1_event WHERE order_id = 'ORD_20260115_SH001_0001' AND version = 1
    """)
PY
dc exec -T api python -m cdp.collect /data/restore-fixture.sqlite3 --format billing-v1 --source-id restore-fixture
dc exec -T api python -m cdp.archive_s3 backup --all | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["version_count"] == 3 and result["results"] == {"uploaded": 3}, result
print("Object backup:", result)
'

# No background writers are present in this temporary project.
dc exec -T postgres pg_dump -U cdp -d cdp --format=custom --no-owner --no-acl > "$work_dir/cdp.dump"
test -s "$work_dir/cdp.dump"
dc exec -T api python -m cdp.collection_state export > "$work_dir/collection-checkpoint.json"
test -s "$work_dir/collection-checkpoint.json"
dc exec -T postgres createdb -U cdp cdp_recovered
dc exec -T postgres pg_restore -U cdp -d cdp_recovered --exit-on-error --no-owner --no-acl < "$work_dir/cdp.dump"

recovered() {
    dc exec -T -e CDP_POSTGRES_DSN=postgresql://cdp:cdp-local-only@postgres:5432/cdp_recovered \
       -e CDP_DATA_DIR=/data/recovered api "$@"
}
recovered python -c 'from pathlib import Path; assert not Path("/data/recovered/ods").exists()'
recovered python -m cdp.archive_s3 restore --all | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["version_count"] == 3 and result["results"] == {"restored": 3}, result
print("Object recovery:", result)
'

references() {
    dc exec -T postgres psql -X -qAt -v ON_ERROR_STOP=1 -U cdp -d "$1" -c '
        SELECT order_id, version, updated_at_utc, archive_sha256
        FROM billing_order_archive ORDER BY order_id, version, updated_at_utc'
}
test "$(references cdp)" = "$(references cdp_recovered)"
recovered python -m cdp.billing_ledger --verify-archive ORD_20260115_SH001_0001 >/dev/null
recovered python -m cdp.billing_ledger --verify-archive ORD_20260115_SH001_0002 >/dev/null
recovered python -m cdp.archive_s3 restore --all | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["results"] == {"already_local": 3}, result
'
printf '%s\n' 'PASS: PostgreSQL dump and S3 recovered two orders, three immutable archive versions.'

dc exec -T api sh -c 'cat > /tmp/collection-checkpoint.json' < "$work_dir/collection-checkpoint.json"
recovered python -m cdp.collection_state restore /tmp/collection-checkpoint.json
recovered python -m cdp.collection_state export | cmp - "$work_dir/collection-checkpoint.json"
recovered python -m cdp.collection_state restore /tmp/collection-checkpoint.json | python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result == {"status":"already_current", "source_count":1}, result
'
dc exec -T api python - <<'PYCODE'
import sqlite3
from contextlib import closing
from datetime import datetime, timezone

updated = datetime.now(timezone.utc).isoformat(timespec="microseconds")
with closing(sqlite3.connect("/data/restore-fixture.sqlite3")) as db, db:
    db.execute("""
        INSERT INTO billing_v1_event
        SELECT ?, order_id, 3, json_set(payload, '$.version', 3, '$.updated_at', ?)
        FROM billing_v1_event WHERE order_id = 'ORD_20260115_SH001_0001' AND version = 2
    """, (updated, updated))
PYCODE
recovered python -m cdp.collect /data/restore-fixture.sqlite3 --format billing-v1 --source-id restore-fixture |
    python3 -c '
import json, sys
result = json.load(sys.stdin)
assert result["outcomes"] == {"updated": 1}, result
print("Incremental resume:", result)
'
recovered python -m cdp.billing_ledger --verify-archive ORD_20260115_SH001_0001 >/dev/null
printf '%s\n' 'PASS: verified collection checkpoint restored; only the new source version was read.'
