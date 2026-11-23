#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
export CDP_POSTGRES_DSN=''
export CDP_DATA_DIR="$work_dir/source"

python - "$work_dir" <<'PY'
import json
import sys
from pathlib import Path

directory = Path(sys.argv[1])
sample = json.loads(Path("samples/order.json").read_text())
before_midnight = sample | {
    "order_id": "demo-order-before-midnight",
    "station_id": "demo-station-002",
    "ended_at": "2026-01-14T23:59:59+08:00",
    "energy_wh": 500,
    "electricity_fee_cents": 40,
    "service_fee_cents": 10,
}
correction = sample | {"version": 2, "service_fee_cents": 400}
references = {
    "2026-01-14": {
        "station_id": "demo-station-002",
        "order_count": 1,
        "energy_wh": 500,
        "electricity_fee_cents": 40,
        "service_fee_cents": 10,
    },
    "2026-01-15": {
        "station_id": "demo-station-001",
        "order_count": 1,
        "energy_wh": 10000,
        "electricity_fee_cents": 800,
        "service_fee_cents": 400,
    },
}
files = {"before-midnight": before_midnight, "correction": correction}
for day, totals in references.items():
    files[day] = {"business_day": day, "rows": [totals]}
for name, payload in files.items():
    (directory / f"{name}.json").write_text(json.dumps(payload))
PY

outcomes=$(python -m cdp.cli samples/order.json "$work_dir/before-midnight.json" \
    "$work_dir/correction.json" "$work_dir/correction.json" samples/order.json)
test "$outcomes" = $'inserted\ninserted\nupdated\nduplicate\nstale'

days=(2026-01-14 2026-01-15)
for day in "${days[@]}"; do
    python -m cdp.reconcile "$work_dir/$day.json" > "$work_dir/source-$day.json"
done

mismatch_status=0
python -m cdp.reconcile samples/reference-day.json > "$work_dir/mismatch.json" || mismatch_status=$?
test "$mismatch_status" -eq 2

python -m cdp.cli --export-archive "$work_dir/archive"
archives=("$work_dir"/archive/*.json)
test "${#archives[@]}" -eq 2

for run in 1 2 3; do
    export CDP_DATA_DIR="$work_dir/replay-$run"
    python -m cdp.cli "${archives[@]}" > /dev/null
    for day in "${days[@]}"; do
        python -m cdp.reconcile "$work_dir/$day.json" > "$work_dir/replay-$run-$day.json"
        cmp "$work_dir/source-$day.json" "$work_dir/replay-$run-$day.json"
    done
    echo "Replay $run: both Shanghai business days match the reference."
done

echo 'Verified cross-midnight attribution, corrections, duplicate and stale delivery, reconciliation mismatch and three clean replays.'
