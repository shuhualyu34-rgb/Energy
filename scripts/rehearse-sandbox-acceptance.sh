#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if (( $# != 0 )); then
    printf 'Usage: %s\n' "${0##*/}" >&2
    exit 2
fi

passed=()
failed=()

run_rehearsal() {
    local scenarios=$1
    shift
    local status
    printf '\n[%s]\n' "$scenarios"
    if bash "$@"; then
        passed+=("$scenarios")
    else
        status=$?
        failed+=("$scenarios (exit $status)")
    fi
}

run_rehearsal 'S1 / S2 / S8' scripts/rehearse-stream-acceptance.sh
run_rehearsal S3 scripts/rehearse-collector-crash.sh
run_rehearsal S4 scripts/rehearse-source-schema.sh --local
run_rehearsal S5 scripts/rehearse-version-rollback.sh
run_rehearsal S6 scripts/rehearse-rate-limit.sh
run_rehearsal S7 scripts/rehearse-full-replay.sh
run_rehearsal S9 scripts/rehearse-tou-billing.sh

printf '\nSynthetic sandbox acceptance summary\n'
for entry in "${passed[@]}"; do
    printf 'PASS  %s\n' "$entry"
done
for entry in "${failed[@]}"; do
    printf 'FAIL  %s\n' "$entry"
done
printf '%s passed, %s failed (script groups)\n' "${#passed[@]}" "${#failed[@]}"
(( ${#failed[@]} == 0 ))
