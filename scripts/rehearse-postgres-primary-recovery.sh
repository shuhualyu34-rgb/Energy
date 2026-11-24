#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

work_dir=$(mktemp -d /tmp/cdp-postgres-ha.XXXXXXXX)
project="cdp-postgres-ha-${work_dir##*.}"
project=${project,,}

dc() {
    docker compose -p "$project" -f compose.postgres-ha.yaml "$@"
}

cleanup() {
    local status=$?
    trap - EXIT
    if ((status != 0)); then
        dc ps -a >&2 || true
        dc logs --tail 30 primary standby restored >&2 || true
    fi
    dc down --volumes >/dev/null 2>&1 || true
    rmdir "$work_dir" || true
    exit "$status"
}
trap cleanup EXIT

sql() {
    local service=$1
    shift
    dc exec -T "$service" psql -XAt -U cdp -d cdp -v ON_ERROR_STOP=1 "$@"
}

wait_for_sql() {
    local service=$1 query=$2 expected=$3 deadline=$((SECONDS + 60)) actual
    until actual=$(sql "$service" -c "$query" 2>/dev/null) && [[ "$actual" == "$expected" ]]; do
        if ((SECONDS >= deadline)); then
            printf '%s did not return %s for %s (last result: %s)\n' "$service" "$expected" "$query" "$actual" >&2
            return 1
        fi
        sleep 1
    done
}

prepare_copy() {
    dc run --rm -T --no-deps --entrypoint bash "$1" -se -- "$1" <<'BASH'
test -z "$(ls -A "$PGDATA")"
cp -a /backup/base/. "$PGDATA/"
chmod 0700 "$PGDATA"
chown postgres:postgres "$PGDATA"
if [[ "$1" == standby ]]; then
    gosu postgres touch "$PGDATA/standby.signal"
    gosu postgres sh -c 'printf "%s\n" "$1" >> "$PGDATA/postgresql.auto.conf"' sh \
        "primary_conninfo = 'host=primary port=5432 user=i2_replica password=i2-replica-local-only application_name=i2_standby'"
else
    gosu postgres touch "$PGDATA/recovery.signal"
fi
BASH
}

dc up -d --wait --wait-timeout 60 primary
dc exec -T primary install -d -m 0700 -o postgres -g postgres /backup/wal /backup/base
dc exec -T -u postgres primary sh -c \
    'printf "%s\n" "host replication i2_replica all scram-sha-256" >> "$PGDATA/pg_hba.conf"'
sql primary <<'SQL'
CREATE ROLE i2_replica WITH LOGIN REPLICATION PASSWORD 'i2-replica-local-only';
SELECT pg_reload_conf();
CREATE TABLE recovery_orders (
    id integer PRIMARY KEY,
    fee_cents integer NOT NULL
);
INSERT INTO recovery_orders VALUES (1, 1100);
SQL

dc exec -T -u postgres -e PGPASSWORD=cdp-local-only primary \
    pg_basebackup -h 127.0.0.1 -U cdp -D /backup/base -X stream --checkpoint=fast --no-password
dc exec -T -u postgres primary pg_verifybackup /backup/base

prepare_copy standby
dc up -d --wait --wait-timeout 60 standby
wait_for_sql standby "SELECT pg_is_in_recovery()" t
[[ "$(sql standby -c "SELECT id, fee_cents FROM recovery_orders ORDER BY id")" == "1|1100" ]]
wait_for_sql primary "SELECT state FROM pg_stat_replication WHERE application_name = 'i2_standby'" streaming
sql primary -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (i2_standby)'"
sql primary -c "SELECT pg_reload_conf()"
wait_for_sql primary "SELECT sync_state FROM pg_stat_replication WHERE application_name = 'i2_standby'" sync

sql primary <<'SQL'
SET statement_timeout = '30s';
SET synchronous_commit = remote_apply;
INSERT INTO recovery_orders VALUES (2, 2400), (3, 3700);
SQL
last_ack=$SECONDS
expected=$(sql primary -c "SELECT id, fee_cents FROM recovery_orders ORDER BY id")
[[ "$expected" == $'1|1100\n2|2400\n3|3700' ]]
wal_segment=$(sql primary -c "SELECT pg_walfile_name(pg_current_wal_insert_lsn())")
sql primary -c "SELECT pg_switch_wal()"
deadline=$((SECONDS + 60))
until dc exec -T -u postgres primary test -s "/backup/wal/$wal_segment"; do
    if ((SECONDS >= deadline)); then
        printf 'WAL archive did not receive %s\n' "$wal_segment" >&2
        exit 1
    fi
    sleep 1
done

verify_recovery() {
    local service=$1 label=$2 started=$3 rto point_age
    wait_for_sql "$service" "SELECT pg_is_in_recovery()" f
    [[ "$(sql "$service" -c "SELECT id, fee_cents FROM recovery_orders ORDER BY id")" == "$expected" ]]
    sql "$service" -c "INSERT INTO recovery_orders VALUES (4, 4800)" >/dev/null
    [[ "$(sql "$service" -c "SELECT fee_cents FROM recovery_orders WHERE id = 4")" == 4800 ]]
    rto=$((SECONDS - started))
    point_age=$((fault_at - last_ack))
    ((rto >= 0 && rto <= 14400 && point_age >= 0 && point_age <= 900))
    printf '%s: RTO %ss; 0 acknowledged orders lost; latest verified recovery point %ss old at failure.\n' \
        "$label" "$rto" "$point_age"
}

fault_at=$SECONDS
dc kill -s SIGKILL primary
sql standby -c "SELECT pg_promote(true, 60)"
verify_recovery standby "Standby promotion" "$fault_at"

restore_started=$SECONDS
prepare_copy restored
dc up -d --wait --wait-timeout 60 restored
verify_recovery restored "Base backup and WAL restore" "$restore_started"
printf '%s\n' 'PASS: promoted standby and independent archive restore both recovered all acknowledged orders.'
