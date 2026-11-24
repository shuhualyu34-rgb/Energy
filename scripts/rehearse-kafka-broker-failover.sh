#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

work_dir=$(mktemp -d /tmp/cdp-kafka-ha.XXXXXXXX)
project="cdp-kafka-ha-${work_dir##*.}"
project=${project,,}
bootstrap=kafka1:9092,kafka2:9092,kafka3:9092
topic=i1.synthetic.orders

dc() {
    docker compose -p "$project" -f compose.kafka-ha.yaml "$@"
}

cleanup() {
    local status=$?
    trap - EXIT
    if ((status != 0)); then
        dc ps -a >&2 || true
        dc logs --tail 25 kafka1 kafka2 kafka3 >&2 || true
    fi
    dc down --volumes >/dev/null 2>&1 || true
    rmdir "$work_dir" || true
    exit "$status"
}
trap cleanup EXIT

dc up -d --wait --wait-timeout 120 kafka1 kafka2 kafka3
dc exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$bootstrap" \
    --create --topic "$topic" --partitions 1 --replication-factor 3 --config min.insync.replicas=2

run_phase() {
    dc run --rm -T --no-deps --entrypoint python client - "$bootstrap" "$topic" "$@" <<'PY'
import json
import sys
from contextlib import closing
from time import monotonic, sleep

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

from cdp.publish import kafka_sender

bootstrap, topic, start, excluded = sys.argv[1:]
start, excluded = int(start), int(excluded)
expected_isr = {1, 2, 3} - {excluded}
batch_size = 10
end = start + batch_size

with closing(KafkaAdminClient(bootstrap_servers=bootstrap.split(","), request_timeout_ms=5000)) as admin:
    deadline = monotonic() + 60
    while True:
        partition, = admin.describe_topics([topic])[0]["partitions"]
        leader = partition["leader"]
        assert set(partition["replicas"]) == {1, 2, 3}, partition
        if set(partition["isr"]) == expected_isr and leader in expected_isr:
            break
        if monotonic() >= deadline:
            raise TimeoutError(f"Expected ISR {expected_isr}, got {partition}")
        sleep(1)

def order(number):
    key = f"synthetic-order-{number:03d}".encode()
    return key, json.dumps({"order_id": key.decode(), "sequence": number}, sort_keys=True).encode()


with kafka_sender(bootstrap, topic) as send:
    for number in range(start, end):
        assert send(*order(number)) == (0, number)

partition = TopicPartition(topic, 0)
with closing(KafkaConsumer(
    bootstrap_servers=bootstrap.split(","), group_id=None,
    enable_auto_commit=False, consumer_timeout_ms=15000,
)) as consumer:
    consumer.assign([partition])
    assert consumer.beginning_offsets([partition])[partition] == 0
    assert consumer.end_offsets([partition])[partition] == end
    consumer.seek(partition, 0)
    for number in range(end):
        message = next(consumer)
        assert (message.offset, message.key, message.value) == (number, *order(number))

print(f"ISR {sorted(expected_isr)}; acknowledged offsets {start}–{end - 1}; "
      f"verified all {end} keys and payloads", file=sys.stderr)
print(leader)
PY
}

leader=$(run_phase 0 0)
[[ "$leader" =~ ^[123]$ ]]
printf 'I1: stopping partition leader kafka%s with SIGKILL.\n' "$leader"
dc kill -s SIGKILL "kafka$leader"
run_phase 10 "$leader" >/dev/null

dc start "kafka$leader"
run_phase 20 0 >/dev/null
printf '%s\n' 'PASS: 30 acknowledged records verified across leader failure and replica recovery.'
