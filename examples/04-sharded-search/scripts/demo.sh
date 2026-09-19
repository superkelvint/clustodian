#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
MANIFEST="$ROOT_DIR/examples/04-sharded-search/Cargo.toml"
ENDPOINT="${CLUSTODIAN_SEARCH_ETCD_ENDPOINT:-http://127.0.0.1:2379}"
PREFIX="${CLUSTODIAN_SEARCH_PREFIX:-sharded-search-demo-$$}"
CLUSTER="sharded-search"
PIDS=()

cleanup() {
  trap - EXIT INT TERM
  for pid in "${PIDS[@]:-}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

run() {
  CLUSTODIAN_SEARCH_ETCD_ENDPOINT="$ENDPOINT" \
  CLUSTODIAN_SEARCH_PREFIX="$PREFIX" \
  CLUSTODIAN_SEARCH_CLUSTER="$CLUSTER" \
    cargo run --quiet --manifest-path "$MANIFEST" --bin sharded-search -- "$@"
}

echo "Configuring 24-partition CRUSH search cluster in $PREFIX"
run setup

run controller >"$ROOT_DIR/.sharded-search-controller.log" 2>&1 &
PIDS+=("$!")
for instance in node-a node-b node-c; do
  CLUSTODIAN_SEARCH_INSTANCE_ID="$instance" \
  CLUSTODIAN_SEARCH_PARTICIPANT_LEASE_TTL_MS=2500 \
  CLUSTODIAN_SEARCH_ETCD_ENDPOINT="$ENDPOINT" \
  CLUSTODIAN_SEARCH_PREFIX="$PREFIX" \
  CLUSTODIAN_SEARCH_CLUSTER="$CLUSTER" \
    cargo run --quiet --manifest-path "$MANIFEST" --bin sharded-search -- participant \
      >"$ROOT_DIR/.sharded-search-$instance.log" 2>&1 &
  PIDS+=("$!")
done

run wait 3
echo "Initial settled placement:"
run status

echo "Adding node-d; CRUSH will rebalance live partitions"
run add-node node-d >/dev/null
CLUSTODIAN_SEARCH_INSTANCE_ID=node-d \
CLUSTODIAN_SEARCH_ETCD_ENDPOINT="$ENDPOINT" \
CLUSTODIAN_SEARCH_PREFIX="$PREFIX" \
CLUSTODIAN_SEARCH_CLUSTER="$CLUSTER" \
  cargo run --quiet --manifest-path "$MANIFEST" --bin sharded-search -- participant \
  >"$ROOT_DIR/.sharded-search-node-d.log" 2>&1 &
PIDS+=("$!")
run wait 4
run status

echo "Stopping node-d and removing its configuration"
kill -TERM "${PIDS[-1]}" 2>/dev/null || true
wait "${PIDS[-1]}" 2>/dev/null || true
unset 'PIDS[-1]'
run remove-node node-d >/dev/null
run wait 3
echo "Final reconverged placement:"
run status
