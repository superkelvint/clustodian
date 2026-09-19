#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
EXAMPLE="$ROOT/examples/02-distributed-cache"
ENDPOINT=${CACHE_ETCD_ENDPOINT:-http://127.0.0.1:2379}
PREFIX=${CACHE_PREFIX:-clustodian-cache-demo-$$}
CLUSTER=${CACHE_CLUSTER:-distributed-cache}
PORT_BASE=${CACHE_PORT_BASE:-18100}
INITIAL_NODES="node-a=127.0.0.1:$PORT_BASE,node-b=127.0.0.1:$((PORT_BASE+1)),node-c=127.0.0.1:$((PORT_BASE+2))"
NODES="$INITIAL_NODES,node-d=127.0.0.1:$((PORT_BASE+3))"
export CACHE_ETCD_ENDPOINT=$ENDPOINT CACHE_PREFIX=$PREFIX CACHE_CLUSTER=$CLUSTER CACHE_NODES=$NODES

PIDS=()
declare -A NODE_PIDS=()
cleanup() {
  trap - EXIT INT TERM
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

cargo build --manifest-path "$EXAMPLE/Cargo.toml" --quiet
BIN="$ROOT/target/debug"
"$BIN/cachectl" init "$INITIAL_NODES"

CACHE_CONTROLLER_ID=cache-controller CACHE_CONTROLLER_LEASE_TTL_MS=1500 \
  "$BIN/cache-controller" & PIDS+=("$!")
for index in 0 1 2; do
  instance="node-$(printf '%s' abc | cut -c$((index+1)))"
  CACHE_INSTANCE_ID=$instance CACHE_LISTEN="127.0.0.1:$((PORT_BASE+index))" \
    CACHE_PARTICIPANT_LEASE_TTL_MS=1500 "$BIN/cache-node" &
  PIDS+=("$!")
  NODE_PIDS[$instance]="${PIDS[${#PIDS[@]}-1]}"
done

for _ in {1..100}; do
  if "$BIN/cachectl" status 2>/dev/null | grep -q '"pending_transitions": \[\]'; then
    break
  fi
  sleep 0.1
done
"$BIN/cachectl" status
echo "PUT greeting hello -> $("$BIN/cachectl" put greeting hello)"

leader=$("$BIN/cachectl" owner greeting)
echo "current leader for greeting: $leader"
echo "killing $leader to demonstrate automatic promotion"
kill -9 "${NODE_PIDS[$leader]}"
sleep 4
echo "GET greeting -> $("$BIN/cachectl" get greeting)"

"$BIN/cachectl" add-node "node-d=127.0.0.1:$((PORT_BASE+3))"
CACHE_INSTANCE_ID=node-d CACHE_LISTEN="127.0.0.1:$((PORT_BASE+3))" \
  CACHE_PARTICIPANT_LEASE_TTL_MS=1500 "$BIN/cache-node" &
PIDS+=("$!")
NODE_PIDS[node-d]="${PIDS[${#PIDS[@]}-1]}"
sleep 3
echo "node-d joined; placement converged"
"$BIN/cachectl" status
kill "${PIDS[4]}" 2>/dev/null || true
"$BIN/cachectl" remove-node node-d
echo "node-d removed; controller will reconverge"
