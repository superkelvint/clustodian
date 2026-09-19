#!/usr/bin/env bash
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "$EXAMPLE_DIR/../.." && pwd)"
BIN="$ROOT_DIR/target/debug/ha-controllers"
export HA_ETCD_ENDPOINT="${HA_ETCD_ENDPOINT:-http://127.0.0.1:2379}"
export HA_PREFIX="${HA_PREFIX:-ha-demo-$$}"
export HA_CLUSTER="${HA_CLUSTER:-ha-control-plane}"

CALLBACK_DIR="$(mktemp -d)"
READY_FILE="$CALLBACK_DIR/authority-ready"
RELEASE_FILE="$CALLBACK_DIR/release"
OUTCOME_FILE="$CALLBACK_DIR/outcome"
PIDS=()

cleanup() {
  set +e
  for pid in "${PIDS[@]:-}"; do
    kill -CONT "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
  if [[ -z "${HA_SKIP_COMPOSE:-}" ]]; then
    docker compose -f "$EXAMPLE_DIR/docker-compose.yml" down -v >/dev/null 2>&1 || true
  fi
  rm -rf "$CALLBACK_DIR"
}
trap cleanup EXIT INT TERM

if [[ -z "${HA_SKIP_COMPOSE:-}" ]]; then
  docker compose -f "$EXAMPLE_DIR/docker-compose.yml" up -d etcd
fi
cargo build --manifest-path "$EXAMPLE_DIR/Cargo.toml"
"$BIN" setup

for id in participant-a participant-b participant-c participant-d; do
  HA_INSTANCE_ID="$id" "$BIN" participant >"$CALLBACK_DIR/$id.log" 2>&1 &
  PIDS+=("$!")
done

HA_CONTROLLER_ID=controller-1 \
  HA_STALE_CONTROLLER_READY_FILE="$READY_FILE" \
  HA_STALE_CONTROLLER_RELEASE_FILE="$RELEASE_FILE" \
  HA_STALE_CONTROLLER_OUTCOME_FILE="$OUTCOME_FILE" \
  "$BIN" controller >"$CALLBACK_DIR/controller-1.log" 2>&1 &
ACTIVE_CONTROLLER_INDEX=${#PIDS[@]}
PIDS+=("$!")
for id in controller-2 controller-3; do
  HA_CONTROLLER_ID="$id" "$BIN" controller >"$CALLBACK_DIR/$id.log" 2>&1 &
  PIDS+=("$!")
done

wait_for() {
  local description="$1"
  local command="$2"
  for _ in {1..160}; do
    if eval "$command"; then
      return 0
    fi
    sleep .25
  done
  echo "timed out waiting for $description" >&2
  return 1
}

wait_for "controller authority" "test -s '$READY_FILE'"
wait_for "initial HA convergence" \
  "'$BIN' status | python3 -c 'import json,sys; s=json.load(sys.stdin); ev=s.get(\"external_view\",{}).get(\"control-work\",{}); raise SystemExit(0 if len(s.get(\"live_instances\",{})) == 4 and len(s.get(\"controllers\",{}).get(\"active\",[])) == 1 and not s.get(\"pending_transitions\") and len(ev) == 4 and all(len(p) == 1 and \"LEADER\" in p.values() for p in ev.values()) else 1)'"
echo '=== before controller failover ==='
"$BIN" status

echo "SIGSTOP controller-1; waiting for its lease to expire"
kill -STOP "${PIDS[$ACTIVE_CONTROLLER_INDEX]}"
wait_for "controller takeover" \
  "'$BIN' status | python3 -c 'import json,sys; s=json.load(sys.stdin); a=s.get(\"controllers\",{}).get(\"active\",[]); raise SystemExit(0 if a and a[0] != \"controller-1\" else 1)'"
echo '=== after takeover ==='
"$BIN" status

echo "SIGCONT stale controller-1; releasing its fenced write attempt"
kill -CONT "${PIDS[$ACTIVE_CONTROLLER_INDEX]}"
touch "$RELEASE_FILE"
wait_for "stale write rejection" "grep -q 'rejected: stale controller authority' '$OUTCOME_FILE'"
echo "stale controller result: $(<"$OUTCOME_FILE")"
echo '=== final control-plane state ==='
"$BIN" status
