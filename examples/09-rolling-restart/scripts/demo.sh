#!/usr/bin/env bash
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "$EXAMPLE_DIR/../.." && pwd)"
BIN="$ROOT_DIR/target/debug/rolling-restart"
export ROLLING_ETCD_ENDPOINT="${ROLLING_ETCD_ENDPOINT:-http://127.0.0.1:23797}"
export ROLLING_CLUSTER="${ROLLING_CLUSTER:-rolling-restart-demo}"
export ROLLING_PREFIX="${ROLLING_PREFIX:-clustodian-rolling-restart-demo-$$}"
CALLBACK_DIR="$(mktemp -d)"
export ROLLING_CALLBACK_CONTROL="$CALLBACK_DIR/control"
export ROLLING_CALLBACK_CONTROL_INSTANCE=state-b
export ROLLING_CALLBACK_RELEASE="$CALLBACK_DIR/release"
export ROLLING_CALLBACK_HITS="$CALLBACK_DIR/hits"

declare -a PIDS=()
cleanup() {
  set +e
  for pid in "${PIDS[@]}"; do kill -CONT "$pid" 2>/dev/null || true; kill -TERM "$pid" 2>/dev/null || true; done
  sleep 0.2
  for pid in "${PIDS[@]}"; do kill -KILL "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  docker compose -f "$EXAMPLE_DIR/docker-compose.yml" down -v >/dev/null 2>&1 || true
  rm -rf "$CALLBACK_DIR"
}
trap cleanup EXIT INT TERM

docker compose -f "$EXAMPLE_DIR/docker-compose.yml" up -d etcd
cargo build --manifest-path "$EXAMPLE_DIR/Cargo.toml"
"$BIN" setup
for controller in controller-a controller-b controller-c; do
  "$BIN" controller "$controller" >"$CALLBACK_DIR/$controller.log" 2>&1 & PIDS+=("$!")
done
for instance in state-a state-b state-c; do
  "$BIN" participant "$instance" >"$CALLBACK_DIR/$instance.log" 2>&1 & PIDS+=("$!")
done

wait_settled() {
  for _ in {1..160}; do
    if "$BIN" observe | python3 -c '
import json
import sys

s = json.load(sys.stdin)
ev = s.get("external_view", {}).get("ledger", {})
ok = bool(ev) and all(
    len(partition) == 3
    and list(partition.values()).count("LEADER") == 1
    and list(partition.values()).count("STANDBY") == 2
    for partition in ev.values()
)
ok = ok and not s.get("pending_transitions")
raise SystemExit(0 if ok else 1)
'
    then return 0; fi
    sleep .25
  done
  echo "timed out waiting for convergence" >&2
  return 1
}

wait_settled
echo '=== before rolling restart ==='
"$BIN" observe
old_session="$($BIN session state-b)"
echo "state-b old SessionId=$old_session"

touch "$ROLLING_CALLBACK_CONTROL"
"$BIN" inject-stale state-b "$old_session" rolling-callback
for _ in {1..100}; do
  grep -q 'phase=started message=rolling-callback' "$ROLLING_CALLBACK_HITS" 2>/dev/null && break
  sleep .1
done
kill -STOP "${PIDS[4]}"
sleep 3
env -u ROLLING_CALLBACK_CONTROL "$BIN" participant state-b >"$CALLBACK_DIR/state-b-replacement.log" 2>&1 & PIDS+=("$!")
for _ in {1..120}; do
  new_session="$($BIN session state-b)"
  [[ "$new_session" != none && "$new_session" -gt "$old_session" ]] && break
  sleep .25
done
echo "state-b replacement SessionId=$new_session (old metadata retained)"
"$BIN" inject-stale state-b "$old_session" stale-after-restart
wait_settled
touch "$ROLLING_CALLBACK_RELEASE"
kill -CONT "${PIDS[4]}"
sleep 1
echo '=== after stale callback and session fencing ==='
"$BIN" observe
echo 'callback evidence:'
cat "$ROLLING_CALLBACK_HITS"
