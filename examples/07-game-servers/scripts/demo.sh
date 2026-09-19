#!/usr/bin/env bash
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "$EXAMPLE_DIR/../.." && pwd)"
BIN="$ROOT_DIR/target/debug/clustodian-game-servers"
export CLUSTODIAN_ETCD_ENDPOINT="${CLUSTODIAN_ETCD_ENDPOINT:-http://127.0.0.1:23797}"
export CLUSTODIAN_GAME_CLUSTER="${CLUSTODIAN_GAME_CLUSTER:-game-demo}"
export CLUSTODIAN_GAME_PREFIX="${CLUSTODIAN_GAME_PREFIX:-clustodian-game-demo-}$$"

declare -a PIDS=()
cleanup() {
  set +e
  for pid in "${PIDS[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  docker compose -f "$EXAMPLE_DIR/docker-compose.yml" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker compose -f "$EXAMPLE_DIR/docker-compose.yml" up -d etcd
cargo build --manifest-path "$EXAMPLE_DIR/Cargo.toml"

"$BIN" admin init
"$BIN" controller >"$EXAMPLE_DIR/controller.log" 2>&1 &
PIDS+=("$!")
for instance in game-a game-b game-c; do
  "$BIN" server "$instance" >"$EXAMPLE_DIR/$instance.log" 2>&1 &
  PIDS+=("$!")
done

wait_for_leader() {
  for _ in {1..80}; do
    if "$BIN" leader game-worlds_0 >/dev/null 2>&1; then return 0; fi
    sleep 0.25
  done
  echo "timed out waiting for a game-world leader" >&2
  return 1
}

wait_for_convergence() {
  for _ in {1..120}; do
    if "$BIN" observe | python3 -c '
import json
import sys

s = json.load(sys.stdin)
ev = s.get("external_view", {}).get("game-worlds", {})
ok = bool(ev) and all(
    len(partition) == 2
    and sum(state == "LEADER" for state in partition.values()) == 1
    and sum(state == "STANDBY" for state in partition.values()) == 1
    for partition in ev.values()
)
ok = ok and not s.get("pending_transitions")
raise SystemExit(0 if ok else 1)
'
    then return 0; fi
    sleep 0.25
  done
  echo "timed out waiting for convergence" >&2
  return 1
}

wait_for_leader
wait_for_convergence
echo "=== initial ownership ==="
"$BIN" observe

leader="$("$BIN" leader game-worlds_0)"
echo "=== killing current owner $leader for game-worlds_0 ==="
case "$leader" in
  game-a) kill -KILL "${PIDS[1]}" ;;
  game-b) kill -KILL "${PIDS[2]}" ;;
  game-c) kill -KILL "${PIDS[3]}" ;;
  *) echo "unknown owner $leader" >&2; exit 1 ;;
esac
wait_for_leader
wait_for_convergence
echo "=== ownership after failover ==="
"$BIN" observe

echo "=== adding game-d ==="
"$BIN" admin add game-d zone-game-d
"$BIN" server game-d >"$EXAMPLE_DIR/game-d.log" 2>&1 &
PIDS+=("$!")
wait_for_convergence
echo "=== ownership after adding game-d ==="
"$BIN" observe

echo "=== removing game-d ==="
kill -TERM "${PIDS[4]}"
wait_for_convergence
"$BIN" admin remove game-d
wait_for_convergence
echo "=== final ownership ==="
"$BIN" observe
