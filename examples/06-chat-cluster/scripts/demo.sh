#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
EXAMPLE="$ROOT/examples/06-chat-cluster"
ENDPOINT=${CHAT_ETCD_ENDPOINT:-${CLUSTODIAN_ETCD_TEST_ENDPOINT:-http://127.0.0.1:2379}}
PREFIX=${CHAT_PREFIX:-clustodian-chat-demo-$$}
export CHAT_ETCD_ENDPOINT="$ENDPOINT" CHAT_PREFIX="$PREFIX" CHAT_CLUSTER=chat-demo

PIDS=()
cleanup() {
  trap - EXIT INT TERM
  for pid in "${PIDS[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

if ! command -v etcd >/dev/null && [[ -z "${CLUSTODIAN_ETCD_TEST_ENDPOINT:-}" ]]; then
  docker compose -f "$EXAMPLE/docker-compose.yml" up -d etcd
fi
cargo build --manifest-path "$EXAMPLE/Cargo.toml" --quiet
BIN="$ROOT/target/debug/chat-cluster"
"$BIN" admin
"$BIN" controller >"$EXAMPLE/controller.log" 2>&1 & PIDS+=("$!")
for index in 0 1 2; do
  port=$((18300 + index))
  CHAT_INSTANCE_ID="node-$(printf '%s' abc | cut -c$((index + 1)))" \
    CHAT_LISTEN="127.0.0.1:$port" "$BIN" participant >"$EXAMPLE/node-$index.log" 2>&1 &
  PIDS+=("$!")
done
sleep 4
"$BIN" observe
echo 'Run the integration test for the deterministic WebSocket reconnect assertion:'
echo "cargo test --manifest-path $EXAMPLE/Cargo.toml --test integration"
