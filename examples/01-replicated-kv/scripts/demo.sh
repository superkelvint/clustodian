#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENDPOINT="${CLUSTODIAN_KV_ETCD_ENDPOINT:-http://127.0.0.1:2379}"
PREFIX="${CLUSTODIAN_KV_PREFIX:-replicated-kv-demo-$$}"
export CLUSTODIAN_KV_ETCD_ENDPOINT="$ENDPOINT" CLUSTODIAN_KV_PREFIX="$PREFIX" CLUSTODIAN_KV_CLUSTER=replicated-kv
export CLUSTODIAN_KV_PEERS="node-a=127.0.0.1:18080,node-b=127.0.0.1:18081,node-c=127.0.0.1:18082"

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]:-}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

if [[ -z "${CLUSTODIAN_KV_SKIP_COMPOSE:-}" ]]; then
  docker compose -f "$DIR/docker-compose.yml" up -d etcd
  trap 'docker compose -f "$DIR/docker-compose.yml" down -v >/dev/null 2>&1 || true; cleanup' EXIT
fi

cargo run --manifest-path "$DIR/Cargo.toml" --bin replicated-kv -- setup
CLUSTODIAN_KV_CONTROLLER_ID=controller-1 cargo run --manifest-path "$DIR/Cargo.toml" --bin replicated-kv -- controller &
PIDS+=("$!")
for spec in "node-a 18080" "node-b 18081" "node-c 18082"; do
  read -r name port <<<"$spec"
  CLUSTODIAN_KV_INSTANCE_ID="$name" CLUSTODIAN_KV_LISTEN="127.0.0.1:$port" cargo run --manifest-path "$DIR/Cargo.toml" --bin replicated-kv -- node &
  PIDS+=("$!")
done

sleep 5
cargo run --manifest-path "$DIR/Cargo.toml" --bin replicated-kv -- status
echo "PUT greeting hello (leader is node-a with this fixed preference list)"
CLUSTODIAN_KV_ADDRESS=127.0.0.1:18080 cargo run --manifest-path "$DIR/Cargo.toml" --bin replicated-kv -- client PUT greeting hello
echo "GET greeting from all three replicas"
for port in 18080 18081 18082; do CLUSTODIAN_KV_ADDRESS="127.0.0.1:$port" cargo run --manifest-path "$DIR/Cargo.toml" --bin replicated-kv -- client GET greeting; done

echo "Killing node-a; waiting for its lease to expire and node-b promotion"
kill -KILL "${PIDS[1]}"
sleep 5
cargo run --manifest-path "$DIR/Cargo.toml" --bin replicated-kv -- status
echo "GET greeting through promoted node-b"
CLUSTODIAN_KV_ADDRESS=127.0.0.1:18081 cargo run --manifest-path "$DIR/Cargo.toml" --bin replicated-kv -- client GET greeting
