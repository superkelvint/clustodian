#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
EXAMPLE="$ROOT/examples/03-job-workers"
ENDPOINT=${JOB_WORKERS_ETCD_ENDPOINT:-${CLUSTODIAN_ETCD_TEST_ENDPOINT:-http://127.0.0.1:2379}}
PREFIX=${JOB_WORKERS_ETCD_PREFIX:-clustodian-job-workers-demo-$$}
export JOB_WORKERS_ETCD_ENDPOINT="$ENDPOINT" JOB_WORKERS_ETCD_PREFIX="$PREFIX"

PIDS=()
COMPOSE_STARTED=0
cleanup() {
  trap - EXIT INT TERM
  for pid in "${PIDS[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [[ "$COMPOSE_STARTED" == 1 ]]; then
    docker compose -f "$EXAMPLE/docker-compose.yml" down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

if ! command -v etcd >/dev/null && [[ -z "${CLUSTODIAN_ETCD_TEST_ENDPOINT:-}" ]]; then
  docker compose -f "$EXAMPLE/docker-compose.yml" up -d etcd
  COMPOSE_STARTED=1
fi

cargo build --manifest-path "$EXAMPLE/Cargo.toml" --quiet
BIN="$ROOT/target/debug/clustodian-job-workers"
"$BIN" setup
"$BIN" controller >"$EXAMPLE/controller.log" 2>&1 & PIDS+=("$!")
for index in 0 1 2; do
  worker="worker-$(printf '%s' abc | cut -c$((index + 1)))"
  "$BIN" worker --instance "$worker" --port "$((18200 + index))" >"$EXAMPLE/$worker.log" 2>&1 &
  PIDS+=("$!")
done

for _ in {1..120}; do
  if "$BIN" observe 2>/dev/null | grep -q '"pending_transitions": \[\]'; then break; fi
  sleep 0.25
done
"$BIN" observe
echo 'processing job-1 on worker-a'
"$BIN" process --port 18200 --partition job-queues_0 --job job-1
echo 'killing worker-a to demonstrate automatic ownership takeover'
kill -KILL "${PIDS[1]}"
sleep 4
"$BIN" observe
echo 'processing job-2 on promoted worker-b'
"$BIN" process --port 18201 --partition job-queues_0 --job job-2
