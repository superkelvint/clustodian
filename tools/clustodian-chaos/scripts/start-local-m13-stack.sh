#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
ETCD_BIN="${CLUSTODIAN_M13_ETCD_BIN:-etcd}"
ETCDCTL_BIN="${CLUSTODIAN_M13_ETCDCTL_BIN:-etcdctl}"
RUNTIME_ENV="${CLUSTODIAN_M13_RUNTIME_ENV:-$ROOT/target/clustodian-chaos/runtime.env}"
STACK_ID="${CLUSTODIAN_M13_STACK_ID:-$$}"
STACK_ROOT="${CLUSTODIAN_M13_LOCAL_ROOT:-$ROOT/target/clustodian-chaos/local-$STACK_ID}"
PID_FILE="$STACK_ROOT/etcd.pids"
mkdir -p "$STACK_ROOT"
command -v "$ETCD_BIN" >/dev/null 2>&1 || { echo "M13 local: etcd not found: $ETCD_BIN" >&2; exit 1; }
command -v "$ETCDCTL_BIN" >/dev/null 2>&1 || { echo "M13 local: etcdctl not found: $ETCDCTL_BIN" >&2; exit 1; }
: >"$PID_FILE"

cleanup_on_failure() {
  if [[ -f "$PID_FILE" ]]; then
    while read -r pid; do
      [[ -n "$pid" ]] && kill -KILL "$pid" >/dev/null 2>&1 || true
    done <"$PID_FILE"
  fi
}
trap cleanup_on_failure ERR

CLIENT_PORTS=(23791 23792 23793)
PEER_PORTS=(23801 23802 23803)
INITIAL_CLUSTER="etcd1=http://127.0.0.1:23801,etcd2=http://127.0.0.1:23802,etcd3=http://127.0.0.1:23803"
for index in 1 2 3; do
  data_dir="$STACK_ROOT/etcd$index"
  mkdir -p "$data_dir"
  nohup "$ETCD_BIN" \
    --name="etcd$index" \
    --data-dir="$data_dir" \
    --listen-client-urls="http://127.0.0.1:${CLIENT_PORTS[$((index - 1))]}" \
    --advertise-client-urls="http://127.0.0.1:${CLIENT_PORTS[$((index - 1))]}" \
    --listen-peer-urls="http://127.0.0.1:${PEER_PORTS[$((index - 1))]}" \
    --initial-advertise-peer-urls="http://127.0.0.1:${PEER_PORTS[$((index - 1))]}" \
    --initial-cluster="$INITIAL_CLUSTER" \
    --initial-cluster-state=new \
    --initial-cluster-token="clustodian-m13-$STACK_ID" \
    --max-request-bytes=67108864 \
    --log-level=warn >"$data_dir/etcd.log" 2>&1 < /dev/null &
  echo $! >>"$PID_FILE"
done

CLIENT_ENDPOINTS="http://127.0.0.1:23791,http://127.0.0.1:23792,http://127.0.0.1:23793"
for attempt in $(seq 1 120); do
  if "$ETCDCTL_BIN" --endpoints="$CLIENT_ENDPOINTS" endpoint health --cluster >/dev/null 2>&1; then
    break
  fi
  if [[ "$attempt" == 120 ]]; then
    echo "M13 local: etcd cluster did not become healthy" >&2
    exit 1
  fi
  sleep .25
done
trap - ERR
mkdir -p "$(dirname -- "$RUNTIME_ENV")"
printf '%s\n' \
  'CLUSTODIAN_M13_RUNTIME=local' \
  "CLUSTODIAN_M13_ETCD_ENDPOINTS=$CLIENT_ENDPOINTS" \
  "CLUSTODIAN_M13_CONTROLLER_ENDPOINTS=$CLIENT_ENDPOINTS" \
  "CLUSTODIAN_M13_PARTICIPANT_ENDPOINTS=$CLIENT_ENDPOINTS" \
  "CLUSTODIAN_M13_OBSERVER_ENDPOINTS=$CLIENT_ENDPOINTS" \
  'CLUSTODIAN_M13_TOXIPROXY_URL=' \
  'CLUSTODIAN_M13_HAS_TOXIPROXY=0' \
  "CLUSTODIAN_M13_LOCAL_PID_FILE=$PID_FILE" \
  "CLUSTODIAN_M13_LOCAL_ROOT=$STACK_ROOT" \
  "CLUSTODIAN_M13_STACK_ID=$STACK_ID" \
  "CLUSTODIAN_M13_LOCAL_ETCD_RESTART_SCRIPT=$ROOT/tools/clustodian-chaos/scripts/restart-local-m13-member.sh" >"$RUNTIME_ENV"
echo "M13 runtime=local env=$RUNTIME_ENV stack=$STACK_ROOT"
