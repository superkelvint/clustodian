#!/usr/bin/env bash
set -euo pipefail

MEMBER="${1:-}"
case "$MEMBER" in
  etcd1) INDEX=0;;
  etcd2) INDEX=1;;
  etcd3) INDEX=2;;
  *) echo "M13 local: unknown etcd member: $MEMBER" >&2; exit 2;;
esac

STACK_ROOT="${CLUSTODIAN_M13_LOCAL_ROOT:?CLUSTODIAN_M13_LOCAL_ROOT is required}"
PID_FILE="${CLUSTODIAN_M13_LOCAL_PID_FILE:?CLUSTODIAN_M13_LOCAL_PID_FILE is required}"
ETCD_BIN="${CLUSTODIAN_M13_ETCD_BIN:-etcd}"
CLIENT_PORTS=(23791 23792 23793)
PEER_PORTS=(23801 23802 23803)
INITIAL_CLUSTER="etcd1=http://127.0.0.1:23801,etcd2=http://127.0.0.1:23802,etcd3=http://127.0.0.1:23803"

mapfile -t PIDS <"$PID_FILE"
OLD_PID="${PIDS[$INDEX]:-}"
if [[ -n "$OLD_PID" ]]; then
  kill -TERM "$OLD_PID" >/dev/null 2>&1 || true
  for _ in $(seq 1 100); do
    kill -0 "$OLD_PID" >/dev/null 2>&1 || break
    sleep .05
  done
  kill -KILL "$OLD_PID" >/dev/null 2>&1 || true
fi

DATA_DIR="$STACK_ROOT/$MEMBER"
nohup "$ETCD_BIN" \
  --name="$MEMBER" \
  --data-dir="$DATA_DIR" \
  --listen-client-urls="http://127.0.0.1:${CLIENT_PORTS[$INDEX]}" \
  --advertise-client-urls="http://127.0.0.1:${CLIENT_PORTS[$INDEX]}" \
  --listen-peer-urls="http://127.0.0.1:${PEER_PORTS[$INDEX]}" \
  --initial-advertise-peer-urls="http://127.0.0.1:${PEER_PORTS[$INDEX]}" \
  --initial-cluster="$INITIAL_CLUSTER" \
  --initial-cluster-state=existing \
  --initial-cluster-token="clustodian-m13-${CLUSTODIAN_M13_STACK_ID:-local}" \
  --log-level=warn >>"$DATA_DIR/etcd.log" 2>&1 < /dev/null &
PIDS[$INDEX]=$!

tmp_file="$PID_FILE.tmp.$$"
printf '%s\n' "${PIDS[@]}" >"$tmp_file"
mv -- "$tmp_file" "$PID_FILE"
