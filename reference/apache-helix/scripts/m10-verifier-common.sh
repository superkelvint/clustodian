#!/usr/bin/env bash
# Shared M10 verifier helpers. Source this file; do not execute it directly.

m10_fail() {
  echo "${M10_VERIFIER_NAME:-verify-m10}: ERROR: $*" >&2
  exit 1
}

m10_check_etcd_version() {
  local actual
  actual="$($ETCD_BIN --version | awk -F': ' '/^etcd Version:/{print $2; exit}')"
  [[ -n "$actual" ]] || m10_fail "could not determine etcd version"
  [[ "$actual" == "$EXPECTED_ETCD_VERSION" ]] || \
    m10_fail "M10 requires etcd $EXPECTED_ETCD_VERSION, found $actual"
  M10_ACTUAL_ETCD_VERSION="$actual"
}

m10_stop_runtime() {
  if [[ -n "${RUNTIME_PID:-}" ]] && kill -0 "$RUNTIME_PID" >/dev/null 2>&1; then
    kill "$RUNTIME_PID" >/dev/null 2>&1 || true
    wait "$RUNTIME_PID" >/dev/null 2>&1 || true
  fi
  RUNTIME_PID=""
}

m10_cleanup() {
  m10_stop_runtime
  if [[ -n "${ETCD_PID:-}" ]] && kill -0 "$ETCD_PID" >/dev/null 2>&1; then
    kill "$ETCD_PID" >/dev/null 2>&1 || true
    wait "$ETCD_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TEMP_DIR:-}" && -d "$TEMP_DIR" ]]; then
    rm -rf "$TEMP_DIR"
  fi
}

m10_start_etcd() {
  read -r CLIENT_PORT PEER_PORT < <(python3 - <<'PY'
import socket
s1 = socket.socket()
s2 = socket.socket()
s1.bind(("127.0.0.1", 0))
s2.bind(("127.0.0.1", 0))
print(s1.getsockname()[1], s2.getsockname()[1])
s1.close()
s2.close()
PY
  )

  local endpoint="http://127.0.0.1:${CLIENT_PORT}"
  local peer_endpoint="http://127.0.0.1:${PEER_PORT}"
  local data_dir="$TEMP_DIR/etcd-data"
  local log_file="$TEMP_DIR/etcd.log"
  mkdir -p "$data_dir"

  "$ETCD_BIN" \
    --name clustodian-m10 \
    --data-dir "$data_dir" \
    --listen-client-urls "$endpoint" \
    --advertise-client-urls "$endpoint" \
    --listen-peer-urls "$peer_endpoint" \
    --initial-advertise-peer-urls "$peer_endpoint" \
    --initial-cluster "clustodian-m10=$peer_endpoint" \
    --initial-cluster-state new \
    --initial-cluster-token "clustodian-m10-verifier-$$" \
    --log-level warn \
    >"$log_file" 2>&1 &
  ETCD_PID=$!

  python3 - "$endpoint" "$ETCD_PID" "$log_file" <<'PY'
import json
import os
import sys
import time
import urllib.error
import urllib.request

endpoint, pid_text, log_path = sys.argv[1:]
pid = int(pid_text)
url = endpoint + "/health"
for _ in range(300):
    try:
        with urllib.request.urlopen(url, timeout=0.2) as response:
            body = response.read().decode("utf-8")
            value = json.loads(body)
            if value.get("health") in (True, "true"):
                raise SystemExit(0)
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        pass
    try:
        os.kill(pid, 0)
    except OSError:
        print("verify-m10: etcd exited before becoming healthy", file=sys.stderr)
        try:
            print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
        except OSError:
            pass
        raise SystemExit(1)
    time.sleep(0.05)

print("verify-m10: timed out waiting for etcd health", file=sys.stderr)
try:
    print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
except OSError:
    pass
raise SystemExit(1)
PY

  export CLUSTODIAN_M10_ETCD_ENDPOINT="$endpoint"
}

m10_wait_for_ready() {
  local ready_file="$1"
  local log_file="$2"
  local pid="$3"

  python3 - "$ready_file" "$log_file" "$pid" "$RUNTIME_READY_TIMEOUT_SECONDS" <<'PY'
import os
import sys
import time

ready, log_path, pid_text, timeout_text = sys.argv[1:]
pid = int(pid_text)
deadline = time.monotonic() + float(timeout_text)
while time.monotonic() < deadline:
    if os.path.exists(ready):
        raise SystemExit(0)
    try:
        os.kill(pid, 0)
    except OSError:
        print("verify-m10: controller runtime exited before readiness", file=sys.stderr)
        try:
            print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
        except OSError:
            pass
        raise SystemExit(1)
    time.sleep(0.025)

print("verify-m10: timed out waiting for controller runtime readiness", file=sys.stderr)
try:
    print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
except OSError:
    pass
raise SystemExit(1)
PY
}

m10_run_rust_scenario() {
  local scenario="$1"
  local prefix="$2"
  local run_id="$3"
  local output="$4"
  local state_file="$TEMP_DIR/${run_id}.driver-state.json"
  local ready_file="$TEMP_DIR/${run_id}.ready"
  local runtime_log="$TEMP_DIR/${run_id}.runtime.log"

  rm -f "$state_file" "$ready_file" "$runtime_log"

  CLUSTODIAN_M10_ETCD_PREFIX="$prefix" \
    timeout "$SCENARIO_TIMEOUT_SECONDS" \
    "$RUST_DRIVER" prepare "$scenario" "$state_file"

  CLUSTODIAN_M10_ETCD_PREFIX="$prefix" \
  CLUSTODIAN_M10_READY_FILE="$ready_file" \
    "$RUST_RUNTIME" >"$runtime_log" 2>&1 &
  RUNTIME_PID=$!

  m10_wait_for_ready "$ready_file" "$runtime_log" "$RUNTIME_PID"

  if ! CLUSTODIAN_M10_ETCD_PREFIX="$prefix" \
      timeout "$SCENARIO_TIMEOUT_SECONDS" \
      "$RUST_DRIVER" run "$scenario" "$state_file" >"$output"; then
    echo "verify-m10: Rust scenario driver failed" >&2
    echo "  scenario: $scenario" >&2
    echo "  runtime log:" >&2
    sed -n '1,240p' "$runtime_log" >&2 || true
    m10_stop_runtime
    return 1
  fi

  if ! kill -0 "$RUNTIME_PID" >/dev/null 2>&1; then
    echo "verify-m10: controller runtime exited unexpectedly" >&2
    echo "  scenario: $scenario" >&2
    echo "  runtime log:" >&2
    sed -n '1,240p' "$runtime_log" >&2 || true
    m10_stop_runtime
    return 1
  fi

  m10_stop_runtime
}
