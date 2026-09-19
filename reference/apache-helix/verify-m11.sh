#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIO_DIR="$ROOT/reference/apache-helix/scenarios/m11"
JAVA_ORACLE="$ROOT/reference/apache-helix/scripts/run-java-m11-integration-oracle.sh"
RUST_RUNTIME="$ROOT/reference/apache-helix/scripts/run-rust-participant-runtime.sh"
RUST_DRIVER="$ROOT/reference/apache-helix/scripts/run-rust-participant-scenario.sh"
COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m11-results.py"
ETCD_BIN="${CLUSTODIAN_M11_ETCD_BIN:-${CLUSTODIAN_M10_ETCD_BIN:-${CLUSTODIAN_M9_ETCD_BIN:-etcd}}}"
EXPECTED_ETCD_VERSION="3.7.1"
RUNTIME_READY_TIMEOUT_SECONDS="${CLUSTODIAN_M11_READY_TIMEOUT_SECONDS:-30}"
SCENARIO_TIMEOUT_SECONDS="${CLUSTODIAN_M11_SCENARIO_TIMEOUT_SECONDS:-120}"

fail() {
  echo "verify-m11: ERROR: $*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || fail "cargo not found"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"
command -v timeout >/dev/null 2>&1 || fail "timeout command not found"
command -v "$ETCD_BIN" >/dev/null 2>&1 || fail "etcd binary not found: $ETCD_BIN"
[[ -x "$JAVA_ORACLE" ]] || fail "missing executable Java M11 integration oracle: $JAVA_ORACLE"
[[ -x "$RUST_RUNTIME" ]] || fail "missing executable Rust participant runtime: $RUST_RUNTIME"
[[ -x "$RUST_DRIVER" ]] || fail "missing executable Rust participant scenario driver: $RUST_DRIVER"
[[ -x "$COMPARATOR" ]] || fail "missing executable M11 comparator: $COMPARATOR"
[[ -x "$ROOT/reference/apache-helix/verify-m10.sh" ]] || fail "missing prerequisite verifier: reference/apache-helix/verify-m10.sh"
[[ -d "$SCENARIO_DIR" ]] || fail "missing M11 scenarios: $SCENARIO_DIR"

actual_etcd_version="$($ETCD_BIN --version | awk -F': ' '/^etcd Version:/{print $2; exit}')"
[[ -n "$actual_etcd_version" ]] || fail "could not determine etcd version"
[[ "$actual_etcd_version" == "$EXPECTED_ETCD_VERSION" ]] || \
  fail "M11 requires etcd $EXPECTED_ETCD_VERSION, found $actual_etcd_version"

echo "==> Verifying prerequisite M10" >&2
"$ROOT/reference/apache-helix/verify-m10.sh"

echo >&2
echo "==> Rust formatting" >&2
(
  cd "$ROOT"
  cargo fmt --all --check
)

echo >&2
echo "==> workspace tests" >&2
(
  cd "$ROOT"
  cargo test --workspace --all-targets --all-features
)

echo >&2
echo "==> workspace clippy" >&2
(
  cd "$ROOT"
  cargo clippy --workspace --all-targets --all-features -- -D warnings
)

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m11.XXXXXX")"
ETCD_PID=""
RUNTIME_PID=""

stop_runtime() {
  if [[ -n "$RUNTIME_PID" ]] && kill -0 "$RUNTIME_PID" >/dev/null 2>&1; then
    kill "$RUNTIME_PID" >/dev/null 2>&1 || true
    wait "$RUNTIME_PID" >/dev/null 2>&1 || true
  fi
  RUNTIME_PID=""
}

cleanup() {
  stop_runtime
  if [[ -n "$ETCD_PID" ]] && kill -0 "$ETCD_PID" >/dev/null 2>&1; then
    kill "$ETCD_PID" >/dev/null 2>&1 || true
    wait "$ETCD_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

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

ETCD_ENDPOINT="http://127.0.0.1:${CLIENT_PORT}"
PEER_ENDPOINT="http://127.0.0.1:${PEER_PORT}"
ETCD_DATA_DIR="$TEMP_DIR/etcd-data"
ETCD_LOG="$TEMP_DIR/etcd.log"
mkdir -p "$ETCD_DATA_DIR"

"$ETCD_BIN" \
  --name clustodian-m11 \
  --data-dir "$ETCD_DATA_DIR" \
  --listen-client-urls "$ETCD_ENDPOINT" \
  --advertise-client-urls "$ETCD_ENDPOINT" \
  --listen-peer-urls "$PEER_ENDPOINT" \
  --initial-advertise-peer-urls "$PEER_ENDPOINT" \
  --initial-cluster "clustodian-m11=$PEER_ENDPOINT" \
  --initial-cluster-state new \
  --initial-cluster-token clustodian-m11-verifier \
  --log-level warn \
  >"$ETCD_LOG" 2>&1 &
ETCD_PID=$!

python3 - "$ETCD_ENDPOINT" "$ETCD_PID" "$ETCD_LOG" <<'PY'
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
            value = json.loads(response.read().decode("utf-8"))
            if value.get("health") in (True, "true"):
                raise SystemExit(0)
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        pass
    try:
        os.kill(pid, 0)
    except OSError:
        print("verify-m11: etcd exited before becoming healthy", file=sys.stderr)
        try:
            print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
        except OSError:
            pass
        raise SystemExit(1)
    time.sleep(0.05)
print("verify-m11: timed out waiting for etcd health", file=sys.stderr)
try:
    print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
except OSError:
    pass
raise SystemExit(1)
PY

export CLUSTODIAN_M11_ETCD_ENDPOINT="$ETCD_ENDPOINT"

wait_for_ready() {
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
        print("verify-m11: participant runtime exited before readiness", file=sys.stderr)
        try:
            print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
        except OSError:
            pass
        raise SystemExit(1)
    time.sleep(0.05)
print("verify-m11: timed out waiting for participant runtime readiness", file=sys.stderr)
try:
    print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
except OSError:
    pass
raise SystemExit(1)
PY
}

run_rust_scenario() {
  local scenario="$1"
  local prefix="$2"
  local run_id="$3"
  local output="$4"
  local state_file="$TEMP_DIR/${run_id}.driver-state.json"
  local ready_file="$TEMP_DIR/${run_id}.ready"
  local event_file="$TEMP_DIR/${run_id}.events.jsonl"
  local control_dir="$TEMP_DIR/${run_id}.control"
  local runtime_log="$TEMP_DIR/${run_id}.runtime.log"

  rm -f "$state_file" "$ready_file" "$event_file" "$runtime_log"
  rm -rf "$control_dir"
  mkdir -p "$control_dir"

  CLUSTODIAN_M11_ETCD_PREFIX="$prefix" \
  CLUSTODIAN_M11_EVENT_FILE="$event_file" \
  CLUSTODIAN_M11_CONTROL_DIR="$control_dir" \
    timeout "$SCENARIO_TIMEOUT_SECONDS" \
    "$RUST_DRIVER" prepare "$scenario" "$state_file"

  CLUSTODIAN_M11_ETCD_PREFIX="$prefix" \
  CLUSTODIAN_M11_READY_FILE="$ready_file" \
  CLUSTODIAN_M11_EVENT_FILE="$event_file" \
  CLUSTODIAN_M11_CONTROL_DIR="$control_dir" \
    "$RUST_RUNTIME" "$scenario" >"$runtime_log" 2>&1 &
  RUNTIME_PID=$!

  wait_for_ready "$ready_file" "$runtime_log" "$RUNTIME_PID"

  if ! CLUSTODIAN_M11_ETCD_PREFIX="$prefix" \
      CLUSTODIAN_M11_EVENT_FILE="$event_file" \
      CLUSTODIAN_M11_CONTROL_DIR="$control_dir" \
      timeout "$SCENARIO_TIMEOUT_SECONDS" \
      "$RUST_DRIVER" run "$scenario" "$state_file" >"$output"; then
    echo "verify-m11: Rust participant scenario driver failed" >&2
    echo "  scenario: $scenario" >&2
    echo "  runtime log:" >&2
    sed -n '1,260p' "$runtime_log" >&2 || true
    stop_runtime
    return 1
  fi

  if ! kill -0 "$RUNTIME_PID" >/dev/null 2>&1; then
    echo "verify-m11: participant runtime exited unexpectedly" >&2
    echo "  scenario: $scenario" >&2
    echo "  runtime log:" >&2
    sed -n '1,260p' "$runtime_log" >&2 || true
    stop_runtime
    return 1
  fi

  stop_runtime
}

shopt -s nullglob
scenarios=("$SCENARIO_DIR"/*.json)
[[ ${#scenarios[@]} -gt 0 ]] || fail "no M11 scenarios found"

echo >&2
echo "==> M11 participant runtime: Helix/ZooKeeper vs clustodian/etcd" >&2
count=0
for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario" .json)"
  java_first="$TEMP_DIR/$name.java.1.json"
  java_second="$TEMP_DIR/$name.java.2.json"
  rust_first="$TEMP_DIR/$name.rust.1.json"
  rust_second="$TEMP_DIR/$name.rust.2.json"

  echo "    $name" >&2

  timeout "$SCENARIO_TIMEOUT_SECONDS" "$JAVA_ORACLE" "$scenario" >"$java_first"
  timeout "$SCENARIO_TIMEOUT_SECONDS" "$JAVA_ORACLE" "$scenario" >"$java_second"

  python3 "$COMPARATOR" "$java_first" "$java_second" || {
    echo "verify-m11: Java/Helix participant result was non-deterministic for $name" >&2
    exit 1
  }

  run_rust_scenario \
    "$scenario" "/clustodian/m11/$name/run-1" "$name.run-1" "$rust_first"
  run_rust_scenario \
    "$scenario" "/clustodian/m11/$name/run-2" "$name.run-2" "$rust_second"

  python3 "$COMPARATOR" "$rust_first" "$rust_second" || {
    echo "verify-m11: Rust participant semantic result was non-deterministic for $name" >&2
    exit 1
  }

  python3 "$COMPARATOR" "$java_first" "$rust_first" || {
    echo "verify-m11: Helix participant semantic mismatch for $name" >&2
    echo "  scenario: $scenario" >&2
    echo "  Java result: $java_first" >&2
    echo "  Rust result: $rust_first" >&2
    exit 1
  }

  count=$((count + 1))
done

echo >&2
echo "verify-m11: PASS" >&2
echo "  prerequisite: M10 passed" >&2
echo "  etcd version: $actual_etcd_version" >&2
echo "  participant scenarios: $count" >&2
echo "  Java authority: Apache Helix 2.0.1 participant + ZooKeeper" >&2
echo "  Rust runtime: clustodian ParticipantRuntime + etcd" >&2
echo "  compared: session/liveness, active CurrentState, pending messages, application handler invocations" >&2
