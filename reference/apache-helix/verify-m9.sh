#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
M8_SCENARIOS="$ROOT/reference/apache-helix/scenarios/m8"
M9_ETCD_SCENARIOS="$ROOT/reference/apache-helix/scenarios/m9/etcd"
JAVA_ORACLE="$ROOT/reference/apache-helix/scripts/run-java-integration-oracle.sh"
RUST_ETCD_RUNNER="$ROOT/reference/apache-helix/scripts/run-rust-etcd-integration.sh"
M8_COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m8-results.py"
M9_ETCD_COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m9-etcd-results.py"
ETCD_BIN="${CLUSTODIAN_M9_ETCD_BIN:-etcd}"
EXPECTED_ETCD_VERSION="3.7.1"

fail() {
  echo "verify-m9: ERROR: $*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || fail "cargo not found"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"
command -v "$ETCD_BIN" >/dev/null 2>&1 || fail "etcd binary not found: $ETCD_BIN"
[[ -x "$JAVA_ORACLE" ]] || fail "missing executable Java integration oracle: $JAVA_ORACLE"
[[ -x "$RUST_ETCD_RUNNER" ]] || fail "missing executable Rust etcd integration runner: $RUST_ETCD_RUNNER"
[[ -x "$M8_COMPARATOR" ]] || fail "missing executable M8 comparator: $M8_COMPARATOR"
[[ -x "$M9_ETCD_COMPARATOR" ]] || fail "missing executable M9 etcd comparator: $M9_ETCD_COMPARATOR"
[[ -x "$ROOT/reference/apache-helix/verify-m8.sh" ]] || fail "missing prerequisite verifier: reference/apache-helix/verify-m8.sh"
[[ -d "$M8_SCENARIOS" ]] || fail "missing M8 scenarios: $M8_SCENARIOS"
[[ -d "$M9_ETCD_SCENARIOS" ]] || fail "missing M9 etcd scenarios: $M9_ETCD_SCENARIOS"

actual_etcd_version="$($ETCD_BIN --version | awk -F': ' '/^etcd Version:/{print $2; exit}')"
[[ -n "$actual_etcd_version" ]] || fail "could not determine etcd version from: $ETCD_BIN --version"
[[ "$actual_etcd_version" == "$EXPECTED_ETCD_VERSION" ]] || \
  fail "M9 requires etcd $EXPECTED_ETCD_VERSION, found $actual_etcd_version"

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m9.XXXXXX")"
ETCD_PID=""
cleanup() {
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
  --name clustodian-m9 \
  --data-dir "$ETCD_DATA_DIR" \
  --listen-client-urls "$ETCD_ENDPOINT" \
  --advertise-client-urls "$ETCD_ENDPOINT" \
  --listen-peer-urls "$PEER_ENDPOINT" \
  --initial-advertise-peer-urls "$PEER_ENDPOINT" \
  --initial-cluster "clustodian-m9=$PEER_ENDPOINT" \
  --initial-cluster-state new \
  --initial-cluster-token clustodian-m9-verifier \
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
for _ in range(200):
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
        print("verify-m9: etcd exited before becoming healthy", file=sys.stderr)
        try:
            print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
        except OSError:
            pass
        raise SystemExit(1)
    time.sleep(0.05)

print("verify-m9: timed out waiting for etcd health", file=sys.stderr)
try:
    print(open(log_path, encoding="utf-8").read(), file=sys.stderr)
except OSError:
    pass
raise SystemExit(1)
PY

export CLUSTODIAN_M9_ETCD_ENDPOINT="$ETCD_ENDPOINT"

run_rust_etcd() {
  local scenario="$1"
  local prefix="$2"
  local output="$3"

  CLUSTODIAN_M9_ETCD_PREFIX="$prefix" \
    "$RUST_ETCD_RUNNER" "$scenario" > "$output"
}

echo "==> Verifying prerequisite M8" >&2
"$ROOT/reference/apache-helix/verify-m8.sh"

echo >&2
echo "==> Rust formatting" >&2
(
  cd "$ROOT"
  cargo fmt --all --check
)

echo >&2
echo "==> clustodian tests" >&2
(
  cd "$ROOT"
  cargo test -p clustodian
)

echo >&2
echo "==> clustodian-conformance tests" >&2
(
  cd "$ROOT"
  cargo test -p clustodian-conformance
)

echo >&2
echo "==> clustodian clippy" >&2
(
  cd "$ROOT"
  cargo clippy -p clustodian --all-targets --all-features -- -D warnings
)

echo >&2
echo "==> clustodian-conformance clippy" >&2
(
  cd "$ROOT"
  cargo clippy -p clustodian-conformance --all-targets --all-features -- -D warnings
)

shopt -s nullglob
m8_scenarios=("$M8_SCENARIOS"/*.json)
[[ ${#m8_scenarios[@]} -gt 0 ]] || fail "no M8 semantic scenarios found"

echo >&2
echo "==> M9 semantic preservation: Helix/ZooKeeper vs clustodian/etcd" >&2
semantic_count=0
for scenario in "${m8_scenarios[@]}"; do
  name="$(basename "$scenario" .json)"
  java_result="$TEMP_DIR/$name.java.json"
  rust_first="$TEMP_DIR/$name.etcd.1.json"
  rust_second="$TEMP_DIR/$name.etcd.2.json"

  echo "    $name" >&2
  "$JAVA_ORACLE" "$scenario" > "$java_result"
  run_rust_etcd "$scenario" "/clustodian/m9/semantic/$name/run-1" "$rust_first"
  run_rust_etcd "$scenario" "/clustodian/m9/semantic/$name/run-2" "$rust_second"

  python3 "$M8_COMPARATOR" "$rust_first" "$rust_second" || {
    echo "verify-m9: etcd-backed M8 semantics were non-deterministic for $name" >&2
    exit 1
  }
  python3 "$M8_COMPARATOR" "$java_result" "$rust_first" || {
    echo "verify-m9: Helix semantic mismatch for $name" >&2
    echo "  scenario:    $scenario" >&2
    echo "  Java result: $java_result" >&2
    echo "  Rust result: $rust_first" >&2
    exit 1
  }

  semantic_count=$((semantic_count + 1))
done

m9_cases=("$M9_ETCD_SCENARIOS"/*.json)
[[ ${#m9_cases[@]} -gt 0 ]] || fail "no M9 etcd-native scenarios found"

echo >&2
echo "==> M9 native etcd backend correctness" >&2
backend_count=0
for scenario in "${m9_cases[@]}"; do
  name="$(basename "$scenario" .json)"
  result_first="$TEMP_DIR/$name.native.1.json"
  result_second="$TEMP_DIR/$name.native.2.json"

  echo "    $name" >&2
  run_rust_etcd "$scenario" "/clustodian/m9/native/$name/run-1" "$result_first"
  run_rust_etcd "$scenario" "/clustodian/m9/native/$name/run-2" "$result_second"

  python3 "$M9_ETCD_COMPARATOR" "$scenario" "$result_first" || {
    echo "verify-m9: etcd backend case failed: $name (run 1)" >&2
    echo "  scenario: $scenario" >&2
    echo "  result:   $result_first" >&2
    exit 1
  }
  python3 "$M9_ETCD_COMPARATOR" "$scenario" "$result_second" || {
    echo "verify-m9: etcd backend case failed: $name (run 2)" >&2
    echo "  scenario: $scenario" >&2
    echo "  result:   $result_second" >&2
    exit 1
  }

  backend_count=$((backend_count + 1))
done

echo >&2
echo "verify-m9: PASS" >&2
echo "  prerequisite: M8 passed" >&2
echo "  etcd version: $actual_etcd_version" >&2
echo "  semantic scenarios: $semantic_count (Helix/ZooKeeper vs clustodian/etcd)" >&2
echo "  etcd-native scenarios: $backend_count" >&2
echo "  Helix authority: participant/session semantics" >&2
echo "  etcd authority: leases, transactional fencing, revisions, watches, compaction recovery" >&2
