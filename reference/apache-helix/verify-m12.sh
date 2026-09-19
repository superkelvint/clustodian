#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
HARNESS_MANIFEST="$ROOT/reference/apache-helix/m12-harness/Cargo.toml"
SCENARIO_RUNNER="$ROOT/reference/apache-helix/scripts/run-m12-scenario.py"
INVARIANTS="$ROOT/reference/apache-helix/scripts/check-m12-invariants.py"
REPEAT_COMPARE="$ROOT/reference/apache-helix/scripts/compare-m12-repeat.py"
ELECTION_DIR="$ROOT/reference/apache-helix/scenarios/m12/election"
E2E_DIR="$ROOT/reference/apache-helix/scenarios/m12/e2e"
ETCD_BIN="${CLUSTODIAN_M12_ETCD_BIN:-etcd}"
EXPECTED_ETCD_VERSION="3.7.1"
SCENARIO_TIMEOUT="${CLUSTODIAN_M12_SCENARIO_TIMEOUT_SECONDS:-180}"
fail() { echo "verify-m12: ERROR: $*" >&2; exit 1; }
command -v cargo >/dev/null 2>&1 || fail "cargo not found"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"
command -v timeout >/dev/null 2>&1 || fail "timeout not found"
command -v "$ETCD_BIN" >/dev/null 2>&1 || fail "etcd not found: $ETCD_BIN"
[[ -x "$ROOT/reference/apache-helix/verify-m11.sh" ]] || fail "missing prerequisite verifier: reference/apache-helix/verify-m11.sh"
[[ -f "$HARNESS_MANIFEST" ]] || fail "missing immutable M12 Rust harness"
[[ -x "$SCENARIO_RUNNER" ]] || fail "missing scenario runner: $SCENARIO_RUNNER"
[[ -x "$INVARIANTS" ]] || fail "missing invariant checker: $INVARIANTS"
[[ -x "$REPEAT_COMPARE" ]] || fail "missing repeat comparator: $REPEAT_COMPARE"
actual_etcd_version="$($ETCD_BIN --version | awk -F': ' '/^etcd Version:/{print $2; exit}')"
[[ "$actual_etcd_version" == "$EXPECTED_ETCD_VERSION" ]] || fail "M12 requires etcd $EXPECTED_ETCD_VERSION, found ${actual_etcd_version:-unknown}"
echo "==> Verifying prerequisite M11" >&2
"$ROOT/reference/apache-helix/verify-m11.sh"
echo >&2
echo "==> Repository hygiene" >&2
(cd "$ROOT" && cargo fmt --all --check && cargo test --workspace --all-targets --all-features && cargo clippy --workspace --all-targets --all-features -- -D warnings)
echo >&2
echo "==> Building immutable M12 verifier harness" >&2
cargo build --quiet --manifest-path "$HARNESS_MANIFEST" --bins
HARNESS_TARGET="$(cd "$(dirname "$HARNESS_MANIFEST")" && pwd)/target/debug"
for bin in m12-controller m12-participant m12-probe m12-fence-probe; do [[ -x "$HARNESS_TARGET/$bin" ]] || fail "harness binary did not build: $bin"; done
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m12.XXXXXX")"
ETCD_PID=""
cleanup() { if [[ -n "$ETCD_PID" ]] && kill -0 "$ETCD_PID" >/dev/null 2>&1; then kill "$ETCD_PID" >/dev/null 2>&1 || true; wait "$ETCD_PID" >/dev/null 2>&1 || true; fi; rm -rf "$TMP_DIR"; }
trap cleanup EXIT
read -r CLIENT_PORT PEER_PORT < <(python3 - <<'PORTS'
import socket
ports=[]
for _ in range(2):
    s=socket.socket(); s.bind(('127.0.0.1',0)); ports.append(s.getsockname()[1]); s.close()
print(*ports)
PORTS
)
CLIENT_URL="http://127.0.0.1:$CLIENT_PORT"
PEER_URL="http://127.0.0.1:$PEER_PORT"
ETCD_LOG="$TMP_DIR/etcd.log"
"$ETCD_BIN" --name clustodian-m12 --data-dir "$TMP_DIR/etcd-data" --listen-client-urls "$CLIENT_URL" --advertise-client-urls "$CLIENT_URL" --listen-peer-urls "$PEER_URL" --initial-advertise-peer-urls "$PEER_URL" --initial-cluster "clustodian-m12=$PEER_URL" --initial-cluster-state new --initial-cluster-token clustodian-m12-verifier --log-level warn >"$ETCD_LOG" 2>&1 &
ETCD_PID=$!
python3 - "$CLIENT_URL" "$ETCD_PID" "$ETCD_LOG" <<'HEALTH'
import json, os, sys, time, urllib.request
endpoint, pid_s, log = sys.argv[1:]; pid=int(pid_s); deadline=time.monotonic()+20
while time.monotonic()<deadline:
    try:
        with urllib.request.urlopen(endpoint+'/health', timeout=.25) as r:
            if str(json.loads(r.read().decode()).get('health')).lower()=='true': raise SystemExit(0)
    except Exception: pass
    try: os.kill(pid,0)
    except OSError: break
    time.sleep(.05)
print('verify-m12: etcd failed to become healthy', file=sys.stderr)
try: print(open(log, encoding='utf-8').read(), file=sys.stderr)
except OSError: pass
raise SystemExit(1)
HEALTH
export CLUSTODIAN_M12_ETCD_ENDPOINT="$CLIENT_URL"
export CLUSTODIAN_M12_HARNESS_DIR="$HARNESS_TARGET"
run_one() {
  local scenario="$1" kind="$2" name out1 out2
  name="$(basename "$scenario" .json)"; out1="$TMP_DIR/$kind-$name.1.json"; out2="$TMP_DIR/$kind-$name.2.json"
  echo "    $kind/$name" >&2
  CLUSTODIAN_M12_ETCD_PREFIX="/clustodian/m12/$kind/$name/run-1" CLUSTODIAN_M12_WORK_DIR="$TMP_DIR/$kind-$name-run1" timeout "$SCENARIO_TIMEOUT" "$SCENARIO_RUNNER" "$scenario" >"$out1"
  "$INVARIANTS" --scenario "$scenario" --result "$out1"
  CLUSTODIAN_M12_ETCD_PREFIX="/clustodian/m12/$kind/$name/run-2" CLUSTODIAN_M12_WORK_DIR="$TMP_DIR/$kind-$name-run2" timeout "$SCENARIO_TIMEOUT" "$SCENARIO_RUNNER" "$scenario" >"$out2"
  "$INVARIANTS" --scenario "$scenario" --result "$out2"
  "$REPEAT_COMPARE" --scenario "$scenario" --left "$out1" --right "$out2"
}
mapfile -t election_scenarios < <(find "$ELECTION_DIR" -maxdepth 1 -type f -name '*.json' | sort)
mapfile -t e2e_scenarios < <(find "$E2E_DIR" -maxdepth 1 -type f -name '*.json' | sort)
[[ ${#election_scenarios[@]} -eq 8 ]] || fail "expected exactly 8 focused election scenarios"
[[ ${#e2e_scenarios[@]} -eq 3 ]] || fail "expected exactly 3 full E2E scenarios"
echo >&2; echo "==> Focused controller-election / fencing tests" >&2
for s in "${election_scenarios[@]}"; do run_one "$s" election; done
echo >&2; echo "==> Full-system application tests (M9 + M10 + M11 + M12)" >&2
for s in "${e2e_scenarios[@]}"; do run_one "$s" e2e; done
echo >&2; echo "verify-m12: PASS" >&2
echo "  prerequisite: M11 passed" >&2
echo "  etcd: $actual_etcd_version" >&2
echo "  focused election/fencing scenarios: ${#election_scenarios[@]}" >&2
echo "  full application E2E scenarios: ${#e2e_scenarios[@]}" >&2
echo "  verifier code supplied entirely under reference/apache-helix" >&2
