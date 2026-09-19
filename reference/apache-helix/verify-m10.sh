#!/usr/bin/env bash
# Fast M10 development verifier.
#
# Deliberately does NOT start Java/ZooKeeper and does NOT run M9/fmt/test/clippy.
# It compares one Rust/etcd execution per scenario against frozen Helix 2.0.1
# goldens. Set CLUSTODIAN_M10_RUST_RUNS=2 to add Rust determinism checking.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIO_DIR="$ROOT/reference/apache-helix/scenarios/m10"
GOLDEN_DIR="$ROOT/reference/apache-helix/goldens/m10"
GOLDEN_MANIFEST="$ROOT/reference/apache-helix/M10-GOLDENS.sha256"
RUST_RUNTIME="$ROOT/reference/apache-helix/scripts/run-rust-controller-runtime.sh"
RUST_DRIVER="$ROOT/reference/apache-helix/scripts/run-rust-controller-scenario.sh"
COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m10-results.py"
COMMON="$ROOT/reference/apache-helix/scripts/m10-verifier-common.sh"
ETCD_BIN="${CLUSTODIAN_M10_ETCD_BIN:-${CLUSTODIAN_M9_ETCD_BIN:-etcd}}"
EXPECTED_ETCD_VERSION="3.7.1"
RUNTIME_READY_TIMEOUT_SECONDS="${CLUSTODIAN_M10_READY_TIMEOUT_SECONDS:-30}"
SCENARIO_TIMEOUT_SECONDS="${CLUSTODIAN_M10_SCENARIO_TIMEOUT_SECONDS:-120}"
RUST_RUNS="${CLUSTODIAN_M10_RUST_RUNS:-1}"
M10_VERIFIER_NAME="verify-m10"

# shellcheck source=/dev/null
source "$COMMON"

command -v cargo >/dev/null 2>&1 || m10_fail "cargo not found"
command -v python3 >/dev/null 2>&1 || m10_fail "python3 not found"
command -v timeout >/dev/null 2>&1 || m10_fail "timeout command not found"
command -v sha256sum >/dev/null 2>&1 || m10_fail "sha256sum not found"
command -v "$ETCD_BIN" >/dev/null 2>&1 || m10_fail "etcd binary not found: $ETCD_BIN"
[[ -x "$RUST_RUNTIME" ]] || m10_fail "missing executable Rust controller runtime: $RUST_RUNTIME"
[[ -x "$RUST_DRIVER" ]] || m10_fail "missing executable Rust controller scenario driver: $RUST_DRIVER"
[[ -x "$COMPARATOR" ]] || m10_fail "missing executable M10 comparator: $COMPARATOR"
[[ -d "$SCENARIO_DIR" ]] || m10_fail "missing M10 scenarios: $SCENARIO_DIR"
[[ -d "$GOLDEN_DIR" ]] || m10_fail "missing M10 goldens: $GOLDEN_DIR; run freeze-m10-goldens.sh"
[[ -f "$GOLDEN_MANIFEST" ]] || m10_fail "missing $GOLDEN_MANIFEST; run freeze-m10-goldens.sh"
[[ "$RUST_RUNS" =~ ^[1-9][0-9]*$ ]] || m10_fail "CLUSTODIAN_M10_RUST_RUNS must be >= 1"

# Bind immutable scenario inputs to the frozen outputs.
echo "==> Verifying M10 frozen-golden manifest" >&2
(
  cd "$ROOT"
  sha256sum -c "reference/apache-helix/M10-GOLDENS.sha256"
) >/dev/null

m10_check_etcd_version

# Build exactly once. The wrappers below execute these binaries directly.
echo "==> Building M10 Rust integration binaries once" >&2
(
  cd "$ROOT"
  cargo build --quiet -p clustodian-conformance \
    --bin controller-runtime \
    --bin controller-scenario
)

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
  if [[ "$CARGO_TARGET_DIR" = /* ]]; then
    M10_TARGET_DIR="$CARGO_TARGET_DIR"
  else
    M10_TARGET_DIR="$ROOT/$CARGO_TARGET_DIR"
  fi
else
  M10_TARGET_DIR="$ROOT/target"
fi
export CLUSTODIAN_M10_CONTROLLER_RUNTIME_BIN="$M10_TARGET_DIR/debug/controller-runtime"
export CLUSTODIAN_M10_CONTROLLER_SCENARIO_BIN="$M10_TARGET_DIR/debug/controller-scenario"
[[ -x "$CLUSTODIAN_M10_CONTROLLER_RUNTIME_BIN" ]] || m10_fail "controller-runtime binary not built"
[[ -x "$CLUSTODIAN_M10_CONTROLLER_SCENARIO_BIN" ]] || m10_fail "controller-scenario binary not built"

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m10-fast.XXXXXX")"
ETCD_PID=""
RUNTIME_PID=""
trap m10_cleanup EXIT

m10_start_etcd

shopt -s nullglob
scenarios=("$SCENARIO_DIR"/*.json)
[[ ${#scenarios[@]} -gt 0 ]] || m10_fail "no M10 scenarios found"

echo >&2
echo "==> M10 fast verification: frozen Helix goldens vs clustodian/etcd" >&2
count=0
for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario" .json)"
  golden="$GOLDEN_DIR/$name.json"
  [[ -f "$golden" ]] || m10_fail "missing golden for $name: $golden"

  echo "    $name" >&2
  first_result=""
  for ((run = 1; run <= RUST_RUNS; run++)); do
    rust_result="$TEMP_DIR/$name.rust.$run.json"
    m10_run_rust_scenario \
      "$scenario" "/clustodian/m10-fast/$name/run-$run" "$name.run-$run" "$rust_result"

    if [[ $run -eq 1 ]]; then
      first_result="$rust_result"
      python3 "$COMPARATOR" "$golden" "$rust_result" || {
        echo "verify-m10: Helix golden mismatch for $name" >&2
        echo "  scenario: $scenario" >&2
        echo "  golden:   $golden" >&2
        echo "  Rust:     $rust_result" >&2
        exit 1
      }
    else
      python3 "$COMPARATOR" "$first_result" "$rust_result" || {
        echo "verify-m10: Rust controller semantic result was non-deterministic for $name" >&2
        exit 1
      }
    fi
  done

  count=$((count + 1))
done

echo >&2
echo "verify-m10: PASS" >&2
echo "  mode: fast frozen-golden verification" >&2
echo "  etcd version: $M10_ACTUAL_ETCD_VERSION" >&2
echo "  controller scenarios: $count" >&2
echo "  Rust runs per scenario: $RUST_RUNS" >&2
echo "  Java/ZooKeeper startups: 0" >&2
echo "  reference: frozen Apache Helix 2.0.1 M10 goldens" >&2
