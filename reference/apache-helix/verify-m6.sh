#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIOS="$ROOT/reference/apache-helix/scenarios/m6"
JAVA_ORACLE="$ROOT/reference/apache-helix/scripts/run-java-oracle.sh"
COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m6-results.py"

fail() {
  echo "verify-m6: ERROR: $*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || fail "cargo not found"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"
[[ -x "$JAVA_ORACLE" ]] || fail "missing executable Java oracle: $JAVA_ORACLE"
[[ -x "$COMPARATOR" ]] || fail "missing executable comparator: $COMPARATOR"
[[ -x "$ROOT/reference/apache-helix/verify-m5.sh" ]] || fail "missing prerequisite verifier: reference/apache-helix/verify-m5.sh"
[[ -d "$SCENARIOS" ]] || fail "missing M6 scenarios: $SCENARIOS"

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m6.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT

echo "==> Verifying prerequisite M5" >&2
"$ROOT/reference/apache-helix/verify-m5.sh"

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
scenarios=("$SCENARIOS"/*.json)
if [[ ${#scenarios[@]} -eq 0 ]]; then
  fail "no M6 scenarios found under $SCENARIOS"
fi

echo >&2
echo "==> Apache Helix 2.0.1 IntermediateStateCalcStage + MessageThrottleStage differential tests" >&2

count=0
for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario" .json)"
  java_first="$TEMP_DIR/$name.java.1.json"
  java_second="$TEMP_DIR/$name.java.2.json"
  rust_first="$TEMP_DIR/$name.rust.1.json"
  rust_second="$TEMP_DIR/$name.rust.2.json"

  echo "    $name" >&2

  # Run both sides twice. For identical semantic input the supported M6 result
  # must be deterministic, independent of incidental map/message ordering.
  "$JAVA_ORACLE" "$scenario" > "$java_first"
  "$JAVA_ORACLE" "$scenario" > "$java_second"

  (
    cd "$ROOT"
    cargo run --quiet -p clustodian-conformance -- "$scenario"
  ) > "$rust_first"

  (
    cd "$ROOT"
    cargo run --quiet -p clustodian-conformance -- "$scenario"
  ) > "$rust_second"

  if ! python3 "$COMPARATOR" "$java_first" "$java_second"; then
    echo >&2
    echo "verify-m6: Apache Helix oracle was non-deterministic for $name" >&2
    exit 1
  fi

  if ! python3 "$COMPARATOR" "$rust_first" "$rust_second"; then
    echo >&2
    echo "verify-m6: clustodian was non-deterministic for $name" >&2
    exit 1
  fi

  if ! python3 "$COMPARATOR" "$java_first" "$rust_first"; then
    echo >&2
    echo "verify-m6: semantic mismatch for $name" >&2
    echo "  scenario:    $scenario" >&2
    echo "  Java result: $java_first" >&2
    echo "  Rust result: $rust_first" >&2
    exit 1
  fi

  count=$((count + 1))
done

echo >&2
echo "verify-m6: PASS" >&2
echo "  prerequisite: M5 passed" >&2
echo "  differential scenarios: $count" >&2
echo "  determinism: Java and Rust each reproduced every scenario" >&2
echo "  compared semantics: intermediate_state + dispatchable_transitions" >&2
echo "  Java authority: Apache Helix 2.0.1 IntermediateStateCalcStage + MessageThrottleStage" >&2
echo "  Rust subject: clustodian intermediate-state calculation and transition throttling" >&2
