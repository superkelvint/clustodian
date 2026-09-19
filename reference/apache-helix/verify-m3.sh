#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIOS="$ROOT/reference/apache-helix/scenarios/m3"
JAVA_ORACLE="$ROOT/reference/apache-helix/scripts/run-java-oracle.sh"
COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m3-results.py"

fail() {
  echo "verify-m3: ERROR: $*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || fail "cargo not found"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"
[[ -x "$JAVA_ORACLE" ]] || fail "missing executable Java oracle: $JAVA_ORACLE"
[[ -x "$COMPARATOR" ]] || fail "missing executable comparator: $COMPARATOR"
[[ -x "$ROOT/reference/apache-helix/verify-m2.sh" ]] || fail "missing prerequisite verifier: reference/apache-helix/verify-m2.sh"
[[ -d "$SCENARIOS" ]] || fail "missing M3 scenarios: $SCENARIOS"

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m3.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT

echo "==> Verifying prerequisite M2" >&2
"$ROOT/reference/apache-helix/verify-m2.sh"

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
  fail "no M3 scenarios found under $SCENARIOS"
fi

echo >&2
echo "==> Apache Helix 2.0.1 SEMI_AUTO differential tests" >&2

count=0
for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario" .json)"
  java_result="$TEMP_DIR/$name.java.json"
  rust_result="$TEMP_DIR/$name.rust.json"

  echo "    $name" >&2

  "$JAVA_ORACLE" "$scenario" > "$java_result"

  (
    cd "$ROOT"
    cargo run --quiet -p clustodian-conformance -- "$scenario"
  ) > "$rust_result"

  if ! python3 "$COMPARATOR" "$java_result" "$rust_result"; then
    echo >&2
    echo "verify-m3: semantic mismatch for $name" >&2
    echo "  scenario:    $scenario" >&2
    echo "  Java result: $java_result" >&2
    echo "  Rust result: $rust_result" >&2
    exit 1
  fi

  count=$((count + 1))
done

echo >&2
echo "verify-m3: PASS" >&2
echo "  prerequisite: M2 passed" >&2
echo "  differential scenarios: $count" >&2
echo "  Java authority: Apache Helix 2.0.1 SEMI_AUTO best-possible-state calculation" >&2
echo "  Rust subject: clustodian SEMI_AUTO best-possible-state calculation" >&2
