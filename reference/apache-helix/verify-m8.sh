#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIOS="$ROOT/reference/apache-helix/scenarios/m8"
ORACLE_PREFLIGHT="$ROOT/reference/apache-helix/verify-m8-oracle.sh"
JAVA_ORACLE="$ROOT/reference/apache-helix/scripts/run-java-integration-oracle.sh"
COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m8-results.py"
IMMUTABLE_MANIFEST="$ROOT/reference/apache-helix/M8-IMMUTABLE.sha256"

fail() {
  echo "verify-m8: ERROR: $*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || fail "cargo not found"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum not found"
[[ -x "$ORACLE_PREFLIGHT" ]] || fail "missing executable M8 oracle preflight: $ORACLE_PREFLIGHT"
[[ -x "$JAVA_ORACLE" ]] || fail "missing executable Java integration oracle: $JAVA_ORACLE"
[[ -x "$COMPARATOR" ]] || fail "missing executable comparator: $COMPARATOR"
[[ -f "$IMMUTABLE_MANIFEST" ]] || fail "missing immutable manifest: $IMMUTABLE_MANIFEST"
[[ -x "$ROOT/reference/apache-helix/verify-m7.sh" ]] || fail "missing prerequisite verifier: reference/apache-helix/verify-m7.sh"
[[ -d "$SCENARIOS" ]] || fail "missing M8 scenarios: $SCENARIOS"

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m8.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT

echo "==> Checking immutable M8 oracle/verifier inputs" >&2
(
  cd "$ROOT"
  sha256sum -c reference/apache-helix/M8-IMMUTABLE.sha256 >&2
)

echo >&2
echo "==> Verifying prerequisite M7" >&2
"$ROOT/reference/apache-helix/verify-m7.sh"

echo >&2
echo "==> Verifying immutable Apache Helix 2.0.1 + ZooKeeper oracle" >&2
"$ORACLE_PREFLIGHT"

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
[[ ${#scenarios[@]} -gt 0 ]] || fail "no M8 scenarios found under $SCENARIOS"

echo >&2
echo "==> Apache Helix 2.0.1 + ZooKeeper participant/session differential tests" >&2
count=0
for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario" .json)"
  java_result="$TEMP_DIR/$name.java.json"
  rust_first="$TEMP_DIR/$name.rust.1.json"
  rust_second="$TEMP_DIR/$name.rust.2.json"

  echo "    $name" >&2
  "$JAVA_ORACLE" "$scenario" > "$java_result"

  (
    cd "$ROOT"
    cargo run --quiet -p clustodian-conformance -- "$scenario"
  ) > "$rust_first"

  (
    cd "$ROOT"
    cargo run --quiet -p clustodian-conformance -- "$scenario"
  ) > "$rust_second"

  python3 "$COMPARATOR" "$rust_first" "$rust_second" || {
    echo "verify-m8: clustodian was non-deterministic for $name" >&2
    exit 1
  }

  python3 "$COMPARATOR" "$java_result" "$rust_first" || {
    echo "verify-m8: semantic mismatch for $name" >&2
    echo "  scenario:    $scenario" >&2
    echo "  Java result: $java_result" >&2
    echo "  Rust result: $rust_first" >&2
    exit 1
  }

  count=$((count + 1))
done

echo >&2
echo "verify-m8: PASS" >&2
echo "  prerequisite: M7 passed" >&2
echo "  integration scenarios: $count" >&2
echo "  Java authority: pinned Apache Helix 2.0.1 participant/session behavior on real test ZooKeeper" >&2
echo "  real session expiry: ZkTestHelper.expireSession(participant.getZkClient())" >&2
echo "  compared semantics: LiveInstance/session identity + Helix stage-derived active CurrentState" >&2
echo "  raw ZooKeeper session IDs: canonicalized to scenario labels" >&2
echo "  Rust subject: clustodian backend-independent participant/session model" >&2
