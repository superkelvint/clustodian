#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIOS="$ROOT/reference/apache-helix/scenarios/m8"
JAVA_BUILD="$ROOT/reference/apache-helix/scripts/build-java-integration-oracle.sh"
JAVA_ORACLE="$ROOT/reference/apache-helix/scripts/run-java-integration-oracle.sh"
COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m8-results.py"
IMMUTABLE_MANIFEST="$ROOT/reference/apache-helix/M8-IMMUTABLE.sha256"

fail() {
  echo "verify-m8-oracle: ERROR: $*" >&2
  exit 1
}

command -v python3 >/dev/null 2>&1 || fail "python3 not found"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum not found"
[[ -x "$JAVA_BUILD" ]] || fail "missing executable Java oracle build script: $JAVA_BUILD"
[[ -x "$JAVA_ORACLE" ]] || fail "missing executable Java integration oracle: $JAVA_ORACLE"
[[ -x "$COMPARATOR" ]] || fail "missing executable comparator: $COMPARATOR"
[[ -f "$IMMUTABLE_MANIFEST" ]] || fail "missing immutable manifest: $IMMUTABLE_MANIFEST"
[[ -d "$SCENARIOS" ]] || fail "missing M8 scenarios: $SCENARIOS"

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m8-oracle.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT

echo "==> Checking immutable M8 oracle/verifier inputs" >&2
(
  cd "$ROOT"
  sha256sum -c reference/apache-helix/M8-IMMUTABLE.sha256 >&2
)

echo >&2
echo "==> Building pinned Apache Helix 2.0.1 + ZooKeeper integration oracle" >&2
"$JAVA_BUILD"

shopt -s nullglob
scenarios=("$SCENARIOS"/*.json)
[[ ${#scenarios[@]} -gt 0 ]] || fail "no M8 scenarios found under $SCENARIOS"

echo >&2
echo "==> Oracle-only M8 integration preflight" >&2
count=0
for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario" .json)"
  first="$TEMP_DIR/$name.java.1.json"
  second="$TEMP_DIR/$name.java.2.json"

  echo "    $name" >&2
  "$JAVA_ORACLE" "$scenario" > "$first"
  "$JAVA_ORACLE" "$scenario" > "$second"
  python3 "$COMPARATOR" "$first" "$second" || {
    echo "verify-m8-oracle: nondeterministic semantic result for $name" >&2
    echo "  scenario: $scenario" >&2
    echo "  run 1:    $first" >&2
    echo "  run 2:    $second" >&2
    exit 1
  }
  count=$((count + 1))
done

echo >&2
echo "verify-m8-oracle: PASS" >&2
echo "  scenarios: $count" >&2
echo "  authority: pinned Apache Helix 2.0.1 + real test ZooKeeper" >&2
echo "  checkpoint pipeline: ReadClusterDataStage -> ResourceComputationStage -> CurrentStateComputationStage" >&2
