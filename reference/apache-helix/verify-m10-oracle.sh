#!/usr/bin/env bash
# Slow provenance check: recompute every M10 result from real Helix/ZooKeeper
# and compare it with the frozen golden. Run this when the oracle/scenarios/
# pinned Helix version change, and before milestone/release signoff.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIO_DIR="$ROOT/reference/apache-helix/scenarios/m10"
GOLDEN_DIR="$ROOT/reference/apache-helix/goldens/m10"
GOLDEN_MANIFEST="$ROOT/reference/apache-helix/M10-GOLDENS.sha256"
JAVA_ORACLE="$ROOT/reference/apache-helix/scripts/run-java-m10-integration-oracle.sh"
COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m10-results.py"
SCENARIO_TIMEOUT_SECONDS="${CLUSTODIAN_M10_SCENARIO_TIMEOUT_SECONDS:-120}"
ORACLE_RUNS="${CLUSTODIAN_M10_ORACLE_RUNS:-1}"

fail() {
  echo "verify-m10-oracle: ERROR: $*" >&2
  exit 1
}

command -v python3 >/dev/null 2>&1 || fail "python3 not found"
command -v timeout >/dev/null 2>&1 || fail "timeout command not found"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum not found"
[[ -x "$JAVA_ORACLE" ]] || fail "missing executable Java M10 oracle: $JAVA_ORACLE"
[[ -x "$COMPARATOR" ]] || fail "missing executable M10 comparator: $COMPARATOR"
[[ -d "$SCENARIO_DIR" ]] || fail "missing M10 scenarios: $SCENARIO_DIR"
[[ -d "$GOLDEN_DIR" ]] || fail "missing M10 goldens: $GOLDEN_DIR"
[[ -f "$GOLDEN_MANIFEST" ]] || fail "missing M10 golden manifest: $GOLDEN_MANIFEST"
[[ "$ORACLE_RUNS" =~ ^[1-9][0-9]*$ ]] || fail "CLUSTODIAN_M10_ORACLE_RUNS must be >= 1"

(
  cd "$ROOT"
  sha256sum -c "reference/apache-helix/M10-GOLDENS.sha256"
) >/dev/null

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m10-oracle.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT

shopt -s nullglob
scenarios=("$SCENARIO_DIR"/*.json)
[[ ${#scenarios[@]} -gt 0 ]] || fail "no M10 scenarios found"

echo "==> M10 live oracle verification: Helix/ZooKeeper vs frozen goldens" >&2
count=0
for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario" .json)"
  golden="$GOLDEN_DIR/$name.json"
  [[ -f "$golden" ]] || fail "missing golden for $name"
  echo "    $name" >&2

  first=""
  for ((run = 1; run <= ORACLE_RUNS; run++)); do
    actual="$TEMP_DIR/$name.java.$run.json"
    timeout "$SCENARIO_TIMEOUT_SECONDS" "$JAVA_ORACLE" "$scenario" >"$actual"
    python3 "$COMPARATOR" "$golden" "$actual" || {
      echo "verify-m10-oracle: frozen golden no longer matches live Helix for $name" >&2
      echo "  scenario: $scenario" >&2
      echo "  golden:   $golden" >&2
      echo "  actual:   $actual" >&2
      exit 1
    }
    if [[ $run -eq 1 ]]; then
      first="$actual"
    else
      python3 "$COMPARATOR" "$first" "$actual" || {
        echo "verify-m10-oracle: live Helix oracle was non-deterministic for $name" >&2
        exit 1
      }
    fi
  done
  count=$((count + 1))
done

echo >&2
echo "verify-m10-oracle: PASS" >&2
echo "  scenarios: $count" >&2
echo "  live oracle runs per scenario: $ORACLE_RUNS" >&2
echo "  authority: Apache Helix 2.0.1 + ZooKeeper" >&2
