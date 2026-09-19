#!/usr/bin/env bash
# Deliberately regenerate the immutable M10 semantic goldens from the real
# Apache Helix 2.0.1 + ZooKeeper oracle.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIO_DIR="$ROOT/reference/apache-helix/scenarios/m10"
GOLDEN_DIR="$ROOT/reference/apache-helix/goldens/m10"
MANIFEST="$ROOT/reference/apache-helix/M10-GOLDENS.sha256"
JAVA_ORACLE="$ROOT/reference/apache-helix/scripts/run-java-m10-integration-oracle.sh"
COMPARATOR="$ROOT/reference/apache-helix/scripts/compare-m10-results.py"
SCENARIO_TIMEOUT_SECONDS="${CLUSTODIAN_M10_SCENARIO_TIMEOUT_SECONDS:-120}"
ORACLE_RUNS="${CLUSTODIAN_M10_FREEZE_ORACLE_RUNS:-2}"
FORCE=0

if [[ ${1:-} == "--force" ]]; then
  FORCE=1
  shift
fi
if [[ $# -ne 0 ]]; then
  echo "usage: $0 [--force]" >&2
  exit 2
fi

fail() {
  echo "freeze-m10-goldens: ERROR: $*" >&2
  exit 1
}

command -v python3 >/dev/null 2>&1 || fail "python3 not found"
command -v timeout >/dev/null 2>&1 || fail "timeout command not found"
command -v sha256sum >/dev/null 2>&1 || fail "sha256sum not found"
[[ -x "$JAVA_ORACLE" ]] || fail "missing executable Java M10 oracle: $JAVA_ORACLE"
[[ -x "$COMPARATOR" ]] || fail "missing executable M10 comparator: $COMPARATOR"
[[ -d "$SCENARIO_DIR" ]] || fail "missing M10 scenarios: $SCENARIO_DIR"
[[ "$ORACLE_RUNS" =~ ^([2-9]|[1-9][0-9]+)$ ]] || fail "CLUSTODIAN_M10_FREEZE_ORACLE_RUNS must be >= 2"

if { [[ -f "$MANIFEST" ]] || find "$GOLDEN_DIR" -type f -name '*.json' -print -quit 2>/dev/null | grep -q .; } && [[ $FORCE -ne 1 ]]; then
  fail "M10 goldens already exist; use --force only for an intentional re-freeze"
fi

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m10-freeze.XXXXXX")"
STAGE="$TEMP_DIR/goldens"
mkdir -p "$STAGE"
trap 'rm -rf "$TEMP_DIR"' EXIT

shopt -s nullglob
scenarios=("$SCENARIO_DIR"/*.json)
[[ ${#scenarios[@]} -gt 0 ]] || fail "no M10 scenarios found"

echo "==> Freezing M10 goldens from real Apache Helix 2.0.1 + ZooKeeper" >&2
for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario" .json)"
  echo "    $name" >&2
  first=""
  for ((run = 1; run <= ORACLE_RUNS; run++)); do
    actual="$TEMP_DIR/$name.java.$run.json"
    timeout "$SCENARIO_TIMEOUT_SECONDS" "$JAVA_ORACLE" "$scenario" >"$actual"
    if [[ $run -eq 1 ]]; then
      first="$actual"
    else
      python3 "$COMPARATOR" "$first" "$actual" || {
        echo "freeze-m10-goldens: Helix oracle was non-deterministic for $name" >&2
        exit 1
      }
    fi
  done

  # Stable human-readable JSON; semantic list canonicalization remains the
  # comparator's responsibility.
  python3 - "$first" "$STAGE/$name.json" <<'PY'
import json
import sys
src, dst = sys.argv[1:]
with open(src, encoding="utf-8") as f:
    value = json.load(f)
with open(dst, "w", encoding="utf-8") as f:
    json.dump(value, f, indent=2, sort_keys=True)
    f.write("\n")
PY
done

rm -rf "$GOLDEN_DIR"
mkdir -p "$GOLDEN_DIR"
cp "$STAGE"/*.json "$GOLDEN_DIR"/

manifest_tmp="$TEMP_DIR/M10-GOLDENS.sha256"
(
  cd "$ROOT"
  find reference/apache-helix/scenarios/m10 reference/apache-helix/goldens/m10 \
    -type f -name '*.json' -print0 \
    | sort -z \
    | xargs -0 sha256sum
) >"$manifest_tmp"
cp "$manifest_tmp" "$MANIFEST"

echo >&2
echo "freeze-m10-goldens: PASS" >&2
echo "  scenarios frozen: ${#scenarios[@]}" >&2
echo "  oracle runs per scenario: $ORACLE_RUNS" >&2
echo "  goldens: reference/apache-helix/goldens/m10/" >&2
echo "  manifest: reference/apache-helix/M10-GOLDENS.sha256" >&2
echo >&2
echo "Commit the goldens and manifest together. Normal verify-m10.sh runs no Java/ZooKeeper." >&2
