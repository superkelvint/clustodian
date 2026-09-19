#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIOS="$ROOT/reference/apache-helix/scenarios/m0"
EXPECTED="$ROOT/reference/apache-helix/expected/m0"

"$ROOT/reference/apache-helix/scripts/build-java-oracle.sh" >&2

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m0.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT

shopt -s nullglob
scenarios=("$SCENARIOS"/*.json)
if [[ ${#scenarios[@]} -eq 0 ]]; then
  echo "no M0 scenarios found under $SCENARIOS" >&2
  exit 1
fi

for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario")"
  expected="$EXPECTED/$name"
  actual="$TEMP_DIR/$name"
  if [[ ! -f "$expected" ]]; then
    echo "missing expected result: $expected" >&2
    exit 1
  fi
  "$ROOT/reference/apache-helix/scripts/run-java-oracle.sh" "$scenario" > "$actual"
  if ! diff -u "$expected" "$actual"; then
    echo "M0 oracle mismatch for $name" >&2
    exit 1
  fi
  echo "verified $name" >&2
done
