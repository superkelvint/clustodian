#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
SCENARIOS="$ROOT/reference/apache-helix/scenarios/m0"
EXPECTED="$ROOT/reference/apache-helix/expected/m0"

"$SCRIPT_DIR/build-apache-helix-reference.sh"
"$SCRIPT_DIR/build-java-oracle.sh"
mkdir -p "$EXPECTED"

shopt -s nullglob
scenarios=("$SCENARIOS"/*.json)
if [[ ${#scenarios[@]} -eq 0 ]]; then
  echo "no M0 scenarios found under $SCENARIOS" >&2
  exit 1
fi

for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario")"
  echo "generating $name" >&2
  "$SCRIPT_DIR/run-java-oracle.sh" "$scenario" > "$EXPECTED/$name"
done
