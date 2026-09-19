#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <prepare|run> <scenario.json> <state.json>" >&2
  exit 2
fi

# Fast verifiers build once and point this wrapper at the exact binary.
# Ordinary/manual use keeps the old cargo-run fallback.
if [[ -n "${CLUSTODIAN_M10_CONTROLLER_SCENARIO_BIN:-}" ]]; then
  [[ -x "$CLUSTODIAN_M10_CONTROLLER_SCENARIO_BIN" ]] || {
    echo "controller scenario binary is not executable: $CLUSTODIAN_M10_CONTROLLER_SCENARIO_BIN" >&2
    exit 1
  }
  exec "$CLUSTODIAN_M10_CONTROLLER_SCENARIO_BIN" "$@"
fi

exec cargo run --quiet --manifest-path "$ROOT/Cargo.toml" \
  -p clustodian-conformance --bin controller-scenario -- "$@"
