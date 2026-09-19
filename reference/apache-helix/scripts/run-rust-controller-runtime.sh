#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"

# Fast verifiers build once and point this wrapper at the exact binary.
# Ordinary/manual use keeps the old cargo-run fallback.
if [[ -n "${CLUSTODIAN_M10_CONTROLLER_RUNTIME_BIN:-}" ]]; then
  [[ -x "$CLUSTODIAN_M10_CONTROLLER_RUNTIME_BIN" ]] || {
    echo "controller runtime binary is not executable: $CLUSTODIAN_M10_CONTROLLER_RUNTIME_BIN" >&2
    exit 1
  }
  exec "$CLUSTODIAN_M10_CONTROLLER_RUNTIME_BIN" "$@"
fi

exec cargo run --quiet --manifest-path "$ROOT/Cargo.toml" \
  -p clustodian-conformance --bin controller-runtime -- "$@"
