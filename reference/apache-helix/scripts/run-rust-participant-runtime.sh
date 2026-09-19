#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <scenario.json>" >&2
  exit 2
fi

exec cargo run --quiet --manifest-path "$ROOT/Cargo.toml" \
  -p clustodian-conformance --bin participant-scenario -- runtime "$1"
