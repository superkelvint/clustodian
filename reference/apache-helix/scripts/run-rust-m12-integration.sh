#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"

fail() {
  echo "run-rust-m12-integration: ERROR: $*" >&2
  exit 1
}

[[ $# -eq 1 ]] || fail "usage: $0 <scenario.json>"
SCENARIO="$1"
[[ -f "$SCENARIO" ]] || fail "scenario not found: $SCENARIO"

: "${CLUSTODIAN_M12_ETCD_ENDPOINT:?CLUSTODIAN_M12_ETCD_ENDPOINT is required}"
: "${CLUSTODIAN_M12_ETCD_PREFIX:?CLUSTODIAN_M12_ETCD_PREFIX is required}"
: "${CLUSTODIAN_M12_WORK_DIR:?CLUSTODIAN_M12_WORK_DIR is required}"

mkdir -p "$CLUSTODIAN_M12_WORK_DIR"

cd "$ROOT"
exec cargo run --quiet \
  -p clustodian-conformance \
  --bin clustodian-m12-integration \
  -- "$SCENARIO"
