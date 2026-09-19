#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <scenario.json>" >&2
  exit 2
fi

: "${CLUSTODIAN_M9_ETCD_ENDPOINT:?CLUSTODIAN_M9_ETCD_ENDPOINT is required}"
: "${CLUSTODIAN_M9_ETCD_PREFIX:?CLUSTODIAN_M9_ETCD_PREFIX is required}"

cd "$ROOT"
exec cargo run --quiet -p clustodian-conformance -- "$1"
