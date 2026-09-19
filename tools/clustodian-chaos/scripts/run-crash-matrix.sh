#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
START="$ROOT/tools/clustodian-chaos/scripts/start-m13-stack.sh"
STOP="$ROOT/tools/clustodian-chaos/scripts/stop-m13-stack.sh"
RUNTIME_ENV="${CLUSTODIAN_M13_RUNTIME_ENV:-$ROOT/target/clustodian-chaos/runtime.env}"
if [[ -n "${CLUSTODIAN_CHAOS_CRASH_MATRIX_WORK_DIR:-}" ]]; then
  WORK_DIR="$CLUSTODIAN_CHAOS_CRASH_MATRIX_WORK_DIR"
  REMOVE_WORK_DIR=0
else
  WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-crash-matrix.XXXXXX")"
  REMOVE_WORK_DIR=1
fi
PREFIX="${CLUSTODIAN_CHAOS_CRASH_MATRIX_PREFIX:-/clustodian/m13/crash-matrix-${$}}"
STARTED_STACK=0

cleanup() {
  if [[ "$REMOVE_WORK_DIR" == 1 ]]; then
    rm -rf "$WORK_DIR"
  fi
  if [[ "$STARTED_STACK" == 1 ]]; then
    "$STOP" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ -z "${CLUSTODIAN_CHAOS_ETCD_ENDPOINTS:-}" ]]; then
  "$START"
  # shellcheck disable=SC1090
  set -a
  source "$RUNTIME_ENV"
  set +a
  export CLUSTODIAN_CHAOS_ETCD_ENDPOINTS="$CLUSTODIAN_M13_ETCD_ENDPOINTS"
  export CLUSTODIAN_CHAOS_CONTROLLER_ENDPOINTS="$CLUSTODIAN_M13_CONTROLLER_ENDPOINTS"
  export CLUSTODIAN_CHAOS_PARTICIPANT_ENDPOINTS="$CLUSTODIAN_M13_PARTICIPANT_ENDPOINTS"
  export CLUSTODIAN_CHAOS_OBSERVER_ENDPOINTS="$CLUSTODIAN_M13_OBSERVER_ENDPOINTS"
  STARTED_STACK=1
fi

if [[ "${CLUSTODIAN_M13_KEEP_WORK_DIR:-0}" == 1 ]]; then
  REMOVE_WORK_DIR=0
fi

export CLUSTODIAN_CHAOS_CLUSTER="${CLUSTODIAN_CHAOS_CLUSTER:-m13}"
export CLUSTODIAN_CHAOS_PREFIX="$PREFIX"
export CLUSTODIAN_CHAOS_WORK_DIR="$WORK_DIR"
export CLUSTODIAN_CHAOS_NODE_BIN="${CLUSTODIAN_CHAOS_NODE_BIN:-$ROOT/target/debug/clustodian-chaos-node}"

echo "running deterministic M13 crash matrix in $WORK_DIR" >&2
(cd "$ROOT" && cargo build --quiet -p clustodian-chaos --bins)
(cd "$ROOT" && cargo run --quiet -p clustodian-chaos -- crash-matrix --prefix "$PREFIX" --work-dir "$WORK_DIR")

if [[ "$REMOVE_WORK_DIR" == 0 ]]; then
  echo "retained crash-matrix work directory: $WORK_DIR" >&2
fi
