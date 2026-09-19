#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
CHAOS="$ROOT/tools/clustodian-chaos"
START="$CHAOS/scripts/start-m13-stack.sh"
STOP="$CHAOS/scripts/stop-m13-stack.sh"
PROJECT="${CLUSTODIAN_M13_COMPOSE_PROJECT:-clustodian-m13-$$}"
SEEDS="${CLUSTODIAN_M13_PR_SEEDS:-187231 4117 9001 12003}"
STEPS="${CLUSTODIAN_M13_PR_STEPS:-20}"
PROFILE="${CLUSTODIAN_M13_PROFILE:-pr}"
TIMEOUT_SECONDS="${CLUSTODIAN_M13_SCENARIO_TIMEOUT_SECONDS:-180}"
RUNTIME_ENV="${CLUSTODIAN_M13_RUNTIME_ENV:-$ROOT/target/clustodian-chaos/runtime.env}"
KEEP_WORK_DIR="${CLUSTODIAN_M13_KEEP_WORK_DIR:-0}"
METRICS_CHECKER="${CLUSTODIAN_M13_METRICS_CHECKER:-}"

fail() {
  echo "verify-m13: ERROR: $*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || fail "cargo not found"
command -v python3 >/dev/null 2>&1 || fail "python3 not found"
[[ -x "$ROOT/reference/apache-helix/verify-m12.sh" ]] || fail "missing M12 verifier"
[[ -x "$START" ]] || fail "missing M13 stack starter"
[[ -x "$STOP" ]] || fail "missing M13 stack stopper"

if [[ "${CLUSTODIAN_M13_SKIP_PREREQUISITE:-0}" == 1 ]]; then
  echo "==> Skipping prerequisite M12 (explicit local iteration mode)" >&2
else
  echo "==> Verifying prerequisite M12" >&2
  "$ROOT/reference/apache-helix/verify-m12.sh"
fi

echo "==> M13 Rust quality gates" >&2
(cd "$ROOT" && cargo fmt --all --check)
(cd "$ROOT" && cargo test --workspace --all-targets --all-features)
(cd "$ROOT" && cargo test -p clustodian \
  --test randomized_cluster_state_machine \
  randomized_cluster_state_machine_smoke)
(cd "$ROOT" && cargo clippy --workspace --all-targets --all-features -- -D warnings)
(cd "$ROOT" && cargo build --quiet -p clustodian-chaos --bins)

export CLUSTODIAN_M13_COMPOSE_PROJECT="$PROJECT"
export CLUSTODIAN_M13_RUNTIME_ENV="$RUNTIME_ENV"
"$START"
# shellcheck disable=SC1090
set -a
source "$RUNTIME_ENV"
set +a
cleanup() {
  "$STOP" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ "\${CLUSTODIAN_M13_CRASH_MATRIX:-1}" == 1 ]]; then
  echo "==> Verifying deterministic M13 crash windows" >&2
  crash_work_dir="$(mktemp -d "\${TMPDIR:-/tmp}/clustodian-m13-crash-matrix.XXXXXX")"
  CLUSTODIAN_CHAOS_CLUSTER="m13" \
  CLUSTODIAN_CHAOS_PREFIX="/clustodian/m13/crash-matrix" \
  CLUSTODIAN_CHAOS_WORK_DIR="$crash_work_dir" \
  CLUSTODIAN_CHAOS_NODE_BIN="$ROOT/target/debug/clustodian-chaos-node" \
  CLUSTODIAN_CHAOS_ETCD_ENDPOINTS="$CLUSTODIAN_M13_ETCD_ENDPOINTS" \
  CLUSTODIAN_CHAOS_CONTROLLER_ENDPOINTS="$CLUSTODIAN_M13_CONTROLLER_ENDPOINTS" \
  CLUSTODIAN_CHAOS_PARTICIPANT_ENDPOINTS="$CLUSTODIAN_M13_PARTICIPANT_ENDPOINTS" \
  CLUSTODIAN_CHAOS_OBSERVER_ENDPOINTS="$CLUSTODIAN_M13_OBSERVER_ENDPOINTS" \
    timeout "$TIMEOUT_SECONDS" \
    cargo run --quiet -p clustodian-chaos -- crash-matrix \
      --prefix "/clustodian/m13/crash-matrix" --work-dir "$crash_work_dir"
  if [[ "$KEEP_WORK_DIR" == 1 ]]; then
    echo "M13 retained crash matrix directory: $crash_work_dir" >&2
  else
    rm -rf "$crash_work_dir"
  fi
fi

if [[ "\${CLUSTODIAN_M13_QUORUM_LOSS:-1}" == 1 ]]; then
  echo "==> Verifying deterministic etcd quorum loss and recovery" >&2
  quorum_work_dir="$(mktemp -d "\${TMPDIR:-/tmp}/clustodian-m13-quorum-loss.XXXXXX")"
  CLUSTODIAN_CHAOS_CLUSTER="m13" \
  CLUSTODIAN_CHAOS_PREFIX="/clustodian/m13/quorum-loss" \
  CLUSTODIAN_CHAOS_WORK_DIR="$quorum_work_dir" \
  CLUSTODIAN_CHAOS_NODE_BIN="$ROOT/target/debug/clustodian-chaos-node" \
  CLUSTODIAN_CHAOS_ETCD_ENDPOINTS="$CLUSTODIAN_M13_ETCD_ENDPOINTS" \
  CLUSTODIAN_CHAOS_CONTROLLER_ENDPOINTS="$CLUSTODIAN_M13_CONTROLLER_ENDPOINTS" \
  CLUSTODIAN_CHAOS_PARTICIPANT_ENDPOINTS="$CLUSTODIAN_M13_PARTICIPANT_ENDPOINTS" \
  CLUSTODIAN_CHAOS_OBSERVER_ENDPOINTS="$CLUSTODIAN_M13_OBSERVER_ENDPOINTS" \
  CLUSTODIAN_CHAOS_TOXIPROXY_URL="${CLUSTODIAN_M13_TOXIPROXY_URL:-}" \
  CLUSTODIAN_M13_COMPOSE_PROJECT="$PROJECT" \
    timeout "$TIMEOUT_SECONDS" \
    cargo run --quiet -p clustodian-chaos -- quorum-loss \
      --prefix "/clustodian/m13/quorum-loss" --work-dir "$quorum_work_dir"
  if [[ "$KEEP_WORK_DIR" == 1 ]]; then
    echo "M13 retained quorum-loss directory: $quorum_work_dir" >&2
  else
    rm -rf "$quorum_work_dir"
  fi
fi

for seed in $SEEDS; do
  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m13-${seed}.XXXXXX")"
  CLUSTODIAN_CHAOS_CLUSTER="m13" \
  CLUSTODIAN_CHAOS_PREFIX="/clustodian/m13/pr-${seed}" \
  CLUSTODIAN_CHAOS_WORK_DIR="$work_dir" \
  CLUSTODIAN_CHAOS_NODE_BIN="$ROOT/target/debug/clustodian-chaos-node" \
  CLUSTODIAN_CHAOS_ETCD_ENDPOINTS="$CLUSTODIAN_M13_ETCD_ENDPOINTS" \
  CLUSTODIAN_CHAOS_CONTROLLER_ENDPOINTS="$CLUSTODIAN_M13_CONTROLLER_ENDPOINTS" \
  CLUSTODIAN_CHAOS_PARTICIPANT_ENDPOINTS="$CLUSTODIAN_M13_PARTICIPANT_ENDPOINTS" \
  CLUSTODIAN_CHAOS_OBSERVER_ENDPOINTS="$CLUSTODIAN_M13_OBSERVER_ENDPOINTS" \
  CLUSTODIAN_CHAOS_TOXIPROXY_URL="${CLUSTODIAN_M13_TOXIPROXY_URL:-}" \
    timeout "$TIMEOUT_SECONDS" \
    cargo run --quiet -p clustodian-chaos -- run --seed "$seed" --steps "$STEPS" --profile "$PROFILE" \
      --trace-out "$work_dir/trace.json"
  if [[ -n "$METRICS_CHECKER" ]]; then
    python3 "$METRICS_CHECKER" --work-dir "$work_dir" --profile "$PROFILE"
  fi
  if [[ "${CLUSTODIAN_M13_RAW_ORACLE:-1}" == 1 ]]; then
    RAW_ORACLE="$ROOT/reference/apache-helix/scripts/check-m13-raw-oracle.py"
    ETCDCTL_BIN="${CLUSTODIAN_M13_ETCDCTL_BIN:-etcdctl}"
    if command -v "$ETCDCTL_BIN" >/dev/null 2>&1; then
      python3 "$RAW_ORACLE" --etcdctl-command "$ETCDCTL_BIN" \
        --endpoints "$CLUSTODIAN_M13_OBSERVER_ENDPOINTS" --prefix "/clustodian/m13/pr-${seed}" \
        --require-derived
    elif [[ "${CLUSTODIAN_M13_RUNTIME:-}" == docker ]]; then
      COMPOSE="${CLUSTODIAN_M13_COMPOSE_FILE:-$ROOT/tools/clustodian-chaos/docker-compose.yml}"
      python3 "$RAW_ORACLE" \
        --etcdctl-command "docker compose -p $PROJECT -f $COMPOSE exec -T etcd1 etcdctl" \
        --endpoints "http://127.0.0.1:2379" --prefix "/clustodian/m13/pr-${seed}" \
        --require-derived
    else
      fail "raw M13 oracle requires etcdctl (set CLUSTODIAN_M13_ETCDCTL_BIN)"
    fi
  fi
  if [[ "$KEEP_WORK_DIR" == 1 ]]; then
    echo "M13 retained work directory: $work_dir" >&2
  else
    rm -rf "$work_dir"
  fi
done

echo "verify-m13: PASS" >&2
if [[ "${CLUSTODIAN_M13_SKIP_PREREQUISITE:-0}" == 1 ]]; then
  echo "  prerequisite: M12 skipped by explicit request" >&2
else
  echo "  prerequisite: M12 passed" >&2
fi
echo "  PR seeds: $SEEDS" >&2
echo "  steps per seed: $STEPS" >&2
