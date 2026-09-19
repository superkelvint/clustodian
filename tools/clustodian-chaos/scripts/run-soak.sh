#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
SEED="${CLUSTODIAN_M13_SOAK_SEED:-187231}"
STEPS="${CLUSTODIAN_M13_SOAK_STEPS:-10000}"
WORK_DIR="${CLUSTODIAN_CHAOS_WORK_DIR:-$ROOT/target/clustodian-chaos/soak-$SEED}"

mkdir -p "$WORK_DIR"
exec cargo run --manifest-path "$ROOT/Cargo.toml" -p clustodian-chaos -- run --seed "$SEED" --steps "$STEPS" --profile soak --trace-out "$WORK_DIR/trace.json"
