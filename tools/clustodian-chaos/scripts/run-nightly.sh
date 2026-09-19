#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
SEEDS="${CLUSTODIAN_M13_NIGHTLY_SEEDS:-187231 4117 9001 12003 77123 99117}"
STEPS="${CLUSTODIAN_M13_NIGHTLY_STEPS:-500}"

for seed in $SEEDS; do
  CLUSTODIAN_M13_PR_SEEDS="$seed" CLUSTODIAN_M13_PR_STEPS="$STEPS" CLUSTODIAN_M13_PROFILE=nightly "$ROOT/reference/apache-helix/verify-m13.sh"
done
