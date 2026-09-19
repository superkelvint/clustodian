#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUNTIME="${CLUSTODIAN_M13_RUNTIME:-auto}"
if [[ "$RUNTIME" != auto && "$RUNTIME" != local && "$RUNTIME" != docker ]]; then
  echo "M13: unknown CLUSTODIAN_M13_RUNTIME: $RUNTIME" >&2
  exit 2
fi
if [[ "$RUNTIME" == docker ]] || [[ "$RUNTIME" == auto ]] && docker info >/dev/null 2>&1; then
  exec "$ROOT/tools/clustodian-chaos/scripts/start-docker-m13-stack.sh"
fi
exec "$ROOT/tools/clustodian-chaos/scripts/start-local-m13-stack.sh"
