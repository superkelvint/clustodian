#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
RUNTIME_ENV="${CLUSTODIAN_M13_RUNTIME_ENV:-$ROOT/target/clustodian-chaos/runtime.env}"
if [[ -f "$RUNTIME_ENV" ]]; then
  # shellcheck disable=SC1090
  source "$RUNTIME_ENV"
fi
if [[ "${CLUSTODIAN_M13_RUNTIME:-}" == local ]]; then
  PID_FILE="${CLUSTODIAN_M13_LOCAL_PID_FILE:-}"
  if [[ -n "$PID_FILE" && -f "$PID_FILE" ]]; then
    while read -r pid; do
      [[ -n "$pid" ]] && kill -TERM "$pid" >/dev/null 2>&1 || true
    done <"$PID_FILE"
    for attempt in $(seq 1 100); do
      alive=0
      while read -r pid; do
        if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then alive=1; fi
      done <"$PID_FILE"
      [[ "$alive" == 0 ]] && break
      sleep .05
    done
    while read -r pid; do
      [[ -n "$pid" ]] && kill -KILL "$pid" >/dev/null 2>&1 || true
    done <"$PID_FILE"
    rm -f "$PID_FILE"
  fi
  exit 0
fi
COMPOSE="$ROOT/tools/clustodian-chaos/docker-compose.yml"
PROJECT="${CLUSTODIAN_M13_COMPOSE_PROJECT:-clustodian-m13}"
docker compose -p "$PROJECT" -f "$COMPOSE" down -v
