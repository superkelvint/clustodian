#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE="$ROOT/tools/clustodian-chaos/docker-compose.yml"
PROJECT="${CLUSTODIAN_M13_COMPOSE_PROJECT:-clustodian-m13}"
RUNTIME_ENV="${CLUSTODIAN_M13_RUNTIME_ENV:-$ROOT/target/clustodian-chaos/runtime.env}"
mkdir -p "$(dirname -- "$RUNTIME_ENV")"
docker compose -p "$PROJECT" -f "$COMPOSE" up -d
python3 "$ROOT/tools/clustodian-chaos/scripts/bootstrap-toxiproxy.py"
for port in 23791 23792 23793; do
  python3 - "$port" <<'PY'
import json
import sys
import time
import urllib.request

port = sys.argv[1]
for _ in range(120):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=.25) as response:
            if json.loads(response.read()).get("health") in (True, "true"):
                break
    except Exception:
        time.sleep(.25)
else:
    raise SystemExit(f"etcd endpoint did not become healthy: {port}")
PY
done
printf '%s\n' \
  'CLUSTODIAN_M13_RUNTIME=docker' \
  'CLUSTODIAN_M13_ETCD_ENDPOINTS=http://127.0.0.1:23791,http://127.0.0.1:23792,http://127.0.0.1:23793' \
  'CLUSTODIAN_M13_CONTROLLER_ENDPOINTS=http://127.0.0.1:12379,http://127.0.0.1:12380,http://127.0.0.1:12381' \
  'CLUSTODIAN_M13_PARTICIPANT_ENDPOINTS=http://127.0.0.1:13379,http://127.0.0.1:13380,http://127.0.0.1:13381' \
  'CLUSTODIAN_M13_OBSERVER_ENDPOINTS=http://127.0.0.1:23791,http://127.0.0.1:23792,http://127.0.0.1:23793' \
  'CLUSTODIAN_M13_TOXIPROXY_URL=http://127.0.0.1:8474' \
  'CLUSTODIAN_M13_HAS_TOXIPROXY=1' \
  "CLUSTODIAN_M13_COMPOSE_PROJECT=$PROJECT" >"$RUNTIME_ENV"
echo "M13 runtime=docker env=$RUNTIME_ENV"
