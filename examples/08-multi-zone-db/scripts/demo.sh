#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "$DIR/../.." && pwd)"
BIN="$ROOT_DIR/target/debug/multi-zone-db"
export ZONEDB_ETCD_ENDPOINT="${ZONEDB_ETCD_ENDPOINT:-http://127.0.0.1:23798}"
export ZONEDB_PREFIX="${ZONEDB_PREFIX:-zonedb-demo-$$}"
export ZONEDB_CLUSTER="${ZONEDB_CLUSTER:-zonedb-demo}"
export ZONEDB_NODES="${ZONEDB_NODES:-db-a=zone-a@127.0.0.1:28101,db-b=zone-b@127.0.0.1:28102,db-c=zone-c@127.0.0.1:28103}"
PIDS=(); cleanup(){ set +e; for p in "${PIDS[@]}"; do kill -TERM "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; docker compose -f "$DIR/docker-compose.yml" down -v >/dev/null 2>&1 || true; }; trap cleanup EXIT INT TERM
docker compose -f "$DIR/docker-compose.yml" up -d etcd
cargo build --manifest-path "$DIR/Cargo.toml"
"$BIN" admin init "$ZONEDB_NODES"
"$BIN" controller >"$DIR/controller.log" 2>&1 & PIDS+=("$!")
for n in db-a db-b db-c; do "$BIN" node "$n" >"$DIR/$n.log" 2>&1 & PIDS+=("$!"); done
wait_for(){ for _ in {1..120}; do if "$BIN" status | python3 -c 'import json,sys; s=json.load(sys.stdin); e=s.get("external_view",{}).get("zoned-db",{}); raise SystemExit(0 if len(e)==6 and not s.get("pending_transitions") and all(sum(v=="LEADER" for v in p.values())==1 and len(p)==3 for p in e.values()) else 1)' ; then return; fi; sleep .25; done; echo timed out >&2; exit 1; }
wait_for; echo '=== initial cross-zone placement ==='; "$BIN" status
"$BIN" put customer-1 alice
echo '=== killing zone-b (db-b) ==='; kill -KILL "${PIDS[2]}"; wait_for || true
for _ in {1..80}; do if "$BIN" get customer-1 | grep -q 'VALUE alice'; then break; fi; sleep .25; done
echo '=== surviving data after zone failure ==='; "$BIN" status; "$BIN" get customer-1
export ZONEDB_NODES="$ZONEDB_NODES,db-d=zone-b@127.0.0.1:28104"
"$BIN" admin add db-d=zone-b@127.0.0.1:28104
"$BIN" node db-d >"$DIR/db-d.log" 2>&1 & PIDS+=("$!")
wait_for; echo '=== healed three-zone placement ==='; "$BIN" status; "$BIN" get customer-1
