#!/usr/bin/env bash
set -Eeuo pipefail

demo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
binary="${OBJECT_STORE_BINARY:-cargo run --quiet --manifest-path "$demo_dir/Cargo.toml" --}"
export OBJECT_STORE_ETCD_ENDPOINT="${OBJECT_STORE_ETCD_ENDPOINT:-http://127.0.0.1:2379}"
export OBJECT_STORE_PREFIX="${OBJECT_STORE_PREFIX:-object-store-demo-$$}"
export OBJECT_STORE_CLUSTER="${OBJECT_STORE_CLUSTER:-objects}"
export OBJECT_STORE_NODES="${OBJECT_STORE_NODES:-object-a@zone-a=127.0.0.1:18101,object-b@zone-b=127.0.0.1:18102,object-c@zone-c=127.0.0.1:18103}"
export OBJECT_STORE_PEERS="object-a=127.0.0.1:18101,object-b=127.0.0.1:18102,object-c=127.0.0.1:18103"
declare -a pids=()
cleanup() { for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT INT TERM

if [[ "$binary" == cargo\ * ]]; then
  # shellcheck disable=SC2086
  $binary setup
else
  "$binary" setup
fi
if [[ "$binary" == cargo\ * ]]; then
  $binary controller >"$demo_dir/controller.log" 2>&1 &
else
  "$binary" controller >"$demo_dir/controller.log" 2>&1 &
fi
pids+=("$!")
for spec in object-a:18101 object-b:18102 object-c:18103; do
  IFS=: read -r name port <<<"$spec"
  export OBJECT_STORE_INSTANCE_ID="$name" OBJECT_STORE_LISTEN="127.0.0.1:$port"
  if [[ "$binary" == cargo\ * ]]; then $binary node >"$demo_dir/$name.log" 2>&1 & else "$binary" node >"$demo_dir/$name.log" 2>&1 & fi
  pids+=("$!")
done
sleep 5
if [[ "$binary" == cargo\ * ]]; then $binary status; else "$binary" status; fi
echo "PUT bucket/photo-001 payload"
if [[ "$binary" == cargo\ * ]]; then $binary put bucket/photo-001 payload; else "$binary" put bucket/photo-001 payload; fi
echo "Kill object-a (zone-a), wait for lease expiry, then inspect recovery"
kill -9 "${pids[1]}" 2>/dev/null || true
sleep 4
if [[ "$binary" == cargo\ * ]]; then $binary status; else "$binary" status; fi
echo "Restart object-a and inspect healed three-zone placement"
export OBJECT_STORE_INSTANCE_ID=object-a OBJECT_STORE_LISTEN=127.0.0.1:18101
if [[ "$binary" == cargo\ * ]]; then $binary node >"$demo_dir/object-a-restarted.log" 2>&1 & else "$binary" node >"$demo_dir/object-a-restarted.log" 2>&1 & fi
pids+=("$!")
sleep 5
if [[ "$binary" == cargo\ * ]]; then $binary status; $binary get bucket/photo-001; else "$binary" status; "$binary" get bucket/photo-001; fi
