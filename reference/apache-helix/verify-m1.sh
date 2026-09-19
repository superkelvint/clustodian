#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
SCENARIOS="$ROOT/reference/apache-helix/scenarios/m1"

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
  if [[ "$CARGO_TARGET_DIR" = /* ]]; then
    M1_TARGET_DIR="$CARGO_TARGET_DIR"
  else
    M1_TARGET_DIR="$ROOT/$CARGO_TARGET_DIR"
  fi
else
  M1_TARGET_DIR="$ROOT/target"
fi
RUST_CONFORMANCE="$M1_TARGET_DIR/debug/clustodian-conformance"

"$ROOT/reference/apache-helix/verify-m0.sh"
(
  cd "$ROOT"
  cargo fmt --all --check
  cargo test -p clustodian --all-targets --all-features
  cargo test -p clustodian-conformance --all-targets --all-features
  cargo clippy -p clustodian --all-targets --all-features -- -D warnings
  cargo clippy -p clustodian-conformance --all-targets --all-features -- -D warnings
  cargo build -p clustodian-conformance --all-features
)

if [[ ! -x "$RUST_CONFORMANCE" ]]; then
  echo "missing Rust conformance executable: $RUST_CONFORMANCE" >&2
  exit 1
fi

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m1.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT

shopt -s nullglob
scenarios=("$SCENARIOS"/*.json)
if [[ ${#scenarios[@]} -eq 0 ]]; then
  echo "no M1 scenarios found under $SCENARIOS" >&2
  exit 1
fi

for scenario in "${scenarios[@]}"; do
  name="$(basename "$scenario")"
  java_result="$TEMP_DIR/$name.java.json"
  rust_result="$TEMP_DIR/$name.rust.json"
  "$ROOT/reference/apache-helix/scripts/run-java-oracle.sh" "$scenario" > "$java_result"
  "$RUST_CONFORMANCE" "$scenario" > "$rust_result"
  python3 "$ROOT/reference/apache-helix/scripts/compare-m1-semantic.py" \
    "$java_result" "$rust_result"
  echo "verified $name" >&2
done
