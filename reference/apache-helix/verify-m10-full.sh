#!/usr/bin/env bash
# Full Rust-side M10 acceptance: prerequisites + quality gates + deterministic
# two-run comparison against the frozen Helix goldens. Still no Java/ZooKeeper.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

[[ -x "$ROOT/reference/apache-helix/verify-m9.sh" ]] || {
  echo "verify-m10-full: missing executable reference/apache-helix/verify-m9.sh" >&2
  exit 1
}

echo "==> Verifying prerequisite M9" >&2
"$ROOT/reference/apache-helix/verify-m9.sh"

echo >&2
echo "==> cargo fmt" >&2
(
  cd "$ROOT"
  cargo fmt --all --check
)

echo >&2
echo "==> workspace tests" >&2
(
  cd "$ROOT"
  cargo test --workspace --all-targets --all-features
)

echo >&2
echo "==> workspace clippy" >&2
(
  cd "$ROOT"
  cargo clippy --workspace --all-targets --all-features -- -D warnings
)

echo >&2
echo "==> deterministic M10 semantic verification" >&2
CLUSTODIAN_M10_RUST_RUNS=2 "$ROOT/reference/apache-helix/verify-m10.sh"

echo >&2
echo "verify-m10-full: PASS" >&2
