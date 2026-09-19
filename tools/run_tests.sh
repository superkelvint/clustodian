#!/usr/bin/env bash

set -u

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/.." && pwd)
cd -- "$repo_root"

usage() {
    printf 'Usage: %s NUMBER_OF_RUNS\n' "$0" >&2
}

if [[ $# -ne 1 || ! $1 =~ ^[1-9][0-9]*$ ]]; then
    usage
    exit 2
fi

runs=$1
failure_dir=${FAILURE_DIR:-"test-failures/$(date -u +%Y%m%dT%H%M%SZ)"}
mkdir -p "$failure_dir"

failures=0
tmp_file=
trap '[[ -z ${tmp_file:-} ]] || rm -f -- "$tmp_file"' EXIT

for ((run = 1; run <= runs; run++)); do
    printf 'Run %d/%d...\n' "$run" "$runs"
    tmp_file=$(mktemp)

    if cargo test --workspace --all-targets --no-default-features >"$tmp_file" 2>&1; then
        rm -f -- "$tmp_file"
        tmp_file=
    else
        failure_log=$(printf '%s/run-%04d.log' "$failure_dir" "$run")
        mv -- "$tmp_file" "$failure_log"
        tmp_file=
        failures=$((failures + 1))
        printf '  FAILED (saved to %s)\n' "$failure_log" >&2
    fi
done

if ((failures > 0)); then
    printf '%d of %d run(s) failed. Logs are in %s.\n' "$failures" "$runs" "$failure_dir" >&2
    exit 1
fi

printf 'All %d run(s) passed.\n' "$runs"
