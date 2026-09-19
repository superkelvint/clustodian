#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

SCENARIO_DIR="$ROOT/reference/apache-helix/scenarios/m2"
JAVA_ORACLE="$ROOT/reference/apache-helix/scripts/run-java-oracle.sh"

die() {
    echo "verify-m2: ERROR: $*" >&2
    exit 1
}

command -v cargo >/dev/null 2>&1 || die "cargo not found"
command -v python3 >/dev/null 2>&1 || die "python3 not found"

[[ -x "$JAVA_ORACLE" ]] || die "missing executable Java oracle: $JAVA_ORACLE"
[[ -d "$SCENARIO_DIR" ]] || die "missing M2 scenario directory: $SCENARIO_DIR"

required_scenarios=(
    01-stable.json
    02-direct-promotion.json
    03-direct-demotion.json
    04-bootstrap.json
    05-multihop-promotion.json
    06-drop.json
    07-multiple-partitions.json
    08-multiple-instances.json
    09-mixed-multihop.json
    10-unreachable.json
)

for required in "${required_scenarios[@]}"; do
    [[ -f "$SCENARIO_DIR/$required" ]] ||
        die "required M2 scenario missing: $required"
done

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/clustodian-m2.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

normalize_transition_result() {
    local input="$1"
    local output="$2"

    python3 - "$input" "$output" <<'PY'
import json
import sys

input_path, output_path = sys.argv[1], sys.argv[2]

with open(input_path, "r", encoding="utf-8") as f:
    data = json.load(f)

if data.get("operation") != "generate_transitions":
    raise SystemExit(
        f"{input_path}: operation must be 'generate_transitions', "
        f"got {data.get('operation')!r}"
    )

if "transitions" not in data:
    raise SystemExit(f"{input_path}: missing top-level 'transitions'")

if not isinstance(data["transitions"], list):
    raise SystemExit(f"{input_path}: 'transitions' must be an array")

required = (
    "resource",
    "partition",
    "instance",
    "from",
    "to",
    "message_type",
)

canonical = []

for index, transition in enumerate(data["transitions"]):
    if not isinstance(transition, dict):
        raise SystemExit(
            f"{input_path}: transitions[{index}] must be an object"
        )

    missing = [key for key in required if key not in transition]
    if missing:
        raise SystemExit(
            f"{input_path}: transitions[{index}] missing fields: "
            + ", ".join(missing)
        )

    item = {key: transition[key] for key in required}

    for key, value in item.items():
        if not isinstance(value, str):
            raise SystemExit(
                f"{input_path}: transitions[{index}].{key} must be a string"
            )

    canonical.append(item)

canonical.sort(
    key=lambda item: (
        item["resource"],
        item["partition"],
        item["instance"],
        item["from"],
        item["to"],
        item["message_type"],
    )
)

with open(output_path, "w", encoding="utf-8") as f:
    json.dump(
        {"transitions": canonical},
        f,
        indent=2,
        sort_keys=True,
    )
    f.write("\n")
PY
}

echo "==> Verifying prerequisite M1"
"$ROOT/reference/apache-helix/verify-m1.sh"

echo
echo "==> Rust formatting"
(
    cd "$ROOT"
    cargo fmt --all --check
)

echo
echo "==> clustodian tests"
(
    cd "$ROOT"
    cargo test -p clustodian
)

echo
echo "==> clustodian-conformance tests"
(
    cd "$ROOT"
    cargo test -p clustodian-conformance
)

echo
echo "==> clustodian clippy"
(
    cd "$ROOT"
    cargo clippy \
        -p clustodian \
        --all-targets \
        --all-features \
        -- \
        -D warnings
)

echo
echo "==> clustodian-conformance clippy"
(
    cd "$ROOT"
    cargo clippy \
        -p clustodian-conformance \
        --all-targets \
        --all-features \
        -- \
        -D warnings
)

mapfile -t scenarios < <(
    find "$SCENARIO_DIR" \
        -maxdepth 1 \
        -type f \
        -name '*.json' \
        -print |
    sort
)

(( ${#scenarios[@]} > 0 )) || die "no M2 scenarios found"

echo
echo "==> Apache Helix 2.0.1 differential transition tests"

count=0

for scenario in "${scenarios[@]}"; do
    name="$(basename "$scenario" .json)"

    java_raw="$TMP_DIR/$name.java.raw.json"
    rust_raw="$TMP_DIR/$name.rust.raw.json"

    java_semantic="$TMP_DIR/$name.java.semantic.json"
    rust_semantic="$TMP_DIR/$name.rust.semantic.json"

    echo "    $name"

    "$JAVA_ORACLE" "$scenario" > "$java_raw"

    (
        cd "$ROOT"
        cargo run \
            --quiet \
            -p clustodian-conformance \
            -- \
            "$scenario"
    ) > "$rust_raw"

    normalize_transition_result "$java_raw" "$java_semantic"
    normalize_transition_result "$rust_raw" "$rust_semantic"

    if ! diff -u "$java_semantic" "$rust_semantic"; then
        echo >&2
        echo "verify-m2: semantic mismatch for $name" >&2
        echo "Java raw output: $java_raw" >&2
        echo "Rust raw output: $rust_raw" >&2
        exit 1
    fi

    count=$((count + 1))
done

echo
echo "verify-m2: PASS"
echo "  M1 prerequisite: passed"
echo "  M2 differential scenarios: $count"
echo "  Java oracle: Apache Helix 2.0.1 MessageGenerationPhase"
echo "  Rust implementation: clustodian transition generation"
