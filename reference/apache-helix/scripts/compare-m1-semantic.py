#!/usr/bin/env python3
"""Compare the semantic state-model portion of Java and Rust M1 results."""

import json
import sys
from pathlib import Path


SEMANTIC_FIELDS = (
    "name",
    "initial_state",
    "valid",
    "top_state",
    "single_top_state",
    "states_priority",
    "transition_priority",
    "state_counts",
    "next_states",
)
EXPECTED_OPERATION = "inspect_state_model"


def read(path: str) -> dict:
    value = json.loads(Path(path).read_text())
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def semantic(value: dict, path: str) -> dict:
    operation = value.get("operation")
    if operation != EXPECTED_OPERATION:
        raise ValueError(
            f"{path} operation must be {EXPECTED_OPERATION!r}, got {operation!r}"
        )
    model = value.get("state_model")
    if not isinstance(model, dict):
        raise ValueError(f"{path} has no state_model object")
    missing = [field for field in SEMANTIC_FIELDS if field not in model]
    if missing:
        raise ValueError(f"{path} is missing semantic fields: {', '.join(missing)}")
    return {field: model[field] for field in SEMANTIC_FIELDS}


def main() -> int:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <java-result.json> <rust-result.json>", file=sys.stderr)
        return 2
    try:
        java_result = read(sys.argv[1])
        rust_result = read(sys.argv[2])
        java_semantic = semantic(java_result, sys.argv[1])
        rust_semantic = semantic(rust_result, sys.argv[2])
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"M1 result parsing failed: {error}", file=sys.stderr)
        return 1

    if java_semantic == rust_semantic:
        return 0

    print("M1 semantic mismatch:", file=sys.stderr)
    print("Java:", json.dumps(java_semantic, sort_keys=True, indent=2), file=sys.stderr)
    print("Rust:", json.dumps(rust_semantic, sort_keys=True, indent=2), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
