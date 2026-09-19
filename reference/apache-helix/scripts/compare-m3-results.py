#!/usr/bin/env python3
import difflib
import json
import sys
from pathlib import Path

EXPECTED_OPERATION = "compute_semi_auto_best_possible"


def fail(message: str) -> None:
    raise SystemExit(f"compare-m3-results: {message}")


def load(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"{path}: top-level JSON value must be an object")
    return value


def canonical_state_map(document: dict, path: Path) -> dict:
    operation = document.get("operation")
    if operation != EXPECTED_OPERATION:
        fail(
            f"{path}: operation must be {EXPECTED_OPERATION!r}, got {operation!r}"
        )

    state_map = document.get("best_possible_state")
    if not isinstance(state_map, dict):
        fail(f"{path}: missing object field 'best_possible_state'")

    canonical: dict[str, dict[str, str]] = {}
    for partition, instance_states in state_map.items():
        if not isinstance(partition, str) or not partition:
            fail(f"{path}: partition names must be non-empty strings")
        if not isinstance(instance_states, dict):
            fail(f"{path}: best_possible_state[{partition!r}] must be an object")

        normalized_instances: dict[str, str] = {}
        for instance, state in instance_states.items():
            if not isinstance(instance, str) or not instance:
                fail(f"{path}: instance names must be non-empty strings")
            if not isinstance(state, str) or not state:
                fail(
                    f"{path}: state for {partition!r}/{instance!r} "
                    "must be a non-empty string"
                )
            normalized_instances[instance] = state

        canonical[partition] = dict(sorted(normalized_instances.items()))

    return dict(sorted(canonical.items()))


def pretty(value: dict) -> str:
    return json.dumps(
        {"best_possible_state": value},
        indent=2,
        sort_keys=True,
    ) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: compare-m3-results.py <java-result.json> <rust-result.json>")

    java_path = Path(sys.argv[1])
    rust_path = Path(sys.argv[2])

    java_semantic = canonical_state_map(load(java_path), java_path)
    rust_semantic = canonical_state_map(load(rust_path), rust_path)

    if java_semantic == rust_semantic:
        return

    java_text = pretty(java_semantic).splitlines(keepends=True)
    rust_text = pretty(rust_semantic).splitlines(keepends=True)
    sys.stderr.writelines(
        difflib.unified_diff(
            java_text,
            rust_text,
            fromfile="apache-helix-2.0.1",
            tofile="clustodian",
        )
    )
    raise SystemExit(1)


if __name__ == "__main__":
    main()
