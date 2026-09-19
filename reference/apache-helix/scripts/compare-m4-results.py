#!/usr/bin/env python3
import difflib
import json
import sys
from pathlib import Path

EXPECTED_OPERATION = "compute_crush_assignment"


def fail(message: str) -> None:
    raise SystemExit(f"compare-m4-results: {message}")


def load(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")

    if not isinstance(value, dict):
        fail(f"{path}: top-level JSON value must be an object")
    return value


def canonical_preference_lists(document: dict, path: Path) -> dict[str, list[str]]:
    operation = document.get("operation")
    if operation != EXPECTED_OPERATION:
        fail(f"{path}: operation must be {EXPECTED_OPERATION!r}, got {operation!r}")

    preference_lists = document.get("preference_lists")
    if not isinstance(preference_lists, dict):
        fail(f"{path}: missing object field 'preference_lists'")

    canonical: dict[str, list[str]] = {}

    for partition, instances in preference_lists.items():
        if not isinstance(partition, str) or not partition:
            fail(f"{path}: partition names must be non-empty strings")
        if not isinstance(instances, list):
            fail(f"{path}: preference_lists[{partition!r}] must be an array")

        normalized: list[str] = []
        seen: set[str] = set()
        for index, instance in enumerate(instances):
            if not isinstance(instance, str) or not instance:
                fail(
                    f"{path}: preference_lists[{partition!r}][{index}] "
                    "must be a non-empty string"
                )
            if instance in seen:
                fail(
                    f"{path}: preference_lists[{partition!r}] contains duplicate "
                    f"instance {instance!r}"
                )
            seen.add(instance)
            normalized.append(instance)

        # Partition-map order is incidental. Preference-list order is semantic
        # and must be preserved exactly because M3 consumes it.
        canonical[partition] = normalized

    return dict(sorted(canonical.items()))


def pretty(value: dict[str, list[str]]) -> str:
    return json.dumps(
        {"preference_lists": value},
        indent=2,
        sort_keys=True,
    ) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: compare-m4-results.py <java-result.json> <rust-result.json>")

    java_path = Path(sys.argv[1])
    rust_path = Path(sys.argv[2])

    java_semantic = canonical_preference_lists(load(java_path), java_path)
    rust_semantic = canonical_preference_lists(load(rust_path), rust_path)

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
