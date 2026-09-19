#!/usr/bin/env python3
import difflib
import json
import sys
from pathlib import Path

EXPECTED_OPERATION = "compute_intermediate_and_throttle"
TRANSITION_FIELDS = ("resource", "partition", "instance", "from", "to", "message_type")


def fail(message: str) -> None:
    raise SystemExit(f"compare-m6-results: {message}")


def load(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"{path}: top-level JSON value must be an object")
    operation = value.get("operation")
    if operation != EXPECTED_OPERATION:
        fail(f"{path}: operation must be {EXPECTED_OPERATION!r}, got {operation!r}")
    return value


def canonical_intermediate(document: dict, path: Path) -> dict[str, dict[str, dict[str, str]]]:
    raw = document.get("intermediate_state")
    if not isinstance(raw, dict):
        fail(f"{path}: missing object field 'intermediate_state'")

    canonical: dict[str, dict[str, dict[str, str]]] = {}
    for resource, partitions in raw.items():
        if not isinstance(resource, str) or not resource:
            fail(f"{path}: intermediate_state resource keys must be non-empty strings")
        if not isinstance(partitions, dict):
            fail(f"{path}: intermediate_state[{resource!r}] must be an object")

        canonical_partitions: dict[str, dict[str, str]] = {}
        for partition, instances in partitions.items():
            if not isinstance(partition, str) or not partition:
                fail(f"{path}: partition keys must be non-empty strings")
            if not isinstance(instances, dict):
                fail(
                    f"{path}: intermediate_state[{resource!r}][{partition!r}] "
                    "must be an object"
                )

            canonical_instances: dict[str, str] = {}
            for instance, state in instances.items():
                if not isinstance(instance, str) or not instance:
                    fail(f"{path}: instance keys must be non-empty strings")
                if not isinstance(state, str) or not state:
                    fail(
                        f"{path}: intermediate state for {resource}/{partition}/{instance} "
                        "must be a non-empty string"
                    )
                canonical_instances[instance] = state

            canonical_partitions[partition] = dict(sorted(canonical_instances.items()))

        canonical[resource] = dict(sorted(canonical_partitions.items()))

    return dict(sorted(canonical.items()))


def canonical_dispatchable(document: dict, path: Path) -> list[dict[str, str]]:
    raw = document.get("dispatchable_transitions")
    if not isinstance(raw, list):
        fail(f"{path}: missing array field 'dispatchable_transitions'")

    canonical: list[dict[str, str]] = []
    seen: set[tuple[str, ...]] = set()

    for index, transition in enumerate(raw):
        if not isinstance(transition, dict):
            fail(f"{path}: dispatchable_transitions[{index}] must be an object")

        missing = [field for field in TRANSITION_FIELDS if field not in transition]
        if missing:
            fail(
                f"{path}: dispatchable_transitions[{index}] missing fields: "
                + ", ".join(missing)
            )

        item: dict[str, str] = {}
        for field in TRANSITION_FIELDS:
            value = transition[field]
            if not isinstance(value, str) or not value:
                fail(
                    f"{path}: dispatchable_transitions[{index}].{field} "
                    "must be a non-empty string"
                )
            item[field] = value

        key = tuple(item[field] for field in TRANSITION_FIELDS)
        if key in seen:
            fail(f"{path}: duplicate dispatchable transition at index {index}: {key!r}")
        seen.add(key)
        canonical.append(item)

    # Message list ordering is not part of the M6 compatibility claim. Which
    # selected messages survive throttling is semantic; incidental Java map/list
    # traversal order is not. Order-sensitive quota competition is represented
    # by distinct fixtures when needed.
    canonical.sort(key=lambda item: tuple(item[field] for field in TRANSITION_FIELDS))
    return canonical


def semantic(document: dict, path: Path) -> dict:
    return {
        "dispatchable_transitions": canonical_dispatchable(document, path),
        "intermediate_state": canonical_intermediate(document, path),
    }


def pretty(value: dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: compare-m6-results.py <left-result.json> <right-result.json>")

    left_path = Path(sys.argv[1])
    right_path = Path(sys.argv[2])

    left = semantic(load(left_path), left_path)
    right = semantic(load(right_path), right_path)

    if left == right:
        return

    sys.stderr.writelines(
        difflib.unified_diff(
            pretty(left).splitlines(keepends=True),
            pretty(right).splitlines(keepends=True),
            fromfile=str(left_path),
            tofile=str(right_path),
        )
    )
    raise SystemExit(1)


if __name__ == "__main__":
    main()
