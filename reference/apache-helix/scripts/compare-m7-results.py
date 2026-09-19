#!/usr/bin/env python3
import difflib
import json
import sys
from pathlib import Path

EXPECTED_OPERATION = "compute_external_view_and_routing"


def fail(message: str) -> None:
    raise SystemExit(f"compare-m7-results: {message}")


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


def canonical_external_view(document: dict, path: Path) -> dict[str, dict[str, dict[str, str]]]:
    raw = document.get("external_view")
    if not isinstance(raw, dict):
        fail(f"{path}: missing object field 'external_view'")

    canonical: dict[str, dict[str, dict[str, str]]] = {}

    for resource, partitions in raw.items():
        if not isinstance(resource, str) or not resource:
            fail(f"{path}: external_view resource keys must be non-empty strings")
        if not isinstance(partitions, dict):
            fail(f"{path}: external_view[{resource!r}] must be an object")

        canonical_partitions: dict[str, dict[str, str]] = {}
        for partition, instances in partitions.items():
            if not isinstance(partition, str) or not partition:
                fail(f"{path}: partition keys must be non-empty strings")
            if not isinstance(instances, dict):
                fail(
                    f"{path}: external_view[{resource!r}][{partition!r}] "
                    "must be an object"
                )

            canonical_instances: dict[str, str] = {}
            for instance, state in instances.items():
                if not isinstance(instance, str) or not instance:
                    fail(f"{path}: instance keys must be non-empty strings")
                if not isinstance(state, str) or not state:
                    fail(
                        f"{path}: state for {resource}/{partition}/{instance} "
                        "must be a non-empty string"
                    )
                canonical_instances[instance] = state

            canonical_partitions[partition] = dict(sorted(canonical_instances.items()))

        canonical[resource] = dict(sorted(canonical_partitions.items()))

    return dict(sorted(canonical.items()))


def canonical_routing_results(document: dict, path: Path) -> list[dict]:
    raw = document.get("routing_results")
    if not isinstance(raw, list):
        fail(f"{path}: missing array field 'routing_results'")

    canonical: list[dict] = []
    seen_ids: set[str] = set()

    for index, result in enumerate(raw):
        if not isinstance(result, dict):
            fail(f"{path}: routing_results[{index}] must be an object")

        query_id = result.get("id")
        instances = result.get("instances")

        if not isinstance(query_id, str) or not query_id:
            fail(f"{path}: routing_results[{index}].id must be a non-empty string")
        if query_id in seen_ids:
            fail(f"{path}: duplicate routing result id {query_id!r}")
        seen_ids.add(query_id)

        if not isinstance(instances, list):
            fail(f"{path}: routing_results[{index}].instances must be an array")

        canonical_instances: list[str] = []
        seen_instances: set[str] = set()
        for instance_index, instance in enumerate(instances):
            if not isinstance(instance, str) or not instance:
                fail(
                    f"{path}: routing_results[{index}].instances[{instance_index}] "
                    "must be a non-empty string"
                )
            if instance in seen_instances:
                fail(
                    f"{path}: routing result {query_id!r} contains duplicate "
                    f"instance {instance!r}"
                )
            seen_instances.add(instance)
            canonical_instances.append(instance)

        # Routing lookup semantics are a set for M7. Do not make incidental
        # Java collection iteration order part of the compatibility contract.
        canonical_instances.sort()
        canonical.append({"id": query_id, "instances": canonical_instances})

    canonical.sort(key=lambda result: result["id"])
    return canonical


def semantic(document: dict, path: Path) -> dict:
    return {
        "external_view": canonical_external_view(document, path),
        "routing_results": canonical_routing_results(document, path),
    }


def pretty(value: dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: compare-m7-results.py <left-result.json> <right-result.json>")

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
