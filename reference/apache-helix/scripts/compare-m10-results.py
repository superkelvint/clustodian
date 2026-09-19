#!/usr/bin/env python3
import difflib
import json
import sys
from pathlib import Path

EXPECTED_OPERATION = "controller_runtime_semantics"


def fail(message: str) -> None:
    raise SystemExit(f"compare-m10-results: {message}")


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


def nonempty_string(value, description: str, path: Path) -> str:
    if not isinstance(value, str) or not value:
        fail(f"{path}: {description} must be a non-empty string")
    return value


def canonical_live_instances(raw, path: Path) -> dict[str, str]:
    if not isinstance(raw, dict):
        fail(f"{path}: live_instances must be an object")
    result: dict[str, str] = {}
    for instance, session in raw.items():
        nonempty_string(instance, "live instance key", path)
        nonempty_string(session, f"live session for {instance}", path)
        result[instance] = session
    return dict(sorted(result.items()))


def canonical_resources(raw, description: str, path: Path) -> dict:
    if not isinstance(raw, dict):
        fail(f"{path}: {description} must be an object")
    result: dict[str, dict[str, str]] = {}
    for resource, partitions in raw.items():
        nonempty_string(resource, f"{description} resource key", path)
        if not isinstance(partitions, dict):
            fail(f"{path}: {description}[{resource!r}] must be an object")
        part_map: dict[str, str] = {}
        for partition, state in partitions.items():
            nonempty_string(partition, f"{description} partition key", path)
            nonempty_string(state, f"{description}[{resource!r}][{partition!r}]", path)
            part_map[partition] = state
        result[resource] = dict(sorted(part_map.items()))
    return dict(sorted(result.items()))


def canonical_active_current_state(raw, path: Path) -> dict:
    if not isinstance(raw, dict):
        fail(f"{path}: active_current_state must be an object")
    result: dict[str, dict] = {}
    for instance, entry in raw.items():
        nonempty_string(instance, "active_current_state instance key", path)
        if not isinstance(entry, dict):
            fail(f"{path}: active_current_state[{instance!r}] must be an object")
        session = nonempty_string(
            entry.get("session"), f"active_current_state[{instance!r}].session", path
        )
        resources = canonical_resources(
            entry.get("resources"), f"active_current_state[{instance!r}].resources", path
        )
        result[instance] = {"session": session, "resources": resources}
    return dict(sorted(result.items()))


def canonical_external_view(raw, path: Path) -> dict:
    if not isinstance(raw, dict):
        fail(f"{path}: external_view must be an object")
    result: dict[str, dict[str, dict[str, str]]] = {}
    for resource, partitions in raw.items():
        nonempty_string(resource, "external_view resource key", path)
        if not isinstance(partitions, dict):
            fail(f"{path}: external_view[{resource!r}] must be an object")
        out_parts: dict[str, dict[str, str]] = {}
        for partition, instances in partitions.items():
            nonempty_string(partition, "external_view partition key", path)
            if not isinstance(instances, dict):
                fail(
                    f"{path}: external_view[{resource!r}][{partition!r}] must be an object"
                )
            out_instances: dict[str, str] = {}
            for instance, state in instances.items():
                nonempty_string(instance, "external_view instance key", path)
                nonempty_string(state, "external_view state", path)
                out_instances[instance] = state
            out_parts[partition] = dict(sorted(out_instances.items()))
        result[resource] = dict(sorted(out_parts.items()))
    return dict(sorted(result.items()))


def canonical_transition(item: dict, index: int, path: Path) -> dict:
    if not isinstance(item, dict):
        fail(f"{path}: pending_transitions[{index}] must be an object")
    required = (
        "resource",
        "partition",
        "instance",
        "target_session",
        "from",
        "to",
        "message_type",
    )
    result = {}
    for key in required:
        result[key] = nonempty_string(
            item.get(key), f"pending_transitions[{index}].{key}", path
        )
    return result


def canonical_transitions(raw, path: Path) -> list[dict]:
    if not isinstance(raw, list):
        fail(f"{path}: pending_transitions must be an array")
    result = [canonical_transition(item, i, path) for i, item in enumerate(raw)]
    result.sort(
        key=lambda item: (
            item["resource"],
            item["partition"],
            item["instance"],
            item["target_session"],
            item["from"],
            item["to"],
            item["message_type"],
        )
    )
    keys = [
        (
            item["resource"],
            item["partition"],
            item["instance"],
            item["target_session"],
            item["from"],
            item["to"],
            item["message_type"],
        )
        for item in result
    ]
    if len(keys) != len(set(keys)):
        fail(f"{path}: duplicate semantic pending transition")
    return result


def canonical_checkpoints(document: dict, path: Path) -> list[dict]:
    raw = document.get("checkpoints")
    if not isinstance(raw, list):
        fail(f"{path}: missing array field 'checkpoints'")
    result: list[dict] = []
    seen: set[str] = set()
    for index, checkpoint in enumerate(raw):
        if not isinstance(checkpoint, dict):
            fail(f"{path}: checkpoints[{index}] must be an object")
        checkpoint_id = nonempty_string(
            checkpoint.get("id"), f"checkpoints[{index}].id", path
        )
        if checkpoint_id in seen:
            fail(f"{path}: duplicate checkpoint id {checkpoint_id!r}")
        seen.add(checkpoint_id)
        result.append(
            {
                "id": checkpoint_id,
                "live_instances": canonical_live_instances(
                    checkpoint.get("live_instances"), path
                ),
                "active_current_state": canonical_active_current_state(
                    checkpoint.get("active_current_state"), path
                ),
                "external_view": canonical_external_view(
                    checkpoint.get("external_view"), path
                ),
                "pending_transitions": canonical_transitions(
                    checkpoint.get("pending_transitions"), path
                ),
            }
        )
    # Checkpoint order is scenario chronology and is semantic.
    return result


def semantic(document: dict, path: Path) -> dict:
    return {"checkpoints": canonical_checkpoints(document, path)}


def pretty(value: dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: compare-m10-results.py <left-result.json> <right-result.json>")
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
