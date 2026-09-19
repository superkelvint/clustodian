#!/usr/bin/env python3
import difflib
import json
import sys
from pathlib import Path

EXPECTED_OPERATION = "participant_session_semantics"


def fail(message: str) -> None:
    raise SystemExit(f"compare-m8-results: {message}")


def load(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"{path}: top-level JSON value must be an object")
    operation = value.get("operation")
    if operation is not None and operation != EXPECTED_OPERATION:
        fail(f"{path}: operation must be {EXPECTED_OPERATION!r}, got {operation!r}")
    return value


def nonempty_string(value, description: str, path: Path) -> str:
    if not isinstance(value, str) or not value:
        fail(f"{path}: {description} must be a non-empty string")
    return value


def canonical_resources(raw, description: str, path: Path) -> dict:
    if not isinstance(raw, dict):
        fail(f"{path}: {description} must be an object")
    resources = {}
    for resource, partitions in raw.items():
        nonempty_string(resource, f"{description} resource key", path)
        if not isinstance(partitions, dict):
            fail(f"{path}: {description}[{resource!r}] must be an object")
        part_map = {}
        for partition, state in partitions.items():
            nonempty_string(partition, f"{description} partition key", path)
            nonempty_string(state, f"{description}[{resource!r}][{partition!r}]", path)
            part_map[partition] = state
        resources[resource] = dict(sorted(part_map.items()))
    return dict(sorted(resources.items()))


def canonical_live_instances(raw, path: Path) -> dict:
    if not isinstance(raw, dict):
        fail(f"{path}: checkpoint.live_instances must be an object")
    result = {}
    for instance, session in raw.items():
        nonempty_string(instance, "live instance key", path)
        nonempty_string(session, f"live session for {instance}", path)
        result[instance] = session
    return dict(sorted(result.items()))


def canonical_active_current_state(raw, path: Path) -> dict:
    if not isinstance(raw, dict):
        fail(f"{path}: checkpoint.active_current_state must be an object")
    result = {}
    for instance, entry in raw.items():
        nonempty_string(instance, "active_current_state instance key", path)
        if not isinstance(entry, dict):
            fail(f"{path}: active_current_state[{instance!r}] must be an object")
        session = nonempty_string(
            entry.get("session"),
            f"active_current_state[{instance!r}].session",
            path,
        )
        resources = canonical_resources(
            entry.get("resources"),
            f"active_current_state[{instance!r}].resources",
            path,
        )
        result[instance] = {"session": session, "resources": resources}
    return dict(sorted(result.items()))


def canonical_checkpoints(document: dict, path: Path) -> list:
    raw = document.get("checkpoints")
    if not isinstance(raw, list):
        fail(f"{path}: missing array field 'checkpoints'")
    result = []
    seen_ids = set()
    for index, checkpoint in enumerate(raw):
        if not isinstance(checkpoint, dict):
            fail(f"{path}: checkpoints[{index}] must be an object")
        checkpoint_id = nonempty_string(checkpoint.get("id"), f"checkpoints[{index}].id", path)
        if checkpoint_id in seen_ids:
            fail(f"{path}: duplicate checkpoint id {checkpoint_id!r}")
        seen_ids.add(checkpoint_id)
        result.append(
            {
                "id": checkpoint_id,
                "live_instances": canonical_live_instances(checkpoint.get("live_instances"), path),
                "active_current_state": canonical_active_current_state(
                    checkpoint.get("active_current_state"), path
                ),
            }
        )
    return result


def canonical_session_comparisons(document: dict, path: Path) -> list:
    raw = document.get("session_comparisons", [])
    if not isinstance(raw, list):
        fail(f"{path}: session_comparisons must be an array")
    result = []
    seen = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            fail(f"{path}: session_comparisons[{index}] must be an object")
        left = nonempty_string(item.get("left"), f"session_comparisons[{index}].left", path)
        right = nonempty_string(item.get("right"), f"session_comparisons[{index}].right", path)
        equal = item.get("equal")
        if not isinstance(equal, bool):
            fail(f"{path}: session_comparisons[{index}].equal must be boolean")
        key = (left, right)
        if key in seen:
            fail(f"{path}: duplicate session comparison {left!r}/{right!r}")
        seen.add(key)
        result.append({"left": left, "right": right, "equal": equal})
    result.sort(key=lambda item: (item["left"], item["right"]))
    return result


def semantic(document: dict, path: Path) -> dict:
    return {
        "checkpoints": canonical_checkpoints(document, path),
        "session_comparisons": canonical_session_comparisons(document, path),
    }


def pretty(value: dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: compare-m8-results.py <left-result.json> <right-result.json>")
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
