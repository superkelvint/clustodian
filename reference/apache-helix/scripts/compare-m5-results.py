#!/usr/bin/env python3
import difflib
import json
import sys
from pathlib import Path

EXPECTED_OPERATION = "select_transitions"
REQUIRED_FIELDS = ("resource", "partition", "instance", "from", "to", "message_type")


def fail(message: str) -> None:
    raise SystemExit(f"compare-m5-results: {message}")


def load(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"{path}: top-level JSON value must be an object")
    return value


def canonical_selected(document: dict, path: Path) -> list[dict[str, str]]:
    operation = document.get("operation")
    if operation != EXPECTED_OPERATION:
        fail(f"{path}: operation must be {EXPECTED_OPERATION!r}, got {operation!r}")

    transitions = document.get("selected_transitions")
    if not isinstance(transitions, list):
        fail(f"{path}: missing array field 'selected_transitions'")

    canonical: list[dict[str, str]] = []
    seen: set[tuple[str, ...]] = set()

    for index, transition in enumerate(transitions):
        if not isinstance(transition, dict):
            fail(f"{path}: selected_transitions[{index}] must be an object")

        missing = [field for field in REQUIRED_FIELDS if field not in transition]
        if missing:
            fail(
                f"{path}: selected_transitions[{index}] missing fields: "
                + ", ".join(missing)
            )

        item: dict[str, str] = {}
        for field in REQUIRED_FIELDS:
            value = transition[field]
            if not isinstance(value, str) or not value:
                fail(
                    f"{path}: selected_transitions[{index}].{field} "
                    "must be a non-empty string"
                )
            item[field] = value

        key = tuple(item[field] for field in REQUIRED_FIELDS)
        if key in seen:
            fail(f"{path}: duplicate selected transition at index {index}: {key!r}")
        seen.add(key)
        canonical.append(item)

    # Selection order is not part of the M5 compatibility claim. Which
    # transitions are selected is semantic; incidental MessageOutput/list order
    # is not. Candidate-order-sensitive choices are covered by separate fixtures.
    canonical.sort(key=lambda item: tuple(item[field] for field in REQUIRED_FIELDS))
    return canonical


def pretty(value: list[dict[str, str]]) -> str:
    return json.dumps(
        {"selected_transitions": value},
        indent=2,
        sort_keys=True,
    ) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: compare-m5-results.py <java-result.json> <rust-result.json>")

    left_path = Path(sys.argv[1])
    right_path = Path(sys.argv[2])

    left = canonical_selected(load(left_path), left_path)
    right = canonical_selected(load(right_path), right_path)

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
