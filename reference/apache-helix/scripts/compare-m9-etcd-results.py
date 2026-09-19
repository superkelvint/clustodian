#!/usr/bin/env python3
import difflib
import json
import sys
from pathlib import Path

EXPECTED_OPERATION = "etcd_coordination_semantics"


def fail(message: str) -> None:
    raise SystemExit(f"compare-m9-etcd-results: {message}")


def load(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"{path}: top-level JSON value must be an object")
    if value.get("operation") != EXPECTED_OPERATION:
        fail(
            f"{path}: operation must be {EXPECTED_OPERATION!r}, "
            f"got {value.get('operation')!r}"
        )
    return value


def canonical(value):
    if isinstance(value, dict):
        return {key: canonical(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [canonical(item) for item in value]
    return value


def subset(expected, actual, path="observations"):
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            fail(f"{path}: expected object, got {type(actual).__name__}")
        result = {}
        for key, expected_value in expected.items():
            if key not in actual:
                fail(f"{path}: missing expected key {key!r}")
            result[key] = subset(expected_value, actual[key], f"{path}.{key}")
        return result
    if isinstance(expected, list):
        if not isinstance(actual, list):
            fail(f"{path}: expected array, got {type(actual).__name__}")
        if len(expected) != len(actual):
            fail(f"{path}: expected {len(expected)} items, got {len(actual)}")
        return [subset(e, a, f"{path}[{i}]") for i, (e, a) in enumerate(zip(expected, actual))]
    return actual


def pretty(value) -> str:
    return json.dumps(canonical(value), indent=2, sort_keys=True) + "\n"


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: compare-m9-etcd-results.py <scenario.json> <result.json>")

    scenario_path = Path(sys.argv[1])
    result_path = Path(sys.argv[2])
    scenario = load(scenario_path)
    result = load(result_path)

    if scenario.get("operation") != EXPECTED_OPERATION:
        fail(f"{scenario_path}: operation must be {EXPECTED_OPERATION!r}")
    if result.get("operation") != EXPECTED_OPERATION:
        fail(f"{result_path}: operation must be {EXPECTED_OPERATION!r}")

    case = scenario.get("case")
    if not isinstance(case, str) or not case:
        fail(f"{scenario_path}: missing non-empty 'case'")
    result_case = result.get("case")
    if result_case is not None and result_case != case:
        fail(f"{result_path}: case {result_case!r} does not match scenario {case!r}")

    expected = scenario.get("expect")
    if not isinstance(expected, dict):
        fail(f"{scenario_path}: missing object field 'expect'")
    observations = result.get("observations")
    if not isinstance(observations, dict):
        fail(f"{result_path}: missing object field 'observations'")

    actual_subset = subset(expected, observations)
    expected_c = canonical(expected)
    actual_c = canonical(actual_subset)
    if expected_c == actual_c:
        return

    sys.stderr.writelines(
        difflib.unified_diff(
            pretty(expected_c).splitlines(keepends=True),
            pretty(actual_c).splitlines(keepends=True),
            fromfile=f"{scenario_path}:expect",
            tofile=f"{result_path}:observations",
        )
    )
    raise SystemExit(1)


if __name__ == "__main__":
    main()
