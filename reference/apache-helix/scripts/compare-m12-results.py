#!/usr/bin/env python3
"""Compare two M12 result documents for semantic equivalence."""

import argparse
import json
import sys

from m12_result import load_json, normalized_result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--java", required=True)
    parser.add_argument("--rust", required=True)
    parser.add_argument("--same-backend", action="store_true")
    args = parser.parse_args()

    try:
        scenario = load_json(args.scenario)
        left = normalized_result(load_json(args.java), scenario)
        right = normalized_result(load_json(args.rust), scenario)
    except (OSError, ValueError, TypeError) as error:
        print(f"M12 comparison failure: {error}", file=sys.stderr)
        return 1

    if left != right:
        label = "repeatability mismatch" if args.same_backend else "Helix/Rust semantic mismatch"
        print(label, file=sys.stderr)
        print("LEFT:", json.dumps(left, indent=2, sort_keys=True), file=sys.stderr)
        print("RIGHT:", json.dumps(right, indent=2, sort_keys=True), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
