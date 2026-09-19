#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import sys


PROFILES = {
    "scale-100-10k": {
        "max_action_latency_millis": 300_000,
        "max_rss_bytes": 4_000_000_000,
        "max_open_fd_count": 20_000,
        "max_external_view_bytes": 50_000_000,
        "max_revision_growth": 200_000,
    },
    "scale-250-50k": {
        "max_action_latency_millis": 900_000,
        "max_rss_bytes": 12_000_000_000,
        "max_open_fd_count": 50_000,
        "max_external_view_bytes": 250_000_000,
        "max_revision_growth": 1_000_000,
    },
}


def limit(profile, name):
    variable = "CLUSTODIAN_SCALE_" + name.upper()
    return int(os.environ.get(variable, PROFILES[profile][name]))


def fail(message):
    print(f"scale metrics: ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", required=True, type=pathlib.Path)
    parser.add_argument("--profile", required=True, choices=PROFILES)
    args = parser.parse_args()
    path = args.work_dir / "metrics.jsonl"
    if not path.exists():
        fail(f"missing {path}")
    samples = [json.loads(line) for line in path.read_text().splitlines() if line]
    if not samples:
        fail("no metric samples")
    latest = samples[-1]
    checks = {
        "action_latency_millis": max(x["action_latency_millis"] for x in samples),
        "rss_bytes": max(x["rss_bytes"] for x in samples),
        "open_fd_count": max(x["open_fd_count"] for x in samples),
        "external_view_bytes": max(x["external_view_bytes"] for x in samples),
        "revision_growth": latest["observer_revision"] - samples[0]["observer_revision"],
    }
    for name, value in checks.items():
        maximum = limit(args.profile, "max_" + name)
        if value > maximum:
            fail(f"{name}={value} exceeds {maximum}")
    print(json.dumps({"profile": args.profile, "samples": len(samples), **checks}, sort_keys=True))


if __name__ == "__main__":
    main()
