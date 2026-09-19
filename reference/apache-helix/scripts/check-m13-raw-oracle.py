#!/usr/bin/env python3
"""Check persisted M13 safety invariants from raw etcd records.

This deliberately does not import clustodian or consume ClusterSnapshot.  It is
the small, read-only oracle used to catch an observer that accidentally agrees
with an incorrect production interpretation of the persisted schema.
"""

import argparse
import base64
import json
import shlex
import subprocess
import sys


def fail(message):
    print(f"M13 raw oracle: ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def segment(value):
    try:
        return bytes.fromhex(value).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as error:
        fail(f"invalid encoded key segment {value!r}: {error}")


def raw_records(command, endpoints, prefix):
    command = shlex.split(command)
    result = subprocess.run(
        command
        + [
            f"--endpoints={endpoints}",
            "get",
            prefix.rstrip("/") + "/",
            "--prefix",
            "--write-out=json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        fail(f"etcdctl failed: {result.stderr.strip()}")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        fail(f"etcdctl returned invalid JSON: {error}")
    records = []
    prefix = prefix.rstrip("/") + "/"
    for item in payload.get("kvs", []):
        key = base64.b64decode(item["key"]).decode("utf-8")
        if not key.startswith(prefix):
            fail(f"raw key escaped namespace: {key}")
        records.append(
            (
                key[len(prefix) :],
                base64.b64decode(item["value"]).decode("utf-8"),
                int(item.get("lease", 0)),
            )
        )
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--etcdctl-command", default="etcdctl")
    parser.add_argument("--endpoints", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--require-derived", action="store_true")
    args = parser.parse_args()
    records = raw_records(args.etcdctl_command, args.endpoints, args.prefix)

    live = {}
    current = {}
    metadata = {}
    active_controller = []
    for key, value, lease in records:
        parts = key.split("/")
        if len(parts) == 2 and parts[0] == "live":
            live[segment(parts[1])] = int(value)
        elif len(parts) == 5 and parts[0] == "current-state":
            instance = segment(parts[1])
            try:
                session = int(segment(parts[2]))
            except ValueError as error:
                fail(f"invalid CurrentState session {parts[2]!r}: {error}")
            resource = segment(parts[3])
            partition = segment(parts[4])
            current.setdefault(instance, {}).setdefault(session, {}).setdefault(resource, {})[
                partition
            ] = value
        elif len(parts) == 2 and parts[0] == "metadata":
            metadata[segment(parts[1])] = value
        elif key == "controller/election/active":
            if lease == 0:
                fail("active controller record has no lease")
            active_controller.append(value)

    if len(active_controller) > 1:
        fail(f"multiple active controllers: {active_controller}")
    active_current = {
        instance: current[instance][session]
        for instance, session in live.items()
        if instance in current and session in current[instance]
    }
    current_state_leaders = {}
    for instance, resources in active_current.items():
        for resource, partitions in resources.items():
            for partition, state in partitions.items():
                if instance not in live:
                    fail(f"active CurrentState for dead instance {instance}")
                if state == "LEADER":
                    current_state_leaders.setdefault((resource, partition), []).append(instance)
    for (resource, partition), leaders in current_state_leaders.items():
        if len(leaders) > 1:
            fail(
                "multiple active CurrentState leaders for "
                f"{resource}/{partition}: {leaders}"
            )

    pending = json.loads(metadata.get("controller/output/pending-transitions", "[]"))
    message_ids = set()
    for transition in pending:
        message_id = transition.get("message_id")
        if not message_id or message_id in message_ids:
            fail("pending transitions contain a missing or duplicate message id")
        message_ids.add(message_id)
        if live.get(transition.get("instance")) != transition.get("target_session"):
            fail(f"pending transition targets a stale session: {transition}")

    external = json.loads(metadata.get("controller/output/external-view", "{}"))
    derived = {}
    for instance, resources in active_current.items():
        for resource, partitions in resources.items():
            for partition, state in partitions.items():
                derived.setdefault(resource, {}).setdefault(partition, {})[instance] = state
    if args.require_derived and external != derived:
        fail("ExternalView does not match active CurrentState")

    for resource, partitions in external.items():
        for partition, states in partitions.items():
            leaders = [
                instance
                for instance, state in states.items()
                if state == "LEADER" and instance in live
            ]
            if len(leaders) > 1:
                fail(f"multiple leaders for {resource}/{partition}: {leaders}")
    print(json.dumps({"records": len(records), "live_instances": len(live), "pending": len(pending)}))


if __name__ == "__main__":
    main()
