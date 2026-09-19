#!/usr/bin/env python3
"""Small dependency-free Toxiproxy REST client used by clustodian-chaos."""

import argparse
import json
import urllib.error
import urllib.request


def request(base, method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base.rstrip("/") + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        body = response.read()
        return json.loads(body) if body else {}


def add(args):
    if args.toxic == "latency":
        attributes = {"latency": args.value, "jitter": args.jitter}
    elif args.toxic == "timeout":
        attributes = {"timeout": args.value}
    elif args.toxic == "bandwidth":
        attributes = {"rate": args.value}
    elif args.toxic == "slow_close":
        attributes = {"delay": args.value}
    elif args.toxic == "reset_peer":
        attributes = {"timeout": args.value}
    elif args.toxic == "slicer":
        attributes = {
            "average_size": 1024,
            "size_variation": 512,
            "delay": args.value,
        }
    else:
        raise SystemExit(f"unsupported Toxiproxy toxic: {args.toxic}")
    payload = {
        "name": args.name,
        "type": args.toxic,
        "stream": args.stream,
        "attributes": attributes,
    }
    try:
        request(args.url, "POST", f"/proxies/{args.proxy}/toxics", payload)
    except urllib.error.HTTPError as error:
        if error.code != 409:
            raise
        request(args.url, "DELETE", f"/proxies/{args.proxy}/toxics/{args.name}")
        request(args.url, "POST", f"/proxies/{args.proxy}/toxics", payload)


def remove(args):
    try:
        request(args.url, "DELETE", f"/proxies/{args.proxy}/toxics/{args.name}")
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--stream", default="downstream")
    parser.add_argument(
        "--toxic",
        choices=[
            "latency",
            "timeout",
            "bandwidth",
            "slow_close",
            "reset_peer",
            "slicer",
        ],
    )
    parser.add_argument("--value", type=int, default=1)
    parser.add_argument("--jitter", type=int, default=0)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("add")
    subparsers.add_parser("remove")
    args = parser.parse_args()
    if args.command == "add":
        add(args)
    else:
        remove(args)


if __name__ == "__main__":
    main()
