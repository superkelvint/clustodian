#!/usr/bin/env python3
"""Create the fixed role paths used by the M13 Docker stack."""

import json
import urllib.error
import urllib.request


def create(name, listen, upstream):
    payload = json.dumps(
        {"name": name, "listen": f"0.0.0.0:{listen}", "upstream": upstream}
    ).encode()
    request = urllib.request.Request(
        "http://127.0.0.1:8474/proxies",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=5).read()
    except urllib.error.HTTPError as error:
        if error.code != 409:
            raise


def main():
    # Each SUT process gets an independent path to every etcd member.  Keep
    # both the small default profile (controller-a..c) and the scale profile
    # (controller-0..2, node-0..249) provisioned by the shared Docker stack.
    for index, controller in enumerate(("controller-a", "controller-b", "controller-c")):
        for member in range(1, 4):
            create(
                f"{controller}-etcd-{member}",
                12379 + index * 3 + member - 1,
                f"etcd{member}:2379",
            )
    for index in range(3):
        for member in range(1, 4):
            create(
                f"controller-{index}-etcd-{member}",
                12388 + index * 3 + member - 1,
                f"etcd{member}:2379",
            )
    for index in range(250):
        for member in range(1, 4):
            create(
                f"node-{index}-etcd-{member}",
                13379 + index * 3 + member - 1,
                f"etcd{member}:2379",
            )


if __name__ == "__main__":
    main()
