# Clustodian M13 runtime

The M13 runner uses the real Helix controller, participant, and observer
processes. Its coordination cluster can run either under Docker Compose or
directly on the host:

```bash
# Auto-select Docker when its daemon is available; otherwise use local etcd.
reference/apache-helix/verify-m13.sh

# Force either runtime.
CLUSTODIAN_M13_RUNTIME=local reference/apache-helix/verify-m13.sh
CLUSTODIAN_M13_RUNTIME=docker reference/apache-helix/verify-m13.sh
```

Local mode requires `etcd` and `etcdctl` on `PATH` (or the
`CLUSTODIAN_M13_ETCD_BIN` and `CLUSTODIAN_M13_ETCDCTL_BIN` overrides). It starts three
real etcd members on loopback ports 23791–23793 and records their PIDs and
endpoints in `target/clustodian-chaos/runtime.env`. The shared stack scripts stop
both runtimes, so local runs do not require Docker or Toxiproxy.

Docker mode additionally provides Toxiproxy and is required for network-fault
actions. Local PR smoke exercises the recoverable process, session, callback,
clock, failpoint, and coordination paths; network and etcd-member faults remain
in the Docker-backed nightly matrix.

The nightly generator has a deterministic fault matrix covering network
disconnect, latency with jitter, timeout, etcd member restart, compaction,
clock suspension, all callback modes, and failpoints. Faults remain active
through the action they precede and are cleaned up afterward. Each controller
and participant process has three independent etcd proxy paths, so a targeted
network fault can isolate one process from all etcd members. The observer child
seeds a namespace watch from one consistent snapshot and checks safety and
derived-state invariants at every observed MVCC revision; the driver consumes
those results. Settled convergence additionally checks ExternalView/current-
state equality, routing parity, replica cardinality, leader cardinality, and
expected process liveness.

The normal verifier runs M12 first. For focused local M13 iteration only, use
`CLUSTODIAN_M13_SKIP_PREREQUISITE=1`; this does not change the normal gate.

The deterministic crash-window suite can be run independently with
`tools/clustodian-chaos/scripts/run-crash-matrix.sh`. The normal M13 verifier runs
the same suite before randomized traces; set `CLUSTODIAN_M13_CRASH_MATRIX=0` only
when iterating locally.

The normal verifier also runs `clustodian-chaos quorum-loss`, which stops two
etcd members, checks that the unavailable interval makes no safety-violating
progress, restores one member to regain quorum, then restores the final member
and requires convergence. Set `CLUSTODIAN_M13_QUORUM_LOSS=0` for local
iteration. Controller SIGTERM is intentionally tested as process termination
followed by lease expiry; participants explicitly revoke their LiveInstance
session during graceful shutdown.

## Fixed scale profiles

Scale acceptance is intentionally separate from randomized chaos. The fixed
profiles create the same workload on every run and record per-action
reconciliation latency, etcd revision growth, pending-transition count,
ExternalView size, observer watch events, process RSS, task count, and open
file descriptors:

```bash
tools/clustodian-chaos/scripts/run-scale.sh
CLUSTODIAN_M13_SCALE_PROFILE=scale-250-50k tools/clustodian-chaos/scripts/run-scale.sh
```

`scale-100-10k` is 100 participants, 3 controllers, 100 resources, 10,000
partitions, and RF=3. `scale-250-50k` is 250 participants, 3 controllers,
250 resources, 50,000 partitions, and RF=3. Thresholds are explicit in
`check-scale-metrics.py` and can be overridden with the corresponding
`CLUSTODIAN_SCALE_*` variables for hardware-specific baselines.

Every scale trace ends with a 30-second `AssertIdle` window. Its default
semantic-write limit is zero (`CLUSTODIAN_CHAOS_IDLE_MAX_REVISION_DELTA`), and it
also bounds aggregate process CPU jiffies (`CLUSTODIAN_CHAOS_IDLE_MAX_CPU_JIFFIES`)
and records RSS, task, and FD deltas. Lease keepalives are not key-value
writes and therefore do not increase the semantic revision.

M13 also runs `reference/apache-helix/scripts/check-m13-raw-oracle.py`. It reads raw
etcd records through an independent etcdctl client and checks session fencing,
pending-transition ownership, controller lease presence, and leader safety
without constructing `ClusterSnapshot`. Set `CLUSTODIAN_M13_RAW_ORACLE=0` only for
local debugging; CI keeps it enabled.
