# M9 verifier contract

M9 is the first milestone with **two verification authorities**.

1. Apache Helix 2.0.1 + ZooKeeper remains authoritative for the participant/session semantics established in M8.
2. A pinned real etcd server is authoritative for backend mechanics that Apache Helix cannot exercise: leases, revisions, transactions, watches, and compaction recovery.

The verifier pins **etcd 3.7.1**.

M9 does not implement the controller runtime. It provides the coordination backend that M10 will consume.

## Required runner

M9 adds one Rust integration entrypoint:

```text
reference/apache-helix/scripts/run-rust-etcd-integration.sh <scenario.json>
```

The verifier starts a real single-node etcd server and exports:

```text
CLUSTODIAN_M9_ETCD_ENDPOINT=http://127.0.0.1:<port>
CLUSTODIAN_M9_ETCD_PREFIX=/clustodian/m9/...
```

The runner must use that real endpoint. It must not substitute an in-memory backend.

`CLUSTODIAN_M9_ETCD_PREFIX` is an isolated key prefix unique to one scenario/run. All keys written by the runner must remain under that prefix.

The runner accepts two scenario operations.

---

## Lane A: M8 semantic preservation on etcd

Operation:

```text
participant_session_semantics
```

The scenario format and result format are exactly the M8 contract.

For every M8 scenario, M9 compares:

```text
Apache Helix 2.0.1 + real ZooKeeper
                vs
clustodian M8 session semantics + real etcd backend
```

using the existing:

```text
compare-m8-results.py
```

The etcd-backed implementation must preserve the observable M8 semantics:

```text
participant connection -> LiveInstance
participant loss -> LiveInstance disappears
reconnect -> new SessionId
stable InstanceId across incarnations
CurrentState belongs to SessionId
stale-session CurrentState is not active
```

Raw ZooKeeper session IDs and raw etcd LeaseIds are not compared.

### SessionId is not LeaseId

`SessionId` remains the backend-independent incarnation identity established in M8.

The etcd backend may internally associate a session with a lease, but production APIs must not define:

```text
SessionId == LeaseId
```

as the semantic model.

---

## Lane B: etcd-native backend correctness

Operation:

```text
etcd_coordination_semantics
```

Each M9 native scenario has:

```json
{
  "scenario_version": 1,
  "operation": "etcd_coordination_semantics",
  "case": "lease_expiry_removes_live_instance",
  "parameters": {},
  "expect": {
    "live_before_expiry": true,
    "live_after_expiry": false
  }
}
```

The Rust runner executes the case against the real etcd endpoint and emits:

```json
{
  "operation": "etcd_coordination_semantics",
  "case": "lease_expiry_removes_live_instance",
  "observations": {
    "live_before_expiry": true,
    "live_after_expiry": false
  }
}
```

The result may contain additional diagnostic observations. `compare-m9-etcd-results.py` requires every field named by `expect` to exist and match exactly.

## Required backend semantics

### Lease-backed liveness

A live participant is represented by a lease-backed LiveInstance record.

Lease expiration and explicit revoke must remove that liveness record through real etcd lease behavior.

Keepalive must preserve the same live session rather than silently creating a new one.

### Atomic registration

Two concurrent attempts to register the same `InstanceId` must not both become live.

Registration must use an etcd transaction or an equivalently atomic etcd primitive. A read-then-put race is not acceptable.

### Transactional session fencing

A session-owned CurrentState write must be conditionally authorized against the currently live `SessionId` for that `InstanceId` in the same etcd transaction.

Conceptually:

```text
IF live/<instance> == this SessionId
THEN write current-state/<instance>/<session>/...
ELSE reject stale session
```

Checking a locally cached session before an unconditional write is not sufficient.

### Persistent vs lease-backed state

Participant liveness is lease-backed.

Persistent metadata and historical session-owned CurrentState are not required to disappear when a participant lease expires.

The active semantic CurrentState is derived from the current LiveInstance session, so stale stored state must remain inactive.

### Revisions / CAS

M9 exposes enough revision information to perform optimistic compare-and-update.

A write conditioned on a stale revision must fail rather than overwrite a newer value.

### Snapshot + watch

The backend must support the semantic pattern:

```text
linearizable snapshot at revision R
        ->
watch from R + 1
```

The verifier cases require no semantic gap between the snapshot and subsequent watched mutations.

etcd-specific event batching is not compatibility data; ordered semantic mutations are.

### Watch resume

After the consumer stops/reconnects, it must be able to resume from the last processed revision without silently missing later mutations.

### Compaction recovery

If the requested historical watch revision has been compacted, the backend must surface/recover that condition by rebuilding an authoritative snapshot and resuming observation from the new snapshot revision.

It must not silently continue with a gap.

## Pinned etcd server

`verify-m9.sh` starts a real single-node etcd server itself.

The default binary is:

```text
etcd
```

and may be overridden only by path through:

```text
CLUSTODIAN_M9_ETCD_BIN
```

The binary must report exactly:

```text
3.7.1
```

The verifier chooses private temporary client/peer ports and deletes the temporary etcd data directory after the run.

No external/shared etcd cluster is used by the acceptance verifier.

## Explicitly out of scope

M9 does not implement or verify:

```text
controller watch/event loop
running M1-M7 automatically after changes
participant transition delivery
participant transition execution
message acknowledgements
controller election
standby controller behavior
Clustodian RPC/data movement
multi-member etcd failure/reconfiguration
TLS/auth operational deployment
ZooKeeper API compatibility
```

Those belong to M10+ or production hardening.
