# Coordination

This module is the boundary between Helix control-plane logic and a shared
coordination store. The current backend is etcd, but the concepts here are
intentionally expressed in terms of snapshots, sessions, revisions, metadata,
and watches rather than application data.

## The mental model

There are two different kinds of identity in this module:

```text
SessionId  logical identity of one participant incarnation
Revision   physical etcd modification position
```

Never use a revision as a session or a session as a revision. A session fences
which participant may publish `CurrentState`; a revision fences which version
of a metadata value or queue a writer has observed.

The key consistency pattern is:

```text
read one snapshot
    -> start a watch after that snapshot's revision
    -> process later events without a gap
```

This lets the controller and participant reason about a coherent point in
time instead of mixing values from different reads.

## Files to read

- `etcd/` contains the concrete backend and its lease, CAS, snapshot, and watch
  implementation.
- `mod.rs` is the coordination namespace and public backend façade.

Read `etcd/README.md` for the storage layout and recovery rules.

## What coordination owns

Coordination owns durable control-plane records, participant liveness, session
fencing, queue revisions, and recovery after disconnected or compacted watches.
It does not own document data, replica synchronization, or the application
operation that makes a state transition real.

The deterministic controller stages should remain usable without this module.
That separation is useful both for tests and for a future backend: the local
algorithms consume explicit values, while coordination supplies those values
and persists the results.
