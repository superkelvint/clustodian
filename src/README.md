# Helix source tree

This is the library's internal control-plane implementation. The crate-level
README explains the product scope; this file explains where to begin in the
source.

## Start here

Read the modules in this order:

1. `model/` — the domain vocabulary and immutable snapshots.
2. `rebalance/` — desired placement and desired replica states.
3. `transition/` — the values passed between controller stages.
4. `controller/` — transition planning, selection, throttling, and publication.
5. `coordination/` — etcd persistence, leases, revisions, and watch recovery.
6. `participant.rs` — application-facing execution of published messages.
7. `routing/` — read-only lookup over the published external view.

The first four modules describe the deterministic semantic path. The last
three connect that path to runtime behavior or consumers.

## One complete cycle

```text
etcd snapshot
    -> CurrentState + live sessions
    -> rebalance placement
    -> BestPossibleState
    -> TransitionRequest
    -> selected/throttled work
    -> TransitionMessage in etcd
    -> participant validates and calls the application
    -> session-owned CurrentState
```

When reading a function, ask which value in this cycle it consumes and which
value it produces. If a function crosses from logical identity to a physical
key, lease, revision, or node index, that conversion should stay inside the
owning module.
