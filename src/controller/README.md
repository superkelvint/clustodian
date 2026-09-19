# Controller

This directory contains the code that decides what the cluster should do next.
It does not move application data and it does not execute transitions. It reads
snapshots of cluster state, computes safe work, and publishes transition
messages for participants.

## The mental model

For each resource and partition, the controller compares:

```text
CurrentState       what participants report now
IdealState         which instances should host the replicas
StateModel         which states and transitions are legal
BestPossibleState  what the controller wants to see next
```

The deterministic pipeline is:

```text
current + ideal + state model
    -> best possible state
    -> transition requests
    -> cardinality-safe selection
    -> intermediate state and throttling
    -> published transition messages
```

The important distinction is between a transition request and a published
message. A request is an in-memory semantic decision. A published message is
work that survives controller cycles until the participant reports the new
state.

## Files to read

- `message_generation.rs` turns current and desired states into one-step
  `TransitionRequest` values.
- `message_selection.rs` applies state-model cardinality rules and accounts for
  transitions already in flight.
- `message_throttle.rs` computes the M6 intermediate state and applies cluster,
  resource, and instance quotas.
- `intermediate_state.rs` holds the immutable state produced during planning.
- `runtime.rs` connects the deterministic stages to etcd watches and output
  publication.

Start with `message_generation.rs`, then read selection and throttling. Read
`runtime.rs` last: it is orchestration and persistence glue around the simpler
algorithms.

## Runtime concepts

`ControllerRuntime` keeps a small amount of mutable loop state: pending
published messages and the sessions seen during the previous reconciliation.
Every reconciliation uses one pinned coordination snapshot. A session
replacement causes the runtime to replan without treating old session work as
current.

Startup, ordinary changes, and session replacement are deliberately separate
reconciliation modes. Startup skips normal throttling so the initial state can
be published; later cycles apply the configured quotas.

The controller only publishes control-plane facts. The participant and the
application own the actual state transition.
