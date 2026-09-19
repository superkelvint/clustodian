# Model

This directory defines the vocabulary used by every other Helix module. It is
the best place to start if the rest of the code feels confusing.

## The mental model

The model separates values that look similar but have different meanings:

```text
InstanceId / PartitionId / ResourceId  logical cluster identities
SessionId                              one participant incarnation
Revision                               coordination-store position
State                                  application-defined state name
CurrentState                           observed replica states
BestPossibleState                      desired replica states
IdealState                             desired placement and replica order
ExternalView                           published aggregation of observations
```

Most model values are immutable after construction. Builders are mutable only
while a value is being assembled; `build` is the publication boundary. This
is why controller code can pass snapshots between stages without sharing a
mutable global runtime object.

## Files to read

- `identity.rs` defines the validated logical IDs and opaque `SessionId`.
- `state.rs` defines state names, transition identities, and cardinalities.
- `state_model.rs` validates a state machine and computes one-step next hops.
- `ideal_state.rs` defines ordered replica placement.
- `current_state.rs` and `best_possible_state.rs` hold observed and desired
  replica maps.
- `external_view.rs` aggregates observed states for routing and publication.
- `session.rs` models live participant sessions and fences stale state.
- `builtins.rs` contains the built-in `LeaderStandby` definition.

Read identity and state first, then the three state maps, and session last.
`replica_state.rs` is an internal helper that owns the nested map invariant and
rejects duplicate partition/instance entries.

## Invariants worth remembering

- Identifiers are validated at construction and their fields are private.
- A successful state-model build contains `DROPPED` and a path from every
  other state to `DROPPED`.
- `CurrentState` is what happened; `BestPossibleState` is what the controller
  wants; neither is authoritative application data.
- A participant's old session may leave stale records behind, but only the
  currently live session appears in the active snapshot.
- Physical storage coordinates never appear in these logical model types.
