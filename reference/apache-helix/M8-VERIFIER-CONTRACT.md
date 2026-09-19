# M8 verifier contract

Operation:

```text
participant_session_semantics
```

M8 verifies the supported Apache Helix 2.0.1 participant/session semantics
against a **real Apache Helix participant running on a real ZooKeeper test
server**.

M8 is still a Helix-semantic milestone. It does **not** introduce etcd. M9 will
map these semantics onto etcd leases, revisions, watches, and transactions.

The semantic concepts are:

```text
InstanceId
    stable participant identity

SessionId
    one concrete participant incarnation

LiveInstance
    the currently live SessionId for an InstanceId

CurrentState
    replica state owned by a specific participant session
```

The essential invariant is that a new participant session is a new incarnation.
State from the old incarnation must never remain authoritative merely because
the stable `InstanceId` is the same.

## Important Helix reconnect behavior

M8 does **not** assume that a replacement session simply starts with an empty
CurrentState.

Apache Helix participant new-session handling may carry partition membership
from previous CurrentState into the replacement session while resetting those
partitions according to the state model's initial-state semantics. The Java
oracle exercises the real Apache Helix 2.0.1 participant code, and the Rust
implementation must match the resulting observable semantics.

The verifier therefore compares the active CurrentState produced by Helix after
real disconnect/reconnect and real ZooKeeper session expiration. It does not
hand-author an expected carry-over policy in the Java adapter.

## Immutable reference infrastructure

The following M8 files are reference infrastructure and are not implementation
targets:

```text
reference/apache-helix/verify-m8.sh
reference/apache-helix/verify-m8-oracle.sh
reference/apache-helix/M8-VERIFIER-CONTRACT.md
reference/apache-helix/M8-IMMUTABLE.sha256
reference/apache-helix/scenarios/m8/
reference/apache-helix/scripts/build-java-integration-oracle.sh
reference/apache-helix/scripts/run-java-integration-oracle.sh
reference/apache-helix/scripts/compare-m8-results.py
reference/apache-helix/integration-oracle/
```

The verifier checks the immutable manifest before running the differential
suite.

## Java authority

The Java side must remain a thin adapter around the pinned Apache Helix 2.0.1
source tree and its integration-test machinery.

It uses the real Helix test infrastructure for:

```text
ZooKeeper test server
MockParticipantManager
ZkTestHelper.expireSession(...)
LiveInstance
CurrentState
ReadClusterDataStage
ResourceComputationStage
CurrentStateComputationStage
```

The adapter may:

```text
parse scenarios
create the test cluster and InstanceConfig records
install scenario resources as real Helix IdealState metadata
start/stop real Helix participants
seed CurrentState records for a named session
force a real ZooKeeper session expiration
run Helix ReadClusterDataStage + ResourceComputationStage + CurrentStateComputationStage
canonicalize nondeterministic session identifiers
serialize JSON
```

It must not implement participant reconnect/carry-over/session selection itself.
In particular, it must not derive active CurrentState by manually selecting a
CURRENTSTATES directory from the LiveInstance session.

## Raw session IDs

ZooKeeper session IDs are nondeterministic and are not compatibility data.

Scenario files assign logical labels such as:

```text
a1
a2
b1
```

The Java oracle binds those labels to the real ZooKeeper/Helix session IDs it
observes. The Rust side binds the same labels to real Rust `SessionId` values.

The comparator sees only logical labels and requested equality relationships;
it never compares raw ZooKeeper session IDs.

## Scenario resources

Resources are declarations used when constructing test CurrentState:

```json
{
  "name": "documents",
  "state_model": "LeaderStandby",
  "partitions": ["documents_0", "documents_1"]
}
```

`state_model` defaults to `LeaderStandby` if omitted.

The Java oracle installs each declaration as real Helix `IdealState` metadata
in ZooKeeper. `ResourceComputationStage` then derives the `Resource` event input
used by `CurrentStateComputationStage`. The preference-list contents are only
test plumbing for materializing the declared partitions; placement behavior is
not part of M8.

CurrentState itself is represented by real Helix `CurrentState` objects and
real state-model definitions installed by Helix cluster creation.

## Scenario operations

### `connect`

```json
{"op":"connect","instance":"node-a","session":"a1"}
```

Start a real `MockParticipantManager`, wait for its real `LiveInstance`, and
bind the logical label to the session recorded by Helix.

### `disconnect`

```json
{"op":"disconnect","instance":"node-a","session":"a1"}
```

Gracefully stop the real participant and wait for its `LiveInstance` to
vanish.

### `expire_and_reconnect`

```json
{
  "op":"expire_and_reconnect",
  "instance":"node-a",
  "from_session":"a1",
  "to_session":"a2"
}
```

Use the real Helix `ZkTestHelper.expireSession(...)` path on the participant's
ZooKeeper client. Wait until Helix publishes a replacement `LiveInstance` with
a different underlying session, then bind `a2` to it.

Changing a Java variable or rewriting the `LiveInstance` record directly is not
an acceptable substitute.

### `publish_current_state`

```json
{
  "op":"publish_current_state",
  "instance":"node-a",
  "session":"a1",
  "resource":"documents",
  "states":{"documents_0":"LEADER"}
}
```

Seed a real Helix `CurrentState` record under the specified **currently live**
session. This is test setup; it does not emulate participant reconnect logic.

### `inject_session_current_state`

```json
{
  "op":"inject_session_current_state",
  "instance":"node-a",
  "session":"a1",
  "resource":"documents",
  "states":{"documents_0":"LEADER"}
}
```

Test-only metadata injection. Unlike `publish_current_state`, it may write under
a stale session label. It exists to prove that old-session metadata does not
become the active CurrentState of a replacement incarnation.

### `checkpoint`

```json
{"op":"checkpoint","id":"after-reconnect"}
```

Capture the observable semantic view.

The Java oracle derives this through the real Helix controller data path:

1. create a fresh `ResourceControllerDataProvider`;
2. run `ReadClusterDataStage` against the real ZooKeeper-backed cluster;
3. run `ResourceComputationStage` so Helix derives the Resource map from the
   real `IdealState` metadata;
4. run `CurrentStateComputationStage`;
5. serialize `LiveInstance` data from the refreshed cache and active replica
   state from the resulting `CurrentStateOutput`.

The adapter never performs the semantic operation “read LiveInstance session,
then manually choose that CURRENTSTATES session directory,” and it does not
manually inject the Resource event attribute required by
`CurrentStateComputationStage`. Session filtering, stale-session treatment, and
the controller-stage dependency chain therefore come from Apache Helix itself.

Because participant new-session work is asynchronous, checkpoints poll this
real stage-derived snapshot until it is stable before serializing it. Stability
waiting does not define any state semantics.

## Oracle preflight

Before Codex or another implementation agent changes Rust, run:

```bash
./reference/apache-helix/verify-m8-oracle.sh
```

This immutable preflight builds the pinned Java integration oracle and executes
every M8 scenario twice against real Helix + ZooKeeper. The two semantic outputs
are compared with `compare-m8-results.py`. It exercises no Rust implementation.

If this command fails, the reference infrastructure is broken and the Rust
implementation task must stop. The implementation agent must not repair the
Java oracle.

## Compared output

```json
{
  "operation": "participant_session_semantics",
  "checkpoints": [
    {
      "id": "after-reconnect",
      "live_instances": {
        "node-a": "a2"
      },
      "active_current_state": {
        "node-a": {
          "session": "a2",
          "resources": {
            "documents": {
              "documents_0": "OFFLINE"
            }
          }
        }
      }
    }
  ],
  "session_comparisons": [
    {"left":"a1","right":"a2","equal":false}
  ]
}
```

The concrete state shown above is illustrative of a state-model reset; the
actual expected output is always generated by the real Helix 2.0.1 oracle.
There are no checked-in hand-authored expected result files.

At each checkpoint:

- `live_instances` is `InstanceId -> logical SessionId` for real current
  `LiveInstance` records;
- `active_current_state` is the state Helix's real
  `CurrentStateComputationStage` considers current for the live participants;
- a live participant with no CurrentState records for its live session has an
  empty `resources` object;
- checkpoint order is semantic and follows scenario chronology.

## What M8 verifies

The supplied scenarios cover:

```text
connect -> LiveInstance
disconnect -> no LiveInstance
same InstanceId -> distinct replacement SessionId
real ZooKeeper expiration -> replacement session
CurrentState session ownership
native Helix reconnect/current-state carry-over behavior
old-session state not authoritative for a new session
independent participant sessions
multiple resources across session replacement
repeated incarnation replacement
reconnect with no previous state
stale old-session metadata remaining non-authoritative
```

## Explicitly out of scope

M8 does not implement or verify:

```text
etcd leases / watches / revisions / transactions
coordination-backend abstraction
controller event loop
participant transition-delivery runtime
controller election
message execution / acknowledgement
exact ZooKeeper paths as a Rust API
raw ZooKeeper session-id representation
ZooKeeper API compatibility
```

Those belong to M9+.
