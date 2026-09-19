# M11 verifier contract

Operation:

```text
participant_runtime_semantics
```

M11 implements the first real **participant runtime**.

It composes the M8 participant/session model and M9 etcd coordination backend
with application-provided state-transition execution:

```text
register participant session
        ->
watch transition messages targeted at that session
        ->
invoke application TransitionHandler
        ->
publish session-owned CurrentState
        ->
complete/remove the message
```

M11 does not run a controller. Transition messages are injected directly by the
integration driver. M13 will compose controller and participant runtimes into
full convergence scenarios.

## Verification authority

M11 uses a strong participant-runtime differential:

```text
real Apache Helix 2.0.1 participant + real ZooKeeper
                         vs
real clustodian ParticipantRuntime + real etcd 3.7.1
```

The Java side must execute state-transition messages through Helix's actual
participant message/state-machine machinery. The Rust side must execute the
same semantic messages through production M11 runtime code.

## Immutable-launcher rule

M11 owns a new Java launcher:

```text
reference/apache-helix/scripts/run-java-m11-integration-oracle.sh
```

Do **not** modify M8 or M10 launchers to add M11 dispatch. Earlier milestone
verification infrastructure remains immutable.

The M11 launcher may reuse the existing pinned Helix build/classpath and test
ZooKeeper infrastructure, but it invokes the M11 participant-runtime oracle
independently.

## Required Rust entrypoints

M11 requires:

```text
reference/apache-helix/scripts/run-rust-participant-runtime.sh <scenario.json>
```

and:

```text
reference/apache-helix/scripts/run-rust-participant-scenario.sh prepare <scenario.json> <state.json>
reference/apache-helix/scripts/run-rust-participant-scenario.sh run     <scenario.json> <state.json>
```

The verifier supplies:

```text
CLUSTODIAN_M11_ETCD_ENDPOINT
CLUSTODIAN_M11_ETCD_PREFIX
CLUSTODIAN_M11_READY_FILE
CLUSTODIAN_M11_EVENT_FILE
CLUSTODIAN_M11_CONTROL_DIR
```

The participant runtime and scenario driver are separate OS processes.

The scenario driver may mutate/read coordination state through production M9
APIs and may control the test TransitionHandler through the supplied test-only
control directory. It must not call participant-runtime processing methods.

In particular the driver must not invoke equivalents of:

```text
process_message()
run_transition()
handle_transition()
publish_transition_result()
```

to make a scenario progress.

## Participant startup

The scenario declares one participant:

```json
{
  "participant": {
    "instance": "node-a",
    "initial_session": "p1",
    "state_model": "LeaderStandby"
  }
}
```

The Rust runtime must:

1. establish a real M9 participant session;
2. create the lease-backed LiveInstance;
3. establish gap-free observation of its message queue;
4. bind the actual session identity to logical label `p1` for the harness;
5. create `CLUSTODIAN_M11_READY_FILE` only after it is safe to inject messages.

The Java oracle must perform the equivalent operation with a real Helix
participant and real ZooKeeper session.

Raw ZooKeeper session IDs and raw etcd lease IDs are not compatibility data.

## Session replacement

For scenarios using:

```json
{
  "op":"expire_and_reconnect",
  "from_session":"p1",
  "to_session":"p2"
}
```

Java must cause a real ZooKeeper session expiration and allow the real Helix
participant to establish its replacement session.

Rust must revoke/lose the real M9 session and exercise the production
ParticipantRuntime's reconnect path. The replacement must have a new
`SessionId` and lease and be bound to `p2`.

Do not emulate replacement by changing a session variable while keeping the old
coordination lease alive.

## Transition messages

The scenario operation:

```json
{
  "op":"send_transition",
  "message_id":"m1",
  "resource":"documents",
  "partition":"documents_0",
  "target_session":"p1",
  "from":"OFFLINE",
  "to":"STANDBY"
}
```

must create a real participant state-transition message through the normal
message storage/queue path.

On Java, inject a real Helix `Message` into the participant's ordinary message
queue. It must be consumed by the real participant message executor and state
machine.

On Rust, write the same production message representation that M10 publishes.
The production ParticipantRuntime must observe it through its M9 watch.

The integration driver must not pass the message directly to the handler.

`message_id` is stable test identity. Java-generated UUIDs or backend-specific
record identities are not compared.

## Target-session fencing

A message is addressed to one participant incarnation.

The ParticipantRuntime must never invoke the application handler for a message
whose target session does not match the active participant session, except if
the pinned Helix 2.0.1 oracle demonstrates a different supported semantic
outcome for the exact scenario.

Most importantly, completion from an old session must never publish state as if
it belonged to a replacement session.

## Application TransitionHandler

M11 introduces the application boundary used to realize a state transition.
Conceptually:

```text
TransitionHandler(
    resource,
    partition,
    from_state,
    to_state,
    transition_id
) -> success | error
```

The exact Rust API is not fixed by the verifier.

The integration adapter supplies a recording test implementation. It may:

```text
return success
return an application error
block until released by the test harness, then return success
```

The handler is test application code. It must not update CurrentState or delete
messages itself; those are ParticipantRuntime responsibilities.

## Handler behavior configuration

Scenarios may declare:

```json
"handler_behaviors": {
  "m1": {"kind":"success"},
  "m2": {"kind":"error"},
  "m3": {"kind":"block_then_success","token":"block-1"}
}
```

A message with no explicit behavior defaults to `success`.

`error` means the application callback throws/returns an error through the
normal participant transition path.

`block_then_success` must signal that the callback has entered, wait until the
scenario driver releases the named token, and then return success.

This blocking facility exists only in integration test code. Production
`clustodian` must not contain file-based transition coordination.

## Success semantics

For a successful transition targeted at the active session, the participant
runtime must use the Helix 2.0.1 oracle as authority for observable lifecycle.
For the supported ordinary transitions this means the application callback is
executed and the resulting CurrentState/message lifecycle matches Helix.

On Rust, the control-plane completion should be session-fenced. Updating
CurrentState and completing/removing the corresponding message must not permit
a stale session to commit after it has lost liveness.

## Transition failure

Application callback failure must follow real Helix 2.0.1 participant
semantics. In particular, the verifier contains a callback-failure scenario and
compares the resulting CurrentState, message lifecycle, and callback record
against Helix rather than hand-authoring a Rust-specific failure policy.

For the LeaderStandby oracle domain this is expected to exercise Helix's
`ERROR` transition semantics.

Do not swallow an application failure and report the requested target state as
successful.

## DROPPED

M11 explicitly exercises the normal path to `DROPPED`.

Do not assume that a permanent `partition = DROPPED` CurrentState record is the
correct observable result. The Java oracle determines the supported Helix
behavior, including removal of partition/resource CurrentState where
applicable.

## From-state mismatch

M11 contains a deliberately mismatched transition scenario.

Do not invent a policy in the verifier. The real Helix 2.0.1 participant oracle
determines:

```text
whether the application callback executes
what happens to the message
what CurrentState is observable afterward
```

The Rust runtime must match that observable behavior for the supported subset.

## Session loss during an in-flight handler

One scenario deliberately does:

```text
message targeted to p1
    -> handler enters and blocks
    -> p1 session expires
    -> participant establishes p2
    -> old handler is released and returns success
```

The old p1 completion must not become authoritative CurrentState for p2.

Cancellation of the application callback is not the correctness mechanism.
Application work may be difficult or impossible to stop immediately. The
correctness boundary is session-fenced control-plane commit.

The exact old-message cleanup behavior remains Helix-oracle-defined and is
compared at the final checkpoint.

## Redelivery / exactly-once boundary

M11 does **not** promise exactly-once application side effects across process
crashes.

There is no atomic transaction spanning arbitrary application data-plane work
and etcd CurrentState/message completion.

A transition may therefore be redelivered after a crash that occurred after
application work but before control-plane completion. The application boundary
must receive a stable transition/message identity sufficient for applications
to implement idempotence where required.

Crash/redelivery orchestration itself is not an M11 acceptance scenario; it is
reserved for M13 hardening.

## Runtime progress / checkpoint synchronization

The scenario driver must not use fixed sleeps as its correctness mechanism.

For ordinary messages, production ParticipantRuntime must expose enough
processed-input progress/status for the driver to wait until every message
mutation preceding a checkpoint has been semantically consumed or completed.
An etcd revision/watermark is one acceptable implementation.

For the blocked-handler scenario, the test handler's explicit entered/returned
signals may additionally be used to coordinate the deliberate race.

Timing values themselves are not compatibility data.

## Compared output

Each implementation emits:

```json
{
  "operation":"participant_runtime_semantics",
  "checkpoints":[
    {
      "id":"after-bootstrap",
      "live_instances":{"node-a":"p1"},
      "active_current_state":{
        "node-a":{
          "session":"p1",
          "resources":{
            "documents":{"documents_0":"STANDBY"}
          }
        }
      },
      "pending_messages":[],
      "handler_events":[
        {
          "message_id":"m1",
          "resource":"documents",
          "partition":"documents_0",
          "from":"OFFLINE",
          "to":"STANDBY",
          "outcome":"success"
        }
      ]
    }
  ]
}
```

### live_instances

Maps InstanceId to logical session labels.

### active_current_state

Uses M8/M9 active-session semantics. Stale session state may physically exist
but is excluded.

### pending_messages

Contains semantic messages still present in the participant's normal message
queue at the checkpoint:

```text
message_id
resource
partition
target_session
from
to
message_type
```

Raw UUIDs, timestamps, backend revisions, and transport metadata are excluded.

Message ordering is not semantic.

### handler_events

Records completed application callback invocations:

```text
message_id
resource
partition
from
to
outcome = success | error
```

Scheduling order across independent partitions is not semantic, so events are
compared as a sorted multiset. Duplicate callback execution remains visible
because event cardinality is preserved.

A stale-session message that never invokes application code therefore has no
handler event.

## Java oracle requirements

The M11 Java oracle should use the actual pinned Helix 2.0.1 equivalents of:

```text
MockParticipantManager / real participant manager
StateMachineEngine
StateModelFactory / StateModel
HelixTaskExecutor
state-transition Message handling
CurrentState publication
real test ZooKeeper/session expiry helpers
```

The recording StateModel is allowed to implement application callbacks and test
blocking/failure behavior. It must not reproduce Helix message validation,
CurrentState update, ERROR handling, or message completion in adapter code.

Those observable results must come from real Helix participant machinery.

## Explicitly out of scope

M11 does not implement or verify:

```text
controller runtime during participant scenarios
controller election/failover
multiple active controllers
full controller -> participant -> controller convergence
application snapshot transfer
WAL/log catch-up
replica validation semantics belonging to Clustodian
application RPC transport
exactly-once application side effects across crashes
administrative ERROR reset/recovery APIs
```

M12 owns controller election/failover.
M13 owns end-to-end convergence and crash/restart hardening.
