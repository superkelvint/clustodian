# M10 verifier contract

M10 implements the first **watch-driven controller runtime**.

It consumes the M9 etcd coordination backend, observes authoritative metadata/liveness changes, runs the already-verified M1-M7 controller computations, and publishes control-plane outputs.

M10 does **not** implement participant transition execution or controller election/failover.

## Verification authority

M10 uses a strong scenario differential:

```text
Apache Helix 2.0.1 controller + real ZooKeeper
                     vs
single clustodian controller runtime + real etcd 3.7.1
```

The two implementations may use completely different watch/event machinery. The verifier compares only observable Helix semantics.

## Required Rust entrypoints

M10 requires two separate Rust-side integration entrypoints.

### Controller runtime

```text
reference/apache-helix/scripts/run-rust-controller-runtime.sh
```

The verifier starts this as a long-running process with:

```text
CLUSTODIAN_M10_ETCD_ENDPOINT=http://127.0.0.1:<port>
CLUSTODIAN_M10_ETCD_PREFIX=/clustodian/m10/...
CLUSTODIAN_M10_READY_FILE=/tmp/.../ready
```

The runtime must use the real M9 etcd backend and production controller-runtime code.

Once its initial authoritative snapshot has been loaded and all required watches have been established without a snapshot/watch gap, it creates `CLUSTODIAN_M10_READY_FILE`.

The runtime then remains alive until terminated by the verifier.

### Scenario driver

```text
reference/apache-helix/scripts/run-rust-controller-scenario.sh prepare <scenario.json> <state.json>
reference/apache-helix/scripts/run-rust-controller-scenario.sh run     <scenario.json> <state.json>
```

It receives the same endpoint/prefix environment.

`prepare` executes the scenario's `setup` operations before the controller starts. It persists only harness bookkeeping such as logical-session-label bindings in the provided state file.

`run` executes the scenario's post-start `steps`, waits for controller processing at checkpoints, and emits the result JSON.

The scenario driver may use production M9 coordination APIs to mutate/read authoritative coordination state. It must **not** depend on or invoke controller pipeline/runtime APIs. In particular it must never call `reconcile()`, `run_once()`, or an equivalent controller method after a scenario mutation.

The verifier deliberately runs the controller and scenario driver as separate OS processes so that M10 exercises watch-driven runtime behavior rather than a test adapter manually triggering controller computation.

## Java integration oracle

Add:

```text
reference/apache-helix/scripts/run-java-m10-integration-oracle.sh
```

The M10 launcher may reuse the existing Java oracle build/classpath
infrastructure, but it independently invokes the real M10 controller-runtime
oracle class. M8 verification infrastructure, including
`run-java-integration-oracle.sh`, remains immutable.

The M10 launcher supports operation:

```text
controller_runtime_semantics
```

The Java side must use a real Apache Helix 2.0.1 controller with real test ZooKeeper.

The Java test driver may seed resource metadata, liveness/session records, and CurrentState required by the scenario, but controller outputs must come from the real Helix controller runtime. It must not call controller stages itself to synthesize expected output.

M10 intentionally does not require a functioning participant transition executor. Controller-generated messages remain pending and are observed as control-plane output. M11 will implement participant message receipt/execution.

## Scenario structure

A scenario has:

```json
{
  "scenario_version": 1,
  "operation": "controller_runtime_semantics",
  "instance_configs": [
    {"name":"node-a","zone":"zone-a"},
    {"name":"node-b","zone":"zone-b"}
  ],
  "setup": [],
  "steps": []
}
```

`instance_configs` declares configured cluster instances. It is distinct from liveness.

### Resource definition

`put_resource` contains a resource definition such as:

```json
{
  "op": "put_resource",
  "resource": {
    "name": "documents",
    "state_model": "LeaderStandby",
    "placement": {
      "kind": "SEMI_AUTO",
      "replicas": 2,
      "preference_lists": {
        "documents_0": ["node-a", "node-b"]
      }
    }
  }
}
```

The optional M10 CRUSH scenario uses:

```json
{
  "placement": {
    "kind": "CRUSH",
    "replicas": 2,
    "partitions": ["documents_0", "documents_1"]
  }
}
```

The Java oracle must configure the actual pinned Helix 2.0.1 `CrushRebalanceStrategy` for this resource; it must not replace that behavior with adapter-side placement.

## Supported setup/step operations

### `put_resource`

Create or update resource/IdealState metadata from the supplied semantic resource definition.

A post-start `put_resource` must be observed by the controller through its runtime watch/event path.

### `connect`

```json
{"op":"connect","instance":"node-a","session":"a1"}
```

Make the instance live and bind the logical session label to the real session identity. On the Rust side this uses the M9 lease-backed registration primitive. On the Java side use real ZooKeeper session/liveness semantics suitable for a controller integration test.

The liveness-only M10 driver must not execute controller transition messages; M11 owns participant runtime behavior.

### `disconnect`

Remove/revoke the active incarnation's liveness while leaving historical session CurrentState physically present where the backend normally does so.

### `expire_and_reconnect`

Expire/replace the active incarnation and bind a new logical session label. The replacement session must be distinct from the old one. M8/M9 semantics remain authoritative.

### `publish_current_state`

Publish CurrentState owned by the specified active session through the session-fenced path.

### `set_transition_throttle`

Configure the supported M6 state-transition throttle semantics in persistent cluster metadata.

### `clear_transition_throttles`

Remove the transition-throttle configuration used by the scenario.

### `checkpoint`

Wait until the controller has processed every authoritative input mutation preceding the checkpoint, then capture the semantic state below.

On the Rust side this must use a controller-runtime processed-revision/watermark or an equivalent production synchronization mechanism. Fixed sleeps are not sufficient as the correctness mechanism.

The Java oracle may use Helix's existing integration/verifier/polling infrastructure to wait for the corresponding controller reaction. Exact timing is not compatibility data.

## Compared checkpoint output

```json
{
  "operation": "controller_runtime_semantics",
  "checkpoints": [
    {
      "id": "after-loss",
      "live_instances": {
        "node-b": "b1"
      },
      "active_current_state": {
        "node-b": {
          "session": "b1",
          "resources": {
            "documents": {
              "documents_0": "STANDBY"
            }
          }
        }
      },
      "external_view": {
        "documents": {
          "documents_0": {
            "node-b": "STANDBY"
          }
        }
      },
      "pending_transitions": [
        {
          "resource": "documents",
          "partition": "documents_0",
          "instance": "node-b",
          "target_session": "b1",
          "from": "STANDBY",
          "to": "LEADER",
          "message_type": "STATE_TRANSITION"
        }
      ]
    }
  ]
}
```

### `live_instances`

Uses logical session labels, never raw ZooKeeper session IDs or etcd lease IDs.

### `active_current_state`

Uses the M8/M9 active-session semantics. Stale session state may exist physically but must not appear here.

### `external_view`

The externally observable state produced by the controller. This remains current/actual state, not BestPossibleState.

### `pending_transitions`

The semantic state-transition messages currently published by the controller and not executed by a participant runtime.

Message UUIDs, timestamps, source controller session, storage revisions, and transport metadata are excluded.

`target_session` **is semantic in M10** and is compared through logical session labels. A transition sent to a stale participant incarnation is incorrect even if its resource/partition/from/to fields otherwise match.

Pending-transition list order is not semantic and is canonicalized as a set of semantic tuples.

## Runtime observation requirement

M10 is specifically a runtime/watch milestone.

The Rust controller must:

1. obtain an authoritative initial coordination snapshot;
2. establish gap-free observation using the M9 snapshot/watch primitives;
3. react automatically to relevant metadata/liveness/CurrentState changes;
4. rerun the supported controller pipeline;
5. publish resulting ExternalView and transition messages;
6. advance a processed input revision/watermark only after outputs corresponding to that input snapshot are committed.

The scenario driver may wait for this watermark. It may not trigger the controller computation itself.

## Single-controller scope

Exactly one Rust controller runtime is started for each M10 scenario.

M10 does not verify controller election, multiple-controller exclusion, or stale-controller fencing. Those belong to M12.

The Java oracle may use whatever single-controller test machinery Helix 2.0.1 requires, but failover/election is not part of the compared M10 contract.

## Explicitly out of scope

M10 does not implement or verify:

```text
participant transition receipt/execution
participant message acknowledgement
automatic CurrentState publication after a message
controller election/failover
multiple simultaneous controllers
Clustodian transition callbacks/data movement
exact event ordering inside the controller
exact watch callback count
ZooKeeper/etcd revision equivalence
exact convergence timing
```

Those belong to M11-M13.
