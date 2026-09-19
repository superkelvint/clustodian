# M7 verifier contract

Operation:

```text
compute_external_view_and_routing
```

M7 verifies the deterministic Apache Helix 2.0.1 **EXTERNALVIEW-backed** snapshot path:

```text
CurrentState
    -> ExternalView

ExternalView + InstanceConfig set
    -> immutable RoutingSnapshot
    -> state-based instance lookup
```

The Java oracle must invoke the real Helix 2.0.1 `ExternalViewComputeStage` path and
the real ExternalView-backed routing-table implementation. Adapter code may
construct the `Resource`, `Partition`, `CurrentStateOutput`, cache/config and
other inputs required by those APIs, but it must not recreate ExternalView
aggregation or routing logic itself.

M7 covers the routing source corresponding to `PropertyType.EXTERNALVIEW` only.
It does **not** cover the separate `PropertyType.CURRENTSTATES` path, including
its participant session / `LiveInstance` semantics.

M7 is snapshot-based. It does **not** test ZooKeeper listeners, watches,
periodic refresh, spectator process lifecycle, participant sessions, or
controller convergence.

## Scenario input

Each scenario has this shape:

```json
{
  "scenario_version": 1,
  "operation": "compute_external_view_and_routing",
  "instances": ["node-a", "node-b"],
  "resources": [
    {
      "name": "documents",
      "current_state": {
        "documents_0": {
          "node-a": "LEADER",
          "node-b": "STANDBY"
        }
      }
    }
  ],
  "routing_queries": [
    {
      "id": "p0-leader",
      "resource": "documents",
      "partition": "documents_0",
      "state": "LEADER"
    }
  ]
}
```

`instances` is specifically the set of instance names for which the Java oracle
constructs real Helix `InstanceConfig` objects and for which Rust constructs the
corresponding routing-instance metadata. It is **not** a live-instance list.

A `current_state` entry is allowed to reference an instance that is absent from
`instances`. In that case:

- the instance still participates in `CurrentState -> ExternalView` aggregation;
- for the EXTERNALVIEW routing path, it must not be routable if Helix omits it
  because no corresponding `InstanceConfig` exists.

This behavior is exercised explicitly by the M7 fixture corpus.

No `IdealState` or `BestPossibleState` is supplied because M7 must not leak
desired state into `ExternalView`.

`LiveInstance` filtering is outside the M7 routing contract. If the concrete
Helix API requires a `LiveInstance` collection as construction plumbing, the
oracle may provide it, but must not substitute CURRENTSTATES/session routing
semantics for the EXTERNALVIEW path.

## Compared output

```json
{
  "operation": "compute_external_view_and_routing",
  "external_view": {
    "documents": {
      "documents_0": {
        "node-a": "LEADER",
        "node-b": "STANDBY"
      }
    }
  },
  "routing_results": [
    {
      "id": "p0-leader",
      "instances": ["node-a"]
    }
  ]
}
```

`external_view` is compared exactly after map-key canonicalization.

Each `routing_results[].instances` value is compared as a **set**. Instance
ordering is not part of the M7 compatibility claim.

There are no checked-in expected answers. Every verification run compares the
Rust implementation directly with the pinned Apache Helix 2.0.1 Java oracle.
