# M6 verifier contract

Operation:

```text
compute_intermediate_and_throttle
```

Each scenario supplies semantic inputs sufficient to construct the real Helix
2.0.1 `IntermediateStateCalcStage` and `MessageThrottleStage` inputs:

- built-in state model;
- live instances;
- one or more resources;
- per-resource preference lists, current state, best-possible state, and
  already-selected transition messages;
- pending/in-flight transitions where relevant;
- state-transition throttle configurations.

The Java adapter must translate these fields into real Helix objects. It must
not implement throttling decisions itself.

The compared result contains exactly:

```json
{
  "operation": "compute_intermediate_and_throttle",
  "intermediate_state": {
    "resource": {
      "partition": {
        "instance": "STATE"
      }
    }
  },
  "dispatchable_transitions": [
    {
      "resource": "resource",
      "partition": "partition",
      "instance": "instance",
      "from": "OFFLINE",
      "to": "STANDBY",
      "message_type": "STATE_TRANSITION"
    }
  ]
}
```

`intermediate_state` is compared exactly after map-key canonicalization.
`dispatchable_transitions` is compared as a semantic set.

There are no checked-in expected answers: every verification run compares the
Rust implementation directly with the pinned Apache Helix 2.0.1 Java oracle.
