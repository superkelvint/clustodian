# M12 production API contract

The immutable M12 harness compiles directly against normal production `clustodian` APIs. M12 implementation work must change production code only; the harness is not an implementation surface.

## Coordination and controller surfaces

The harness uses production APIs in:

```text
clustodian::coordination::etcd
clustodian::election
clustodian::runtime::ControllerRuntime
clustodian::admin
clustodian::observe
```

Controller-owned writes must accept an opaque production authority guard and validate that authority transactionally in etcd. `ClusterObserver::snapshot()` must expose semantic controller membership, LiveInstances, active CurrentState, ExternalView, pending transitions, and routing results.

## Participant surface — canonical M11 API

M12 deliberately does **not** define a second participant runtime API.

The immutable participant process imports the already-established M11 production surface:

```rust
use clustodian::model::{leader_standby, InstanceId};
use clustodian::participant::{
    ParticipantRuntime,
    TransitionExecution,
    TransitionHandler,
    TransitionHandlerError,
};
```

and constructs the runtime as:

```rust
let runtime = ParticipantRuntime::new(
    coordination,
    instance,
    leader_standby(),
    handler,
)
.with_lease_ttl(participant_lease_ttl)?;

runtime.run(|| Ok(())).await?;
```

The application callback implements:

```rust
impl TransitionHandler for Handler {
    fn handle(
        &self,
        execution: &TransitionExecution,
    ) -> Result<(), TransitionHandlerError>;
}
```

The verifier consumes the canonical M11 execution identity and transition fields through:

```text
TransitionExecution::resource()
TransitionExecution::partition()
TransitionExecution::source_state()
TransitionExecution::target_state()
TransitionExecution::transition_id()
```

The participant instance identity belongs to the running participant process and is supplied to the immutable mock handler by the harness; it is not added to `TransitionExecution` merely for M12.

### Architectural requirement

There must be exactly one production participant execution engine: `clustodian::participant::ParticipantRuntime`.

M12 must not require or preserve a second participant loop in `runtime.rs`. In particular, M12 verification must exercise the same M11 production machinery responsible for:

```text
registration and session lifecycle
pending-transition watch/recovery
per-partition in-flight execution
stable transition identity
application callback execution
session-fenced CurrentState completion
participant progress tracking
```

The M12 mock callback may block one callback using verifier-owned filesystem barriers. That blocking behavior exists only in `reference/apache-helix/m12-harness`; production `clustodian` must not implement a cluster-wide application transition gate for the verifier.
