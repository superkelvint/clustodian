# M12 verifier contract — complete immutable harness

M12 is the first full-system HA usability gate.

**No M12 verification code is implementation work.** Every verifier script, scenario, process wrapper, mock application callback, invariant checker, and failure injector lives under `reference/apache-helix/` and is immutable. Production implementation work belongs only in `src/` and normal production crates.

M12 first requires `verify-m11.sh` to pass. M0-M11 already establish Apache Helix 2.0.1 conformance for the semantic pipeline and runtimes. M12 adds etcd-specific controller authority/election/failover invariants and full-stack integration; it does not require a new Java oracle adapter.

The gate runs 8 focused election/fencing scenarios and 3 application-shaped E2E scenarios against a private real etcd 3.7.1. Controllers and participants run as separate OS processes. The immutable participant wrapper calls the canonical `clustodian::participant::ParticipantRuntime` from M11 directly and provides only the application callback; production M11 owns registration/session lifecycle, message delivery, execution concurrency, transition identity, session-fenced completion, progress tracking, and CurrentState publication. No second M12 participant runtime is permitted.

The four verifier-owned Rust binaries under `m12-harness/` call normal production APIs. Their imports and signatures are an executable API contract. If they do not compile before M12, that is the correct failure. Do not edit the harness.

Focused stale-controller fencing uses `m12-fence-probe`: it holds a production controller authority guard across real lease expiry and then attempts a controller-owned transactional write with that stale guard. The write must be rejected at the etcd authority boundary.

The three E2E scenarios are: replicated database HA (CRUSH, 6 partitions, RF=3), elastic sharded search/cache (CRUSH, 24 partitions, RF=2, failover mid-rebalance), and rolling participant restart + controller lease loss (SEMI_AUTO, 8 partitions, RF=3).

Settled checkpoints independently require one active controller, no pending transitions, current-session-owned CurrentState, no stale message targets, at most one LEADER, exactly one LEADER for active partitions, and restored RF when enough nodes are live.


## Canonical participant runtime requirement

M12 must exercise the same participant execution engine verified by M11:

```text
clustodian::participant::ParticipantRuntime
```

The M12 harness must not use `runtime::ParticipantRuntime`, a duplicate participant loop, or a verifier-driven production transition gate. Its mock `TransitionHandler` is the application/data-plane callback only. A deliberately blocked callback is implemented entirely in the immutable verifier harness; all message receipt, per-partition execution, reconnect/session behavior, CurrentState completion, and progress semantics remain owned by the canonical M11 runtime.
