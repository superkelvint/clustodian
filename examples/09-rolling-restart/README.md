# Rolling restart and session fencing

This showcase runs three controllers and three stateful participants over a
fixed SEMI_AUTO preference list. It demonstrates the difference between a
stable participant identity (`state-b`) and its monotonic incarnation
(`SessionId`): restarting `state-b` creates a new session, while the old
session's physical CurrentState records remain in etcd but are no longer
authoritative.

The process-level test holds a real transition callback at a file barrier,
pauses that participant with `SIGSTOP`, waits for its lease to expire, starts
the replacement, and resumes the old process with `SIGCONT`. The old callback
cannot publish CurrentState. A second transition addressed to the old session
is consumed as stale without invoking the replacement callback.

## Run

Requirements: Rust, Docker Compose, and etcd client access.

```bash
./scripts/demo.sh
```

Use an existing etcd endpoint with `ROLLING_ETCD_ENDPOINT`. The commands are:

```text
rolling-restart setup
rolling-restart controller <id>
rolling-restart participant <instance>
rolling-restart observe
rolling-restart session <instance>
rolling-restart inject-stale <instance> <old-session> [message-id]
```

The demo prints settled placement, old and new sessions, retained stale
metadata, stale callback evidence, and final convergence. Application state is
deliberately limited to transition log records; Clustodian owns coordination,
placement, and fencing, not application persistence.

## Integration test

The mandatory integration test launches the actual binary and a real etcd
process (or uses `CLUSTODIAN_ETCD_TEST_ENDPOINT`). It asserts three-controller
election, fixed placement, three participants, a higher SessionId after a
`SIGSTOP`/lease-expiry restart, retained old-session metadata, rejection of a
stale queued message, and rejection of a blocked old callback's state publish.

```bash
cargo test --manifest-path examples/09-rolling-restart/Cargo.toml --test integration
```
