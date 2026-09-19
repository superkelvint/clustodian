# Replicated key-value store

This is the smallest Clustodian showcase: one `kv_0` partition, three
participants, and the built-in `LeaderStandby` state model. The controller
assigns one `LEADER` and two `STANDBY` replicas. Writes are accepted only by
the leader and are synchronously forwarded to the other two demo processes.

Clustodian coordinates membership, roles, leases, transitions, and observed
state. The tiny TCP/`BTreeMap` data plane in this directory owns application
values; the library does not replicate application data for it.

## Run the demo

Requirements: Rust, Docker Compose, and an etcd image pull. From this
directory:

```bash
./scripts/demo.sh
```

The script configures the resource, starts a real controller and three real
participants, writes and reads `greeting`, kills `node-a` with `SIGKILL`, and
then reads the replicated value through the promoted `node-b`.

To use an existing etcd instead of Compose, set
`CLUSTODIAN_KV_ETCD_ENDPOINT` and `CLUSTODIAN_KV_SKIP_COMPOSE=1`.

## Commands

The one binary has these modes:

```text
replicated-kv setup
replicated-kv controller
replicated-kv node
replicated-kv status
replicated-kv client GET <key>
replicated-kv client PUT <key> <value>
```

The controller and nodes read `CLUSTODIAN_KV_ETCD_ENDPOINT`,
`CLUSTODIAN_KV_PREFIX`, and `CLUSTODIAN_KV_CLUSTER`. Nodes additionally read
`CLUSTODIAN_KV_INSTANCE_ID`, `CLUSTODIAN_KV_LISTEN`, and
`CLUSTODIAN_KV_PEERS` (`node-a=host:port,...`).

## Integration test

`tests/integration.rs` starts a package-local etcd process (or uses
`CLUSTODIAN_ETCD_TEST_ENDPOINT`), configures the real Clustodian admin API,
spawns the actual controller and participant binary, performs TCP writes and
reads, kills the observed leader, waits for lease expiry and reconciliation,
and verifies promotion plus continued data availability.

```bash
cargo test --manifest-path Cargo.toml --all-targets
```

The test requires `etcd` on `PATH` unless
`CLUSTODIAN_ETCD_TEST_ENDPOINT` points at a running etcd.

## Limitations

This intentionally demonstrates control-plane coordination, not durable
storage. Values are only in process memory, so a restarted participant starts
empty. The demo's synchronous forwarding gives the initial cluster a copy on
all three nodes; it is not a general-purpose replication protocol. Failover
is lease-bound (two seconds in the test), and routing is fixed to one
partition.
