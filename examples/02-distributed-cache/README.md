# Distributed cache

This is a runnable Clustodian showcase with twelve cache partitions, two
replicas per partition, and one `LEADER` plus `STANDBY` replicas. `cachectl`
routes writes to the observed leader. A successful write is synchronously
copied to the live standby, while the cache contents remain process-local:
Clustodian coordinates ownership and failover, but is not a data replication
engine.

## Run it

Start etcd with `docker compose up -d etcd`, then run:

```bash
cargo run --manifest-path examples/02-distributed-cache/Cargo.toml --bin cachectl -- \
  init node-a=127.0.0.1:18100,node-b=127.0.0.1:18101,node-c=127.0.0.1:18102
examples/02-distributed-cache/scripts/demo.sh
```

The script starts one controller and three participants, writes data, kills the
current leader, waits for lease-backed failover, adds a fourth node, and removes
it again. It uses `SIGKILL` for the failure demonstration so the controller must
observe lease expiry.

The package is a workspace member, and its live integration test is the
acceptance check:

```bash
cargo test --manifest-path examples/02-distributed-cache/Cargo.toml
```

The test uses `CLUSTODIAN_ETCD_TEST_ENDPOINT` when set; otherwise it starts a
temporary local `etcd` binary. It starts the actual controller and cache-node
executables, then asserts sharding, leader-only writes, synchronous standby
replication, leader death/promotion, and add/remove-node convergence.
