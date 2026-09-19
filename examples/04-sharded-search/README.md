# Sharded search cluster

This showcase runs a small search control plane with Clustodian:

- 24 search partitions;
- replication factor 2;
- built-in `LeaderStandby` state model;
- deterministic CRUSH placement across three participants;
- a live fourth node that receives rebalanced partitions;
- node removal followed by full reconvergence.

The participant data plane is deliberately small: it records the partitions
and roles assigned to its process. Clustodian supplies membership, placement,
transitions, ExternalView, and routing; it does not store or replicate search
documents.

## Run

Start etcd, then run the demo:

```bash
docker compose up -d etcd
./scripts/demo.sh
```

The script configures the cluster, starts one controller and three
participants, prints settled ownership, adds `node-d`, prints the moved
partitions, removes it, and prints the final RF=2 state.

To use an existing etcd, export `CLUSTODIAN_ETCD_TEST_ENDPOINT` before running
the integration test. Otherwise the test starts a temporary local `etcd` binary.

## Integration test

`tests/integration.rs` is intentionally non-ignored. It starts the actual
`sharded-search controller` and `sharded-search participant` binaries, waits
for live etcd sessions and drained transition queues, and asserts:

- all 24 partitions have two distinct replicas;
- each partition has one `LEADER` and one `STANDBY`;
- adding a node changes at least one replica set and assigns work to it;
- removing the node removes it from ExternalView and restores RF=2;
- Observer routing results exactly match the settled ExternalView.

Run it with:

```bash
cargo test --manifest-path examples/04-sharded-search/Cargo.toml --test integration
```
