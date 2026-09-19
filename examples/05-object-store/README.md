# Topology-aware object-store coordinator

This example coordinates twelve object ranges with Clustodian.  The resource
uses `LeaderStandby`, RF=3, and topology-aware CRUSH at `/zone/instance`, so a
settled range has one replica in each of `zone-a`, `zone-b`, and `zone-c`.

Object values are deliberately held in memory by the demo process. A leader
replicates writes synchronously to its surviving standbys, and a rejoining
standby requests a range snapshot from its peers. The control plane remains in
etcd; this is not a durable object store.

## Run

Start etcd, then run:

```bash
export OBJECT_STORE_ETCD_ENDPOINT=http://127.0.0.1:2379
export OBJECT_STORE_PREFIX=object-store-demo
export OBJECT_STORE_CLUSTER=objects
export OBJECT_STORE_NODES='object-a@zone-a=127.0.0.1:18101,object-b@zone-b=127.0.0.1:18102,object-c@zone-c=127.0.0.1:18103'
cargo run --manifest-path examples/05-object-store/Cargo.toml -- setup
./examples/05-object-store/scripts/demo.sh
```

The script shows initial zone-diverse placement, writes an object, kills one
whole zone, verifies a surviving replica takes leadership, then restarts the
zone and verifies CRUSH restores all three zones and the object value.

## Integration test

`tests/integration.rs` starts a real etcd process unless
`CLUSTODIAN_ETCD_TEST_ENDPOINT` is supplied. It starts the actual controller
and node binaries, asserts zone diversity for every range, writes and reads an
object from all replicas, kills a zone, verifies degraded recovery and data
availability, then rejoins the zone and verifies healed placement and snapshot
recovery.

```bash
cargo test --manifest-path examples/05-object-store/Cargo.toml --test integration
```
