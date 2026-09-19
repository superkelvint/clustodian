# Rebalance

Rebalancing answers one question: which instances should host each replica?
It produces ordered preference lists and desired replica states. It does not
send messages, touch etcd, or execute application work.

## The two supported strategies

### SEMI_AUTO

`semi_auto.rs` starts with the caller's ordered preference list. It filters out
instances that are not live, assigns states in the state model's priority
order, preserves useful current assignments when possible, and marks removed
replicas `DROPPED`.

### CRUSH

`crush.rs` builds a small topology tree and deterministically selects instances
using the supported Helix 2.0.1 CRUSH behavior. Hierarchical topologies prefer
distinct fault zones; flat topologies select instances directly.

The CRUSH implementation contains compatibility code for Java string hashes,
Java map bucket ordering, SHA-1-derived node IDs, and Jenkins hashing. Those
details are kept below the placement flow because they exist to reproduce the
oracle, not because they are the conceptual model of rebalancing.

## Reading order

Start with `semi_auto.rs` to understand the ordinary placement flow. Read the
public types and `compute_crush_assignment` in `crush.rs` next. Only then read
`TopologyTree` and the hashing helpers.

The `usize` node indexes inside CRUSH are physical implementation coordinates.
They must be resolved back to `InstanceId` before crossing the module boundary.
This is why callers receive `BTreeMap<PartitionId, Vec<InstanceId>>` rather
than CRUSH node numbers.
