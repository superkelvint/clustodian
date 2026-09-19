# Routing

Routing turns a published `ExternalView` into lookup results for callers that
need to know where a state is advertised.

## The important boundary

`RoutingSnapshot` is built from two independent inputs:

```text
ExternalView           observed state published by the control plane
configured instances   InstanceConfig membership allowed to answer routing
```

`instances_for` returns configured instances advertising the requested state
for a resource partition. Missing resources, partitions, and states return an
empty set.

This module intentionally does not check live sessions. Routing follows the
EXTERNALVIEW-style configured-membership path; participant liveness and
session fencing belong to `model::session` and `coordination::etcd`.

## Reading the code

There is one implementation file, `snapshot.rs`. Read its constructor first,
then the two accessors, and finally `instances_for`. The small amount of code
is deliberate: routing should be a read-only view over already-published
state, not a second placement or health algorithm.
