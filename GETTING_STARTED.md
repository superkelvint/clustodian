# Getting started with Clustodian

Clustodian is a control plane for partitioned, replicated applications.

It decides **where replicas should live, which replica should be active, and what state transitions should happen**. Your application still owns the actual data plane: opening replicas, copying data, catching up logs, serving traffic, and so on.

If you only want to see Clustodian work, start with the demo below. If you are integrating it into an application, skip to [Build your first cluster](#build-your-first-cluster).

## Fastest path: run the demo

The smallest complete example is `examples/01-replicated-kv`.

It starts:

- one Clustodian controller;
- three participant processes;
- one replicated partition;
- one `LEADER` and two `STANDBY` replicas; and
- a real etcd instance.

From the example directory:

```bash
cd examples/01-replicated-kv
./scripts/demo.sh
```

The script configures the cluster, starts the controller and participants, writes a value, kills the leader, waits for failover, and reads the value through the promoted replica.

This is the easiest way to see the complete runtime before writing any integration code.

---

## Build your first cluster

### 1. Add Clustodian

```toml
[dependencies]
clustodian = "0.1.0"
```

The distributed runtime also needs a running etcd. For local development, Clustodian defaults to:

```text
http://127.0.0.1:2379
```

You can configure a cluster with environment variables:

```bash
export CLUSTODIAN_CLUSTER=my-app
export CLUSTODIAN_ETCD_ENDPOINTS=http://127.0.0.1:2379
```

`CLUSTODIAN_NAMESPACE` is optional. By default it is derived from the cluster name.

### 2. Connect

```rust
use clustodian::{Cluster, ClusterConfig};

let cluster = Cluster::connect(ClusterConfig::from_env()?).await?;
```

A `Cluster` is the main application handle. From it you can configure resources, run controllers, run participants, and observe routing state.

### 3. Describe the cluster

Use the admin API to declare the instances and replicated resources Clustodian should manage:

```rust
use clustodian::{ClusterSpec, Placement, ResourceSpec};

cluster
    .admin()
    .apply(
        ClusterSpec::new()
            .instances(["node-a", "node-b", "node-c"])
            .resource(
                ResourceSpec::leader_standby("cache")
                    .partitions(12)
                    .replicas(2)
                    .placement(Placement::Crush),
            ),
    )
    .await?;
```

This says:

- the cluster has three instances;
- the `cache` resource has 12 partitions;
- each partition has two replicas; and
- each partition uses leader/standby states.

Clustodian now knows the **desired control-plane shape**. It still does not create or copy application data.

### 4. Run a controller

At least one controller process drives the cluster toward its desired state:

```rust
cluster
    .controller("controller-1")
    .run_until_signal()
    .await?;
```

You may run multiple controllers for high availability. Clustodian uses etcd-backed election and fencing so that only the active controller can publish authoritative state.

### 5. Run each participant

Each application node runs a participant under its stable instance name:

```rust
cluster
    .participant("node-a")
    .resource("cache", cache_handler)
    .run_until_signal()
    .await?;
```

Run the same application on `node-b`, `node-c`, and any other configured instances with their own instance IDs.

The participant receives state transitions for the replicas assigned to that node.

### 6. Implement the transition handler

Your handler turns Clustodian's control-plane transitions into real application work:

```rust
use clustodian::{
    ResourceHandler,
    ResourceState,
    ResourceTransition,
    TransitionContext,
    TransitionError,
};

struct CacheHandler;

impl ResourceHandler for CacheHandler {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        match transition.target() {
            ResourceState::Leader => {
                // Catch up the replica, then begin serving as leader.
            }
            ResourceState::Standby => {
                // Open or prepare the replica as a standby.
            }
            ResourceState::Offline | ResourceState::Dropped => {
                // Stop serving and release local resources.
            }
            ResourceState::Error | ResourceState::Other(_) => {}
        }

        Ok(())
    }
}
```

The important boundary is:

> **Clustodian decides which state a replica should enter. Your handler makes that state real.**

For example, moving `OFFLINE -> STANDBY` might mean restoring a snapshot and catching up a WAL. Moving `STANDBY -> LEADER` might mean confirming the replica is current before accepting writes.

Clustodian does not implement those data-plane operations for you.

---

## What runs where?

A typical deployment looks like this:

```text
                    etcd
                     |
        +------------+------------+
        |                         |
   controller                 controller
    (active)                   (standby)
        |
        +---------------------------+
        |            |              |
      node-a        node-b         node-c
   participant   participant    participant
        |            |              |
   your data      your data       your data
     plane          plane           plane
```

The controller computes and publishes work. Participants execute that work through your handlers. etcd supplies coordination, leases, watches, election, and fencing.

---

## Do you need the distributed runtime?

Not always.

Use the **distributed runtime** when you want Clustodian to manage live participants, failover, controller election, transition delivery, and restart/session fencing. This requires etcd.

Use the **deterministic core** when you only want placement, desired-state computation, transition planning, or routing from snapshots you provide yourself. The deterministic core does not require etcd or background processes.

---

## Where to go next

- **[README.md](README.md)** — project overview, capabilities, examples, and supported scope.
- **[RUNTIME.md](RUNTIME.md)** — production etcd configuration, TLS, sessions, fencing, controller failover, runtime architecture, and verification.
- **[`examples/01-replicated-kv`](examples/01-replicated-kv/)** — smallest complete runtime example.
- **[`examples/`](examples/)** — larger examples covering caches, queues, search, object storage, rolling restarts, multi-zone placement, and HA controllers.

For a first integration, start by copying the structure of `01-replicated-kv`: **setup -> controller -> participant -> transition handler**. Replace its in-memory data plane with your own application logic.
