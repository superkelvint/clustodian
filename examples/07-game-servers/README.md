# Game-server allocator

This sample runs a small stateful game service where each game-worlds_N
partition represents a game world. Clustodian assigns two live servers to each
world using CRUSH: one server becomes LEADER (the active owner) and one is
STANDBY (the takeover candidate).

The game data plane is intentionally tiny. A participant keeps a process-local
registry of worlds and prints every ownership transition. Clustodian provides
membership, placement, failover, and session fencing; it does not replicate
game state or persist the world registry.

## Run the demo

Prerequisites are Rust, Docker Compose, and a local etcd client endpoint.

    ./scripts/demo.sh

The script starts etcd, configures three servers and six worlds, starts one
controller and three game servers, prints settled ownership, kills a current
owner, and waits for its standby to take over. It then adds game-d, starts it,
shows rebalancing, stops it, removes it from the configuration, and waits for
final convergence.

To use an existing etcd instance instead:

    CLUSTODIAN_ETCD_ENDPOINT=http://127.0.0.1:2379 ./scripts/demo.sh

The binary commands are:

    clustodian-game-servers admin init
    clustodian-game-servers controller
    clustodian-game-servers server <instance-id>
    clustodian-game-servers observe
    clustodian-game-servers leader <partition>
    clustodian-game-servers admin add <instance-id> [zone]
    clustodian-game-servers admin remove <instance-id>

## Integration test

The mandatory integration test starts a real etcd process (or uses
CLUSTODIAN_ETCD_TEST_ENDPOINT), runs the actual controller and participant
runtimes, asserts one active owner and one standby for every world, aborts an
owner without revoking its lease, verifies standby promotion after lease
expiry, then adds and removes a participant and verifies placement and
ownership reconverge with no pending transitions.

Run it with:

    cargo test --manifest-path examples/07-game-servers/Cargo.toml --test integration

## Design notes

- SIGKILL or task abort demonstrates lease-backed failure detection. A
  graceful SIGTERM revokes the server's lease immediately.
- A server must be stopped before admin remove; this prevents a configured live
  participant from being accidentally reintroduced during reconciliation.
- The CRUSH placement in this sample uses the library's current flat instance
  topology. The zone strings are retained as instance metadata for future
  topology-aware variants.
