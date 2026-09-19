# Multi-zone replicated database

This standalone example uses Clustodian to coordinate six `LeaderStandby`
partitions at RF=3. `CrushWithTopology` places each replica in a different
`zone`, while the tiny TCP data plane synchronously copies writes to replicas.

Start etcd, then run:

```bash
ZONEDB_NODES='db-a=zone-a@127.0.0.1:28101,db-b=zone-b@127.0.0.1:28102,db-c=zone-c@127.0.0.1:28103' \
  ZONEDB_ETCD_ENDPOINT=http://127.0.0.1:23798 ZONEDB_PREFIX=zonedb-demo \
  ZONEDB_CLUSTER=zonedb-demo cargo run -- admin init "$ZONEDB_NODES"
```

`cargo run -- controller` and one `cargo run -- node db-{a,b,c}` process complete
the cluster. `status`, `put KEY VALUE`, and `get KEY` inspect/use the database.
The supplied `scripts/demo.sh` automates convergence, a whole-zone process
failure, failover, replacement, and cross-zone healing.

The integration test is intentionally live: it starts etcd and the actual
controller/node binaries, asserts every settled partition has three distinct
zones, kills one zone, verifies reads still work, then adds a replacement in
the failed zone and asserts RF and zone diversity recover.
