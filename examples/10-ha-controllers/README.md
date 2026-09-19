# HA control-plane demo

This showcase runs three real Clustodian controllers and four real
participants against etcd. One controller wins the lease-backed election and
reconciles four single-owner partitions; the other two remain standby
candidates.

The integration test drives the exact failure sequence: settle the cluster,
send SIGSTOP to the active controller until its lease expires, wait for
takeover, send SIGCONT to the stale process, and verify that a write carrying
the old controller identity and lease is rejected by the transactional fence.

Run with Docker Compose:

    ./scripts/demo.sh

The test uses CLUSTODIAN_ETCD_TEST_ENDPOINT when set, otherwise it starts a
temporary local etcd binary. There is no nested Cargo workspace.
