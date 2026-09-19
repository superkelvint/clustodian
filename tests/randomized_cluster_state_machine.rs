#![cfg(not(feature = "shuttle"))]

#[path = "support/random_cluster/mod.rs"]
mod random_cluster;

mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn randomized_cluster_state_machine_smoke() {
    random_cluster::run_smoke()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn randomized_cluster_state_machine_stress() {
    random_cluster::run_stress()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
}
