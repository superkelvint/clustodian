#![cfg(not(feature = "shuttle"))]

use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::{ClusterObserver, ClusterSnapshot};
use clustodian_distributed_cache::{partition_for_key, request_at, PARTITION_COUNT, RESOURCE};
use std::collections::{BTreeMap, BTreeSet};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};

static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

struct EtcdFixture {
    child: Option<Child>,
    data_dir: Option<TempDir>,
    endpoint: String,
}

impl EtcdFixture {
    fn start() -> Self {
        if let Ok(endpoint) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Self {
                child: None,
                data_dir: None,
                endpoint,
            };
        }
        let client_port = free_port();
        let peer_port = free_port();
        let endpoint = format!("http://127.0.0.1:{client_port}");
        let peer_endpoint = format!("http://127.0.0.1:{peer_port}");
        let data_dir = tempdir().expect("temporary etcd directory");
        let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| "etcd".to_owned());
        let child = Command::new(binary)
            .args([
                "--name",
                "cache-integration",
                "--data-dir",
                data_dir.path().to_str().expect("temporary path is utf8"),
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer_endpoint,
                "--initial-advertise-peer-urls",
                &peer_endpoint,
                "--initial-cluster",
                &format!("cache-integration={peer_endpoint}"),
                "--initial-cluster-state",
                "new",
                "--initial-cluster-token",
                "cache-integration",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("etcd must be installed or CLUSTODIAN_ETCD_TEST_ENDPOINT set");
        Self {
            child: Some(child),
            data_dir: Some(data_dir),
            endpoint,
        }
    }

    async fn connect(&mut self, prefix: &str) -> EtcdCoordination {
        for _ in 0..200 {
            if let Some(child) = self.child.as_mut() {
                if let Some(error) =
                    clustodian_test_support::child_exit_message(child, "distributed-cache etcd")
                {
                    panic!("{error}");
                }
            }
            if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: self.endpoint.clone(),
                prefix: prefix.to_owned(),
                cluster: "distributed-cache-integration".to_owned(),
            })
            .await
            {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return backend;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("etcd did not become ready");
    }
}

impl Drop for EtcdFixture {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.data_dir.take();
    }
}

struct Processes {
    controller: Child,
    nodes: BTreeMap<String, Child>,
}

impl Processes {
    fn kill_node(&mut self, instance: &str) {
        let child = self.nodes.get_mut(instance).expect("node is running");
        let _ = child.kill();
        let _ = child.wait();
    }
}

impl Drop for Processes {
    fn drop(&mut self) {
        for child in self.nodes.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.controller.kill();
        let _ = self.controller.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_cache_shards_replicate_fail_over_and_rebalance() {
    let mut fixture = EtcdFixture::start();
    let run = NEXT_RUN.fetch_add(1, Ordering::Relaxed);
    let prefix = format!("clustodian-cache-integration-{}-{run}", std::process::id());
    let backend = fixture.connect(&prefix).await;
    let ports = [free_port(), free_port(), free_port(), free_port()];
    let nodes = format!(
        "node-a=127.0.0.1:{},node-b=127.0.0.1:{},node-c=127.0.0.1:{},node-d=127.0.0.1:{}",
        ports[0], ports[1], ports[2], ports[3]
    );
    let base_env = [
        ("CACHE_ETCD_ENDPOINT", fixture.endpoint.as_str()),
        ("CACHE_PREFIX", prefix.as_str()),
        ("CACHE_CLUSTER", "distributed-cache-integration"),
        ("CACHE_NODES", nodes.as_str()),
    ];
    let initial_nodes = format!(
        "node-a=127.0.0.1:{},node-b=127.0.0.1:{},node-c=127.0.0.1:{}",
        ports[0], ports[1], ports[2]
    );
    run_ctl(&base_env, &["init", &initial_nodes]);

    let controller = spawn_controller(&base_env, "cache-controller");
    let mut processes = Processes {
        controller,
        nodes: BTreeMap::new(),
    };
    for (index, instance) in ["node-a", "node-b", "node-c"].into_iter().enumerate() {
        processes.nodes.insert(
            instance.to_owned(),
            spawn_node(&base_env, instance, ports[index]),
        );
    }

    let initial = wait_for_settled(
        &backend,
        &BTreeSet::from_iter([
            "node-a".to_owned(),
            "node-b".to_owned(),
            "node-c".to_owned(),
        ]),
    )
    .await;
    assert_settled(
        &initial,
        3,
        &BTreeSet::from_iter([
            "node-a".to_owned(),
            "node-b".to_owned(),
            "node-c".to_owned(),
        ]),
    );
    let key = "greeting";
    let partition = partition_for_key(key);
    let initial_leader = leader_for(&initial, &partition);
    let initial_standby = standby_for(&initial, &partition);
    assert_eq!(
        request_at(&address_for(&nodes, &initial_standby), "PUT greeting bad"),
        Ok("ERR not-leader".to_owned())
    );
    assert_eq!(
        request_at(&address_for(&nodes, &initial_leader), "PUT greeting hello"),
        Ok("OK".to_owned())
    );
    assert_eq!(
        request_at(&address_for(&nodes, &initial_standby), "GET greeting"),
        Ok("VALUE hello".to_owned())
    );
    for index in 0..PARTITION_COUNT {
        let partition = format!("{RESOURCE}_{index}");
        let key = key_for_partition(&partition);
        let leader = leader_for(&initial, &partition);
        assert_eq!(
            request_at(
                &address_for(&nodes, &leader),
                &format!("PUT {key} value-{index}"),
            ),
            Ok("OK".to_owned())
        );
    }

    processes.kill_node(&initial_leader);
    let after_failure = wait_for_failover(&backend, &initial_leader).await;
    assert_eq!(after_failure.live_instances.len(), 2);
    let promoted = leader_for(&after_failure, &partition);
    assert_ne!(promoted, initial_leader);
    assert_eq!(
        request_at(&address_for(&nodes, &promoted), "GET greeting"),
        Ok("VALUE hello".to_owned())
    );

    let mut expected_after_add = after_failure
        .live_instances
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    expected_after_add.insert("node-d".to_owned());
    run_ctl(
        &base_env,
        &["add-node", &format!("node-d=127.0.0.1:{}", ports[3])],
    );
    processes.nodes.insert(
        "node-d".to_owned(),
        spawn_node(&base_env, "node-d", ports[3]),
    );
    let before_add = after_failure.clone();
    let after_add =
        wait_for_instance_assignment(&backend, &expected_after_add, "node-d", &mut processes).await;
    assert_settled(&after_add, 3, &expected_after_add);
    assert!(
        host_sets_changed(&before_add, &after_add),
        "adding node-d did not change placement: before={} after={}",
        before_add.external_view,
        after_add.external_view
    );
    assert!(after_add
        .external_view
        .get(RESOURCE)
        .and_then(|value| value.as_object())
        .is_some_and(|resources| resources.values().any(|partition| {
            partition
                .as_object()
                .is_some_and(|replicas| replicas.contains_key("node-d"))
        })));
    for (partition, _replicas) in after_add.external_view[RESOURCE]
        .as_object()
        .unwrap()
        .iter()
        .filter_map(|(partition, replicas)| {
            replicas.as_object().map(|replicas| (partition, replicas))
        })
        .filter(|(_, replicas)| replicas.contains_key("node-d"))
    {
        let index = partition
            .strip_prefix("cache_")
            .expect("cache partition has numeric suffix");
        let key = key_for_partition(partition);
        assert_eq!(
            request_at(&address_for(&nodes, "node-d"), &format!("GET {key}")),
            Ok(format!("VALUE value-{index}"))
        );
    }

    processes.kill_node("node-d");
    wait_until(|| async {
        backend
            .live_session(&clustodian::model::InstanceId::new("node-d").unwrap())
            .await
            .unwrap()
            .is_none()
    })
    .await;
    let expected_final = after_failure
        .live_instances
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    run_ctl(&base_env, &["remove-node", "node-d"]);
    let final_snapshot = wait_for_settled(&backend, &expected_final).await;
    assert_settled(&final_snapshot, 2, &expected_final);
    assert!(!final_snapshot.external_view.to_string().contains("node-d"));
}

async fn wait_for_settled(
    backend: &EtcdCoordination,
    expected_live: &BTreeSet<String>,
) -> ClusterSnapshot {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let snapshot = ClusterObserver::new(backend.clone())
            .snapshot()
            .await
            .unwrap();
        if snapshot
            .live_instances
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            == *expected_live
            && settled_shape(&snapshot)
        {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for settled cache cluster: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_failover(backend: &EtcdCoordination, failed: &str) -> ClusterSnapshot {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let snapshot = ClusterObserver::new(backend.clone())
            .snapshot()
            .await
            .unwrap();
        if !snapshot.live_instances.contains_key(failed)
            && snapshot.live_instances.len() == 2
            && !external_view_contains(&snapshot, failed)
            && settled_shape(&snapshot)
        {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for failover: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_instance_assignment(
    backend: &EtcdCoordination,
    expected_live: &BTreeSet<String>,
    instance: &str,
    processes: &mut Processes,
) -> ClusterSnapshot {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(child) = processes.nodes.get_mut(instance) {
            if let Some(status) = child.try_wait().expect("cache node process status") {
                panic!("cache node {instance} exited before registering: {status}");
            }
        }
        let snapshot = ClusterObserver::new(backend.clone())
            .snapshot()
            .await
            .unwrap();
        if snapshot
            .live_instances
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            == *expected_live
            && settled_shape(&snapshot)
            && external_view_contains(&snapshot, instance)
        {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {instance} to receive a cache partition: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn external_view_contains(snapshot: &ClusterSnapshot, instance: &str) -> bool {
    snapshot
        .external_view
        .get(RESOURCE)
        .and_then(|resource| resource.as_object())
        .is_some_and(|partitions| {
            partitions.values().any(|partition| {
                partition
                    .as_object()
                    .is_some_and(|replicas| replicas.contains_key(instance))
            })
        })
}

fn key_for_partition(partition: &str) -> String {
    (0..10_000)
        .map(|index| format!("cache-key-{index}"))
        .find(|key| partition_for_key(key) == partition)
        .unwrap_or_else(|| panic!("no cache key found for {partition}"))
}

fn settled_shape(snapshot: &ClusterSnapshot) -> bool {
    snapshot
        .processed_revision
        .is_some_and(|processed| processed >= snapshot.authoritative_revision)
        && snapshot.pending_transitions.is_empty()
        && snapshot
            .external_view
            .get(RESOURCE)
            .and_then(|value| value.as_object())
            .is_some_and(|partitions| {
                partitions.len() == PARTITION_COUNT
                    && partitions.values().all(|partition| {
                        let Some(replicas) = partition.as_object() else {
                            return false;
                        };
                        replicas.len() == 2
                            && replicas
                                .values()
                                .filter(|state| state.as_str() == Some("LEADER"))
                                .count()
                                == 1
                            && replicas
                                .values()
                                .filter(|state| state.as_str() == Some("STANDBY"))
                                .count()
                                == 1
                    })
            })
}

fn assert_settled(snapshot: &ClusterSnapshot, live_count: usize, expected: &BTreeSet<String>) {
    assert_eq!(snapshot.live_instances.len(), live_count);
    assert_eq!(
        snapshot
            .live_instances
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        *expected
    );
    assert!(
        settled_shape(snapshot),
        "cluster did not settle: {snapshot:?}"
    );
}

fn leader_for(snapshot: &ClusterSnapshot, partition: &str) -> String {
    snapshot.external_view[RESOURCE][partition]
        .as_object()
        .and_then(|replicas| {
            replicas
                .iter()
                .find(|(_, state)| state.as_str() == Some("LEADER"))
                .map(|(instance, _)| instance.clone())
        })
        .expect("partition has one leader")
}

fn standby_for(snapshot: &ClusterSnapshot, partition: &str) -> String {
    snapshot.external_view[RESOURCE][partition]
        .as_object()
        .and_then(|replicas| {
            replicas
                .iter()
                .find(|(_, state)| state.as_str() == Some("STANDBY"))
                .map(|(instance, _)| instance.clone())
        })
        .expect("partition has one standby")
}

fn host_sets_changed(before: &ClusterSnapshot, after: &ClusterSnapshot) -> bool {
    (0..PARTITION_COUNT).any(|index| {
        let partition = format!("{RESOURCE}_{index}");
        let hosts = |snapshot: &ClusterSnapshot| {
            snapshot.external_view[RESOURCE][&partition]
                .as_object()
                .map(|replicas| replicas.keys().cloned().collect::<BTreeSet<_>>())
                .unwrap_or_default()
        };
        hosts(before) != hosts(after)
    })
}

fn address_for(nodes: &str, instance: &str) -> String {
    nodes
        .split(',')
        .find_map(|entry| entry.strip_prefix(&format!("{instance}=")))
        .expect("instance has an address")
        .to_owned()
}

fn run_ctl(env: &[(&str, &str)], args: &[&str]) {
    let binary = std::env::var("CARGO_BIN_EXE_cachectl").expect("cachectl binary path");
    let output = Command::new(binary)
        .args(args)
        .envs(env.iter().copied())
        .output()
        .expect("cachectl starts");
    assert!(
        output.status.success(),
        "cachectl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn spawn_controller(env: &[(&str, &str)], id: &str) -> Child {
    let binary = std::env::var("CARGO_BIN_EXE_cache-controller").expect("controller binary path");
    Command::new(binary)
        .envs(env.iter().copied())
        .env("CACHE_CONTROLLER_ID", id)
        .env("CACHE_CONTROLLER_LEASE_TTL_MS", "1500")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("controller starts")
}

fn spawn_node(env: &[(&str, &str)], instance: &str, port: u16) -> Child {
    let binary = std::env::var("CARGO_BIN_EXE_cache-node").expect("cache-node binary path");
    Command::new(binary)
        .envs(env.iter().copied())
        .env("CACHE_INSTANCE_ID", instance)
        .env("CACHE_LISTEN", format!("127.0.0.1:{port}"))
        .env("CACHE_PARTICIPANT_LEASE_TTL_MS", "1500")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("cache node starts")
}

async fn wait_until<F, Fut>(mut predicate: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(20);
    while !predicate().await {
        assert!(Instant::now() < deadline, "condition did not become true");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn free_port() -> u16 {
    clustodian_test_support::allocate_port().expect("allocate coordinated test port")
}
