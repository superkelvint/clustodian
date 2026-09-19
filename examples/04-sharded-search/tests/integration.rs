#![cfg(not(feature = "shuttle"))]

use clustodian::admin::{ClusterAdmin, InstanceSpec, PlacementSpec, ResourceSpec};
use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::{ClusterObserver, ClusterSnapshot};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tempfile::{tempdir, TempDir};

const RESOURCE: &str = "search";
const PARTITIONS: usize = 24;
const REPLICAS: usize = 2;
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct EtcdFixture {
    child: Option<Child>,
    data_dir: Option<TempDir>,
    endpoint: String,
    prefix: String,
}

impl EtcdFixture {
    async fn new() -> Self {
        let prefix = format!(
            "sharded-search-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        );
        if let Ok(endpoint) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Self {
                child: None,
                data_dir: None,
                endpoint,
                prefix,
            };
        }
        let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| String::from("etcd"));
        let client_port = free_port();
        let peer_port = free_port();
        let endpoint = format!("http://127.0.0.1:{client_port}");
        let peer_endpoint = format!("http://127.0.0.1:{peer_port}");
        let data_dir = tempdir().expect("create etcd data directory");
        let child = Command::new(binary)
            .args([
                "--name",
                "sharded-search-test",
                "--data-dir",
                data_dir.path().to_str().expect("etcd data path is utf8"),
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer_endpoint,
                "--initial-advertise-peer-urls",
                &peer_endpoint,
                "--initial-cluster",
                &format!("sharded-search-test={peer_endpoint}"),
                "--initial-cluster-state",
                "new",
                "--initial-cluster-token",
                "sharded-search-tests",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("etcd is required; install etcd or set CLUSTODIAN_ETCD_TEST_ENDPOINT");
        let mut fixture = Self {
            child: Some(child),
            data_dir: Some(data_dir),
            endpoint,
            prefix,
        };
        for _ in 0..160 {
            if let Some(child) = fixture.child.as_mut() {
                if let Some(error) =
                    clustodian_test_support::child_exit_message(child, "sharded-search etcd")
                {
                    panic!("{error}");
                }
            }
            if let Ok(backend) = fixture.backend_once().await {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return fixture;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("etcd did not become ready")
    }

    async fn backend(&self) -> EtcdCoordination {
        for _ in 0..160 {
            if let Ok(backend) = self.backend_once().await {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return backend;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("etcd did not become ready at {}", self.endpoint)
    }

    async fn backend_once(
        &self,
    ) -> Result<EtcdCoordination, clustodian::coordination::etcd::CoordinationError> {
        EtcdCoordination::connect(EtcdCoordinationConfig {
            endpoint: self.endpoint.clone(),
            prefix: self.prefix.clone(),
            cluster: String::from("sharded-search"),
        })
        .await
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

struct Process(Option<Child>);

impl Process {
    fn spawn(binary: &str, mode: &str, fixture: &EtcdFixture, instance: Option<&str>) -> Self {
        let mut command = Command::new(binary);
        command
            .arg(mode)
            .env("CLUSTODIAN_SEARCH_ETCD_ENDPOINT", &fixture.endpoint)
            .env("CLUSTODIAN_SEARCH_PREFIX", &fixture.prefix)
            .env("CLUSTODIAN_SEARCH_CLUSTER", "sharded-search")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        if let Some(instance) = instance {
            command
                .env("CLUSTODIAN_SEARCH_INSTANCE_ID", instance)
                .env("CLUSTODIAN_SEARCH_PARTICIPANT_LEASE_TTL_MS", "2500");
        } else {
            command
                .env("CLUSTODIAN_SEARCH_CONTROLLER_ID", "search-controller-1")
                .env("CLUSTODIAN_SEARCH_CONTROLLER_LEASE_TTL_MS", "1500");
        }
        Self(Some(command.spawn().expect("example process starts")))
    }

    fn stop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.0 = None;
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crush_rebalances_and_reconverges_through_real_processes() {
    let fixture = EtcdFixture::new().await;
    let backend = fixture.backend().await;
    let admin = ClusterAdmin::new(backend.clone());
    admin.ensure_cluster("sharded-search").await.unwrap();
    for instance in ["node-a", "node-b", "node-c"] {
        admin
            .put_instance(InstanceSpec {
                instance_id: instance.to_owned(),
                zone: String::from("default"),
            })
            .await
            .unwrap();
    }
    admin
        .put_resource(ResourceSpec {
            name: RESOURCE.to_owned(),
            partitions: PARTITIONS,
            replicas: REPLICAS,
            state_model: String::from("LeaderStandby"),
            placement: PlacementSpec::Crush,
        })
        .await
        .unwrap();

    let binary = std::env::var("CARGO_BIN_EXE_sharded-search")
        .expect("cargo exposes the sharded-search binary");
    let mut controller = Process::spawn(&binary, "controller", &fixture, None);
    let mut nodes = [
        Process::spawn(&binary, "participant", &fixture, Some("node-a")),
        Process::spawn(&binary, "participant", &fixture, Some("node-b")),
        Process::spawn(&binary, "participant", &fixture, Some("node-c")),
    ];
    let observer = ClusterObserver::new(backend.clone());
    let initial = wait_for(&observer, |snapshot| {
        snapshot.live_instances.len() == 3 && converged(snapshot, 3)
    })
    .await;
    assert_roles_are_settled(&initial);
    let initial_placement = placement(&initial.external_view);
    assert_eq!(initial_placement.len(), PARTITIONS);
    assert_routing_matches_external_view(&initial);

    let failed_leadership = leaders_for_instance(&initial.external_view, "node-a");
    assert!(
        !failed_leadership.is_empty(),
        "node-a should lead at least one partition before failover"
    );
    nodes[0].stop();
    let after_failover = wait_for(&observer, |snapshot| {
        snapshot.live_instances.len() == 2
            && converged(snapshot, 2)
            && failed_leadership.iter().all(|partition| {
                leader_for_partition(&snapshot.external_view, partition)
                    .as_deref()
                    .is_some_and(|leader| leader != "node-a")
            })
    })
    .await;
    assert_roles_are_settled(&after_failover);
    assert_routing_matches_external_view(&after_failover);

    nodes[0] = Process::spawn(&binary, "participant", &fixture, Some("node-a"));
    let after_restart = wait_for(&observer, |snapshot| {
        snapshot.live_instances.len() == 3 && converged(snapshot, 3)
    })
    .await;
    assert_roles_are_settled(&after_restart);
    assert_routing_matches_external_view(&after_restart);

    admin
        .put_instance(InstanceSpec {
            instance_id: String::from("node-d"),
            zone: String::from("default"),
        })
        .await
        .unwrap();
    let mut node_d = Process::spawn(&binary, "participant", &fixture, Some("node-d"));
    let after_add = wait_for(&observer, |snapshot| {
        snapshot.live_instances.len() == 4 && converged(snapshot, 4)
    })
    .await;
    assert_roles_are_settled(&after_add);
    let added_placement = placement(&after_add.external_view);
    assert_eq!(added_placement.len(), PARTITIONS);
    assert!(added_placement
        .values()
        .any(|hosts| hosts.contains("node-d")));
    assert!(initial_placement
        .iter()
        .any(|(partition, hosts)| added_placement.get(partition) != Some(hosts)));
    assert_routing_matches_external_view(&after_add);

    node_d.stop();
    wait_for(&observer, |snapshot| snapshot.live_instances.len() == 3).await;
    admin.remove_instance("node-d").await.unwrap();
    let after_remove = wait_for(&observer, |snapshot| {
        snapshot.live_instances.len() == 3
            && converged(snapshot, 3)
            && placement(&snapshot.external_view)
                .values()
                .all(|hosts| !hosts.contains("node-d"))
    })
    .await;
    assert_roles_are_settled(&after_remove);
    assert_eq!(placement(&after_remove.external_view).len(), PARTITIONS);
    assert_routing_matches_external_view(&after_remove);
    controller.stop();
    for node in &mut nodes {
        node.stop();
    }
}

fn converged(snapshot: &ClusterSnapshot, expected_live: usize) -> bool {
    if !snapshot.pending_transitions.is_empty()
        || snapshot.processed_revision.map(|r| r.value())
            < Some(snapshot.authoritative_revision.value())
    {
        return false;
    }
    snapshot.live_instances.len() == expected_live
        && placement(&snapshot.external_view).len() == PARTITIONS
        && placement(&snapshot.external_view)
            .values()
            .all(|hosts| hosts.len() == REPLICAS)
        && roles_are_settled(snapshot)
        && snapshot
            .routing_results
            .iter()
            .filter(|result| result.resource == RESOURCE && result.state == "LEADER")
            .count()
            == PARTITIONS
}

fn placement(view: &Value) -> BTreeMap<String, BTreeSet<String>> {
    view.get(RESOURCE)
        .and_then(Value::as_object)
        .map(|partitions| {
            partitions
                .iter()
                .filter_map(|(partition, instances)| {
                    let instances = instances.as_object()?;
                    Some((
                        partition.clone(),
                        instances.keys().cloned().collect::<BTreeSet<_>>(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn roles_are_settled(snapshot: &ClusterSnapshot) -> bool {
    snapshot
        .external_view
        .get(RESOURCE)
        .and_then(Value::as_object)
        .is_some_and(|partitions| partitions.values().all(role_counts_are_settled))
}

fn assert_roles_are_settled(snapshot: &ClusterSnapshot) {
    let partitions = snapshot
        .external_view
        .get(RESOURCE)
        .and_then(Value::as_object)
        .expect("search resource exists in ExternalView");
    assert_eq!(partitions.len(), PARTITIONS);
    for (partition, instances) in partitions {
        assert!(
            role_counts_are_settled(instances),
            "{partition} should have exactly one LEADER and one STANDBY: {instances}"
        );
    }
}

fn role_counts_are_settled(instances: &Value) -> bool {
    let Some(instances) = instances.as_object() else {
        return false;
    };
    instances.len() == REPLICAS
        && instances
            .values()
            .filter(|state| state.as_str() == Some("LEADER"))
            .count()
            == 1
        && instances
            .values()
            .filter(|state| state.as_str() == Some("STANDBY"))
            .count()
            == 1
}

fn leaders_for_instance(view: &Value, instance: &str) -> BTreeSet<String> {
    view.get(RESOURCE)
        .and_then(Value::as_object)
        .map(|partitions| {
            partitions
                .iter()
                .filter_map(|(partition, instances)| {
                    instances
                        .as_object()
                        .and_then(|states| states.get(instance))
                        .and_then(Value::as_str)
                        .filter(|state| *state == "LEADER")
                        .map(|_| partition.clone())
                })
                .collect()
        })
        .unwrap_or_default()
}

fn leader_for_partition(view: &Value, partition: &str) -> Option<String> {
    view.get(RESOURCE)
        .and_then(Value::as_object)
        .and_then(|partitions| partitions.get(partition))
        .and_then(Value::as_object)
        .and_then(|instances| {
            instances
                .iter()
                .find(|(_, state)| state.as_str() == Some("LEADER"))
                .map(|(instance, _)| instance.clone())
        })
}

fn assert_routing_matches_external_view(snapshot: &ClusterSnapshot) {
    let view = snapshot
        .external_view
        .get(RESOURCE)
        .unwrap()
        .as_object()
        .unwrap();
    for (partition, instances) in view {
        let states = instances.as_object().unwrap();
        for state in ["LEADER", "STANDBY"] {
            let expected = states
                .iter()
                .filter(|(_, value)| value.as_str() == Some(state))
                .map(|(instance, _)| instance.clone())
                .collect::<Vec<_>>();
            let result = snapshot
                .routing_results
                .iter()
                .find(|result| {
                    result.resource == RESOURCE
                        && result.partition == *partition
                        && result.state == state
                })
                .expect("ExternalView state has a routing result");
            assert_eq!(&result.instances, &expected);
        }
    }
}

async fn wait_for<F>(observer: &ClusterObserver, predicate: F) -> ClusterSnapshot
where
    F: Fn(&ClusterSnapshot) -> bool,
{
    let mut last = None;
    for _ in 0..300 {
        let snapshot = observer
            .snapshot()
            .await
            .expect("observer snapshot succeeds");
        if predicate(&snapshot) {
            return snapshot;
        }
        last = Some(snapshot);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for convergence: {last:#?}");
}

fn free_port() -> u16 {
    clustodian_test_support::allocate_port().expect("allocate coordinated test port")
}
