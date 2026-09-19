#![cfg(all(unix, not(feature = "shuttle")))]

use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::ClusterObserver;
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};

const RESOURCE: &str = "game-worlds";
static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

struct EtcdFixture {
    child: Option<Child>,
    _data_dir: Option<TempDir>,
    endpoint: String,
}

impl EtcdFixture {
    async fn start() -> Self {
        if let Ok(endpoint) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Self {
                child: None,
                _data_dir: None,
                endpoint,
            };
        }
        let client_port = free_port();
        let peer_port = free_port();
        let endpoint = format!("http://127.0.0.1:{client_port}");
        let peer = format!("http://127.0.0.1:{peer_port}");
        let data_dir = tempdir().expect("temporary etcd data directory");
        let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| "etcd".into());
        let child = Command::new(binary)
            .args([
                "--name",
                "game-test",
                "--data-dir",
                data_dir.path().to_str().unwrap(),
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer,
                "--initial-advertise-peer-urls",
                &peer,
                "--initial-cluster",
                &format!("game-test={peer}"),
                "--initial-cluster-state",
                "new",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("etcd must be installed or CLUSTODIAN_ETCD_TEST_ENDPOINT set");
        let mut fixture = Self {
            child: Some(child),
            _data_dir: Some(data_dir),
            endpoint,
        };
        for _ in 0..160 {
            if let Some(child) = fixture.child.as_mut() {
                if let Some(error) =
                    clustodian_test_support::child_exit_message(child, "game-servers etcd")
                {
                    panic!("{error}");
                }
            }
            if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: fixture.endpoint.clone(),
                prefix: "game-probe".into(),
                cluster: "probe".into(),
            })
            .await
            {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return fixture;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("etcd did not become ready");
    }

    async fn connect(&self, prefix: &str, cluster: &str) -> EtcdCoordination {
        for _ in 0..160 {
            if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: self.endpoint.clone(),
                prefix: prefix.to_owned(),
                cluster: cluster.to_owned(),
            })
            .await
            {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return backend;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("coordination backend did not become ready");
    }
}

impl Drop for EtcdFixture {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct Processes {
    controller: Child,
    servers: BTreeMap<String, Child>,
}

impl Drop for Processes {
    fn drop(&mut self) {
        let _ = self.controller.kill();
        let _ = self.controller.wait();
        for child in self.servers.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn game_world_ownership_fails_over_and_reconverges_after_membership_changes() {
    let etcd = EtcdFixture::start().await;
    let prefix = format!(
        "game-integration-{}-{}",
        std::process::id(),
        NEXT_RUN.fetch_add(1, Ordering::Relaxed)
    );
    let cluster = "game-integration";
    let backend = etcd.connect(&prefix, cluster).await;
    let binary = std::env::var("CARGO_BIN_EXE_clustodian-game-servers")
        .expect("Cargo exposes the game-server binary to integration tests");
    let common = [
        ("CLUSTODIAN_ETCD_ENDPOINT", etcd.endpoint.as_str()),
        ("CLUSTODIAN_GAME_PREFIX", prefix.as_str()),
        ("CLUSTODIAN_GAME_CLUSTER", cluster),
    ];
    let mut setup = Command::new(&binary);
    setup.args(["admin", "init"]).envs(common.iter().copied());
    assert!(setup.status().expect("run game setup").success());
    let mut controller = Command::new(&binary);
    controller
        .args(["controller"])
        .envs(common.iter().copied())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let controller = controller.spawn().expect("spawn game controller");
    let mut processes = Processes {
        controller,
        servers: BTreeMap::new(),
    };
    for instance in ["game-a", "game-b", "game-c"] {
        let mut server = Command::new(&binary);
        server
            .args(["server", instance])
            .envs(common.iter().copied())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        processes
            .servers
            .insert(instance.into(), server.spawn().expect("spawn game server"));
    }
    let observer = ClusterObserver::new(backend.clone());
    let initial = wait_for(&observer, |snapshot| settled(snapshot, 3)).await;
    let partition = "game-worlds_0";
    let failed = leader(&initial.external_view, partition);
    let mut dead = processes
        .servers
        .remove(&failed)
        .expect("leader process exists");
    let _ = dead.kill();
    let _ = dead.wait();
    let after_failure = wait_for(&observer, |snapshot| {
        snapshot.live_instances.len() == 2
            && !snapshot.live_instances.contains_key(&failed)
            && settled(snapshot, 2)
            && leader(&snapshot.external_view, partition) != failed
    })
    .await;
    assert_ne!(leader(&after_failure.external_view, partition), failed);

    let replacement = "game-d";
    let mut add = Command::new(&binary);
    add.args(["admin", "add", replacement, "zone-d"])
        .envs(common.iter().copied());
    assert!(add.status().expect("add game server").success());
    let mut server = Command::new(&binary);
    server
        .args(["server", replacement])
        .envs(common.iter().copied())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    processes.servers.insert(
        replacement.into(),
        server.spawn().expect("spawn replacement server"),
    );
    let after_add = wait_for(&observer, |snapshot| {
        settled(snapshot, 3) && assigned_to(snapshot, replacement)
    })
    .await;
    assert!(
        assigned_to(&after_add, replacement),
        "new game server should receive at least one world assignment"
    );

    let mut replacement = processes
        .servers
        .remove("game-d")
        .expect("replacement process");
    let _ = replacement.kill();
    let _ = replacement.wait();
    wait_for(&observer, |snapshot| {
        !snapshot.live_instances.contains_key("game-d")
    })
    .await;
    let mut remove = Command::new(&binary);
    remove
        .args(["admin", "remove", "game-d"])
        .envs(common.iter().copied());
    assert!(remove.status().expect("remove game server").success());
    let final_snapshot = wait_for(&observer, |snapshot| settled(snapshot, 2)).await;
    assert!(!final_snapshot.external_view.to_string().contains("game-d"));
}

async fn wait_for<F>(
    observer: &ClusterObserver,
    predicate: F,
) -> clustodian::observe::ClusterSnapshot
where
    F: Fn(&clustodian::observe::ClusterSnapshot) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let snapshot = observer.snapshot().await.expect("observe game cluster");
        if predicate(&snapshot) {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for game convergence: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn assigned_to(snapshot: &clustodian::observe::ClusterSnapshot, instance: &str) -> bool {
    snapshot.external_view[RESOURCE]
        .as_object()
        .is_some_and(|partitions| {
            partitions.values().any(|partition| {
                partition
                    .as_object()
                    .is_some_and(|entries| entries.contains_key(instance))
            })
        })
}

fn settled(snapshot: &clustodian::observe::ClusterSnapshot, live: usize) -> bool {
    snapshot.live_instances.len() == live
        && snapshot
            .processed_revision
            .is_some_and(|processed| processed >= snapshot.authoritative_revision)
        && snapshot.pending_transitions.is_empty()
        && snapshot.external_view[RESOURCE]
            .as_object()
            .is_some_and(|partitions| {
                partitions.len() == 6
                    && partitions.values().all(|partition| {
                        let Some(entries) = partition.as_object() else {
                            return false;
                        };
                        entries.len() == live.min(2)
                            && entries
                                .values()
                                .filter(|state| state.as_str() == Some("LEADER"))
                                .count()
                                == 1
                            && entries
                                .values()
                                .filter(|state| state.as_str() == Some("STANDBY"))
                                .count()
                                == live.min(2).saturating_sub(1)
                    })
            })
}

fn leader(view: &Value, partition: &str) -> String {
    view[RESOURCE][partition]
        .as_object()
        .unwrap()
        .iter()
        .find(|(_, state)| state.as_str() == Some("LEADER"))
        .map(|(instance, _)| instance.clone())
        .expect("partition leader")
}

fn free_port() -> u16 {
    clustodian_test_support::allocate_port().expect("allocate coordinated test port")
}
