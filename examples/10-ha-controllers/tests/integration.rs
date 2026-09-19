#![cfg(all(unix, not(feature = "shuttle")))]

use clustodian::admin::{ClusterAdmin, InstanceSpec, PlacementSpec, ResourceSpec};
use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::{ClusterObserver, ClusterSnapshot};
use clustodian_ha_controllers::{PARTITIONS, RESOURCE};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};

static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

struct Etcd {
    child: Option<Child>,
    _dir: Option<TempDir>,
    endpoint: String,
    prefix: String,
}
impl Etcd {
    async fn new() -> Option<Self> {
        let prefix = format!(
            "ha-test-{}-{}",
            std::process::id(),
            NEXT_RUN.fetch_add(1, Ordering::Relaxed)
        );
        if let Ok(endpoint) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Some(Self {
                child: None,
                _dir: None,
                endpoint,
                prefix,
            });
        }
        let endpoint = format!("http://127.0.0.1:{}", free_port()?);
        let peer = format!("http://127.0.0.1:{}", free_port()?);
        let dir = tempdir().ok()?;
        let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| "etcd".into());
        let child = Command::new(binary)
            .args([
                "--name",
                "ha-test",
                "--data-dir",
                dir.path().to_str()?,
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer,
                "--initial-advertise-peer-urls",
                &peer,
                "--initial-cluster",
                &format!("ha-test={peer}"),
                "--initial-cluster-state",
                "new",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .ok()?;
        let mut fixture = Self {
            child: Some(child),
            _dir: Some(dir),
            endpoint,
            prefix,
        };
        for _ in 0..160 {
            if let Some(child) = fixture.child.as_mut() {
                if let Some(error) =
                    clustodian_test_support::child_exit_message(child, "ha-controllers etcd")
                {
                    eprintln!("{error}");
                    return None;
                }
            }
            if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: fixture.endpoint.clone(),
                prefix: fixture.prefix.clone(),
                cluster: "ha-control-plane".into(),
            })
            .await
            {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return Some(fixture);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }
}
impl Drop for Etcd {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
struct Processes(Vec<Child>);
impl Drop for Processes {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let pid = child.id().to_string();
            let _ = Command::new("kill").args(["-CONT", &pid]).status();
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn free_port() -> Option<u16> {
    clustodian_test_support::allocate_port().ok()
}
fn common(command: &mut Command, fixture: &Etcd) {
    command
        .env("HA_ETCD_ENDPOINT", &fixture.endpoint)
        .env("HA_PREFIX", &fixture.prefix)
        .env("HA_CLUSTER", "ha-control-plane")
        .env("HA_CONTROLLER_LEASE_TTL_MS", "1000")
        .env("HA_PARTICIPANT_LEASE_TTL_MS", "1500")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
}

fn binary() -> String {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_ha-controllers") {
        return path;
    }
    let mut path = std::env::current_exe().expect("integration test executable path");
    path.pop();
    if path.file_name().is_some_and(|name| name == "deps") {
        path.pop();
    }
    path.push(format!("ha-controllers{}", std::env::consts::EXE_SUFFIX));
    path.to_string_lossy().into_owned()
}
async fn settled(observer: &ClusterObserver, active: Option<&str>) -> ClusterSnapshot {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(snapshot) = observer.snapshot().await {
            let active_ok = active.map_or(true, |id| {
                snapshot.controllers.active == vec![id.to_owned()]
            });
            if active_ok
                && snapshot.live_instances.len() == 4
                && snapshot.pending_transitions.is_empty()
                && PARTITIONS.iter().all(|p| {
                    snapshot.external_view[RESOURCE][p]
                        .as_object()
                        .is_some_and(|e| e.len() == 1 && e.values().any(|s| s == "LEADER"))
                })
            {
                return snapshot;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for HA convergence"
        );
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_sigstop_takeover_sigcont_and_fenced_write() {
    let fixture = Etcd::new()
        .await
        .expect("install etcd or set CLUSTODIAN_ETCD_TEST_ENDPOINT");
    let backend = EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint: fixture.endpoint.clone(),
        prefix: fixture.prefix.clone(),
        cluster: "ha-control-plane".into(),
    })
    .await
    .unwrap();
    let admin = ClusterAdmin::new(backend.clone());
    admin.ensure_cluster("ha-control-plane").await.unwrap();
    for id in [
        "participant-a",
        "participant-b",
        "participant-c",
        "participant-d",
    ] {
        admin
            .put_instance(InstanceSpec {
                instance_id: id.into(),
                zone: "control-plane".into(),
            })
            .await
            .unwrap();
    }
    let preferences = PARTITIONS
        .iter()
        .enumerate()
        .map(|(n, p)| {
            (
                (*p).into(),
                vec![[
                    "participant-a",
                    "participant-b",
                    "participant-c",
                    "participant-d",
                ][n]
                    .into()],
            )
        })
        .collect();
    admin
        .put_resource(ResourceSpec {
            name: RESOURCE.into(),
            partitions: 4,
            replicas: 1,
            state_model: "LeaderStandby".into(),
            placement: PlacementSpec::SemiAuto {
                preference_lists: preferences,
            },
        })
        .await
        .unwrap();
    let binary = binary();
    let stale_dir = tempdir().unwrap();
    let ready_file = stale_dir.path().join("authority-ready");
    let release_file = stale_dir.path().join("release");
    let outcome_file = stale_dir.path().join("outcome");
    let mut processes = Processes(Vec::new());
    for id in [
        "participant-a",
        "participant-b",
        "participant-c",
        "participant-d",
    ] {
        let mut command = Command::new(&binary);
        command.arg("participant").env("HA_INSTANCE_ID", id);
        common(&mut command, &fixture);
        processes.0.push(command.spawn().unwrap());
    }
    let mut first = Command::new(&binary);
    first
        .arg("controller")
        .env("HA_CONTROLLER_ID", "controller-1");
    common(&mut first, &fixture);
    first
        .env("HA_STALE_CONTROLLER_READY_FILE", &ready_file)
        .env("HA_STALE_CONTROLLER_RELEASE_FILE", &release_file)
        .env("HA_STALE_CONTROLLER_OUTCOME_FILE", &outcome_file);
    let first_index = processes.0.len();
    processes.0.push(first.spawn().unwrap());
    let observer = ClusterObserver::new(backend.clone());
    wait_file(&ready_file, "authority-ready").await;
    let initial = settled(&observer, Some("controller-1")).await;
    assert!(backend
        .get_metadata("demo/controller-1-before-failover")
        .await
        .unwrap()
        .is_some());
    let old_id = backend
        .controller_snapshot()
        .await
        .unwrap()
        .controller_election()
        .active()
        .map(|(id, _lease)| id.to_owned())
        .unwrap();
    for id in ["controller-2", "controller-3"] {
        let mut command = Command::new(&binary);
        command.arg("controller").env("HA_CONTROLLER_ID", id);
        common(&mut command, &fixture);
        processes.0.push(command.spawn().unwrap());
    }
    let active_pid = processes.0[first_index].id();
    assert_eq!(initial.controllers.active, vec!["controller-1".to_owned()]);
    Command::new("kill")
        .args(["-STOP", &active_pid.to_string()])
        .status()
        .unwrap();
    let takeover = wait_active_other(&observer, &old_id).await;
    assert_eq!(takeover.controllers.active.len(), 1);
    assert_ne!(takeover.controllers.active[0], old_id);
    Command::new("kill")
        .args(["-CONT", &active_pid.to_string()])
        .status()
        .unwrap();
    std::fs::write(&release_file, b"release").unwrap();
    wait_file(&outcome_file, "rejected: stale controller authority").await;
    assert!(backend
        .get_metadata("demo/controller-1-after-failover")
        .await
        .unwrap()
        .is_none());
    let final_snapshot = settled(&observer, None).await;
    assert_eq!(final_snapshot.controllers.active.len(), 1);
}
async fn wait_active_other(observer: &ClusterObserver, old: &str) -> ClusterSnapshot {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(snapshot) = observer.snapshot().await {
            if snapshot.controllers.active.iter().any(|id| id != old) {
                return snapshot;
            }
        }
        assert!(Instant::now() < deadline, "controller takeover timed out");
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
}
async fn wait_file(path: &Path, needle: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if std::fs::read_to_string(path)
            .unwrap_or_default()
            .contains(needle)
        {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {needle}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
