#![cfg(not(feature = "shuttle"))]

use clustodian::admin::{ClusterAdmin, InstanceSpec, PlacementSpec, ResourceSpec};
use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::{ClusterObserver, ClusterSnapshot};
use clustodian::transition::TransitionMessage;
use clustodian_rolling_restart::{
    preference_lists, PARTICIPANTS, PARTITION_COUNT, REPLICA_COUNT, RESOURCE,
};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tempfile::{tempdir, TempDir};

static NEXT_RUN: AtomicU64 = AtomicU64::new(1);

struct EtcdFixture {
    child: Option<Child>,
    data_dir: Option<TempDir>,
    endpoint: String,
}

impl EtcdFixture {
    async fn start() -> Self {
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
        let peer = format!("http://127.0.0.1:{peer_port}");
        let data_dir = tempdir().expect("temporary etcd directory");
        let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| "etcd".to_owned());
        let child = Command::new(binary)
            .args([
                "--name",
                "rolling-restart-test",
                "--data-dir",
                data_dir.path().to_str().expect("temporary path is utf8"),
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer,
                "--initial-advertise-peer-urls",
                &peer,
                "--initial-cluster",
                &format!("rolling-restart-test={peer}"),
                "--initial-cluster-state",
                "new",
                "--initial-cluster-token",
                "rolling-restart-test",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("install etcd or set CLUSTODIAN_ETCD_TEST_ENDPOINT");
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
                    clustodian_test_support::child_exit_message(child, "rolling-restart etcd")
                {
                    panic!("{error}");
                }
            }
            if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: self.endpoint.clone(),
                prefix: prefix.to_owned(),
                cluster: "rolling-restart-integration".to_owned(),
            })
            .await
            {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return backend;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("etcd did not become ready")
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
    children: Vec<Child>,
}

impl Drop for Processes {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = signal(child, "CONT");
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn free_port() -> u16 {
    clustodian_test_support::allocate_port().expect("allocate coordinated test port")
}

fn binary() -> String {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_rolling-restart") {
        return path;
    }

    let mut path = std::env::current_exe().expect("current test binary path");
    path.pop();
    if path.file_name().is_some_and(|name| name == "deps") {
        path.pop();
    }
    path.push(format!("rolling-restart{}", std::env::consts::EXE_SUFFIX));
    path.to_string_lossy().into_owned()
}

fn spawn(args: &[&str], envs: &[(&str, &str)]) -> Child {
    let mut command = Command::new(binary());
    command
        .args(args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (key, value) in envs {
        command.env(key, value);
    }
    command.spawn().expect("spawn rolling-restart process")
}

fn signal(child: &Child, name: &str) -> bool {
    Command::new("kill")
        .args([format!("-{name}"), child.id().to_string()])
        .status()
        .is_ok_and(|status| status.success())
}

async fn wait_snapshot(
    backend: &EtcdCoordination,
    predicate: impl Fn(&ClusterSnapshot) -> bool,
) -> ClusterSnapshot {
    let observer = ClusterObserver::new(backend.clone());
    let mut last = None;
    for _ in 0..240 {
        let snapshot = observer.snapshot().await.expect("cluster snapshot");
        if predicate(&snapshot) {
            return snapshot;
        }
        last = Some(snapshot);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for cluster predicate: {last:#?}")
}

fn assert_settled(snapshot: &ClusterSnapshot, expected_state_b_session: Option<u64>) {
    assert_eq!(snapshot.live_instances.len(), 3);
    let resources = snapshot.external_view[RESOURCE]
        .as_object()
        .expect("ledger external view");
    assert_eq!(resources.len(), PARTITION_COUNT);
    for partition in resources.values() {
        let replicas = partition.as_object().expect("partition replicas");
        assert_eq!(replicas.len(), REPLICA_COUNT);
        assert_eq!(
            replicas
                .values()
                .filter(|state| state.as_str() == Some("LEADER"))
                .count(),
            1
        );
        assert_eq!(
            replicas
                .values()
                .filter(|state| state.as_str() == Some("STANDBY"))
                .count(),
            2
        );
    }
    assert!(snapshot.pending_transitions.is_empty());
    if let Some(session) = expected_state_b_session {
        assert_eq!(snapshot.active_current_state["state-b"].session, session);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rolling_restart_rejects_stale_state_and_message_process_level() {
    let mut fixture = EtcdFixture::start().await;
    let prefix = format!(
        "rolling-restart-test-{}-{}",
        std::process::id(),
        NEXT_RUN.fetch_add(1, Ordering::Relaxed)
    );
    let backend = fixture.connect(&prefix).await;
    let admin = ClusterAdmin::new(backend.clone());
    admin
        .ensure_cluster("rolling-restart-integration")
        .await
        .expect("cluster metadata");
    for instance in PARTICIPANTS {
        admin
            .put_instance(InstanceSpec {
                instance_id: instance.to_owned(),
                zone: format!("z-{instance}"),
            })
            .await
            .expect("instance metadata");
    }
    admin
        .put_resource(ResourceSpec {
            name: RESOURCE.to_owned(),
            partitions: PARTITION_COUNT,
            replicas: REPLICA_COUNT,
            state_model: "LeaderStandby".to_owned(),
            placement: PlacementSpec::SemiAuto {
                preference_lists: preference_lists(),
            },
        })
        .await
        .expect("resource metadata");

    let hit_dir = tempdir().expect("callback directory");
    let control = hit_dir.path().join("control");
    let release = hit_dir.path().join("release");
    let hits = hit_dir.path().join("hits");
    let envs = || {
        vec![
            ("ROLLING_ETCD_ENDPOINT", fixture.endpoint.as_str()),
            ("ROLLING_PREFIX", prefix.as_str()),
            ("ROLLING_CLUSTER", "rolling-restart-integration"),
            (
                "ROLLING_CALLBACK_CONTROL",
                control.to_str().expect("control path"),
            ),
            ("ROLLING_CALLBACK_CONTROL_INSTANCE", "state-b"),
            (
                "ROLLING_CALLBACK_RELEASE",
                release.to_str().expect("release path"),
            ),
            ("ROLLING_CALLBACK_HITS", hits.to_str().expect("hits path")),
        ]
    };
    let mut processes = Processes {
        children: Vec::new(),
    };
    let mut state_b_index = None;
    for id in PARTICIPANTS {
        if id == "state-b" {
            state_b_index = Some(processes.children.len());
        }
        processes
            .children
            .push(spawn(&["participant", id], &envs()));
    }
    let state_b_index = state_b_index.expect("state-b participant index");

    wait_snapshot(&backend, |snapshot| snapshot.live_instances.len() == 3).await;
    let controller_start = processes.children.len();
    for id in ["controller-a", "controller-b", "controller-c"] {
        processes.children.push(spawn(&["controller", id], &envs()));
    }
    let controller_indices = controller_start..processes.children.len();

    let initial = wait_snapshot(&backend, |snapshot| {
        snapshot.live_instances.len() == 3
            && snapshot.controllers.active.len() == 1
            && snapshot.controllers.standby.len() == 2
            && snapshot.pending_transitions.is_empty()
            && settled_shape(snapshot)
    })
    .await;
    assert_settled(&initial, None);

    // The pending-transition queue is controller-owned derived state. Freeze
    // every controller before injecting the deliberately held transition so
    // a concurrent reconciliation cannot replace it while this test is
    // establishing the in-flight callback boundary.
    for index in controller_indices.clone() {
        assert!(signal(&processes.children[index], "STOP"));
    }
    let old_session = initial.live_instances["state-b"];

    let partition = initial.active_current_state["state-b"].resources[RESOURCE]
        .keys()
        .next()
        .expect("state-b partition")
        .clone();
    let current_state = &initial.active_current_state["state-b"].resources[RESOURCE][&partition];
    let (from, to) = if current_state == "LEADER" {
        ("LEADER", "STANDBY")
    } else {
        ("STANDBY", "LEADER")
    };
    let message = TransitionMessage {
        message_id: "blocked-old-callback".to_owned(),
        resource: RESOURCE.to_owned(),
        partition: partition.clone(),
        instance: "state-b".to_owned(),
        target_session: old_session,
        from: from.to_owned(),
        to: to.to_owned(),
        message_type: "STATE_TRANSITION".to_owned(),
    };
    std::fs::write(&control, b"block").expect("callback barrier");
    backend
        .inject_pending_transition(&message)
        .await
        .expect("inject callback transition");
    wait_file_contains(&hits, "phase=started message=blocked-old-callback").await;
    assert!(signal(&processes.children[state_b_index], "STOP"));
    wait_snapshot(&backend, |snapshot| {
        !snapshot.live_instances.contains_key("state-b")
    })
    .await;

    let replacement_envs = envs()
        .into_iter()
        .filter(|(key, _)| *key != "ROLLING_CALLBACK_CONTROL")
        .collect::<Vec<_>>();
    processes
        .children
        .push(spawn(&["participant", "state-b"], &replacement_envs));
    let replacement = wait_snapshot(&backend, |snapshot| {
        snapshot
            .live_instances
            .get("state-b")
            .is_some_and(|session| *session > old_session)
    })
    .await;
    let new_session = replacement.live_instances["state-b"];
    assert!(new_session > old_session);
    let observer = ClusterObserver::new(backend.clone());
    assert!(observer
        .session_current_state_exists("state-b", &old_session.to_string())
        .await
        .expect("old session metadata"));
    assert_eq!(
        replacement.active_current_state["state-b"].session,
        new_session
    );

    let stale = TransitionMessage {
        message_id: "stale-after-restart".to_owned(),
        resource: RESOURCE.to_owned(),
        partition,
        instance: "state-b".to_owned(),
        target_session: old_session,
        from: "OFFLINE".to_owned(),
        to: "STANDBY".to_owned(),
        message_type: "STATE_TRANSITION".to_owned(),
    };
    backend
        .inject_pending_transition(&stale)
        .await
        .expect("inject stale transition");
    wait_snapshot(&backend, |snapshot| snapshot.pending_transitions.is_empty()).await;
    let hit_text = std::fs::read_to_string(&hits).expect("callback hits");
    assert!(!hit_text.contains("message=stale-after-restart"));

    std::fs::write(&release, b"release").expect("release callback barrier");
    assert!(signal(&processes.children[state_b_index], "CONT"));
    for index in controller_indices {
        assert!(signal(&processes.children[index], "CONT"));
    }
    wait_file_contains(&hits, "phase=finished message=blocked-old-callback").await;
    let final_snapshot = wait_snapshot(&backend, |snapshot| {
        snapshot.pending_transitions.is_empty()
            && snapshot
                .active_current_state
                .get("state-b")
                .is_some_and(|active| active.session == new_session)
            && settled_shape(snapshot)
    })
    .await;
    assert_settled(&final_snapshot, Some(new_session));
    assert!(observer
        .session_current_state_exists("state-b", &old_session.to_string())
        .await
        .expect("retained old metadata"));
}

fn settled_shape(snapshot: &ClusterSnapshot) -> bool {
    snapshot
        .processed_revision
        .is_some_and(|processed| processed >= snapshot.authoritative_revision)
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
                        replicas.len() == REPLICA_COUNT
                            && replicas
                                .values()
                                .filter(|state| state.as_str() == Some("LEADER"))
                                .count()
                                == 1
                            && replicas
                                .values()
                                .filter(|state| state.as_str() == Some("STANDBY"))
                                .count()
                                == 2
                    })
            })
}

async fn wait_file_contains(path: &Path, needle: &str) {
    for _ in 0..240 {
        if std::fs::read_to_string(path)
            .unwrap_or_default()
            .contains(needle)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "timed out waiting for {needle}; callback log={:?}",
        std::fs::read_to_string(path).unwrap_or_default()
    )
}
