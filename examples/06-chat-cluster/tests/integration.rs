#![cfg(all(unix, not(feature = "shuttle")))]

use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::{ClusterObserver, ClusterSnapshot};
use clustodian_chat_cluster::{leader_for_room, room_partition, RESOURCE};
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};

static NEXT_TEST: AtomicU64 = AtomicU64::new(1);

struct EtcdFixture {
    child: Option<Child>,
    _data_dir: Option<TempDir>,
    endpoint: String,
}

impl EtcdFixture {
    async fn new() -> Option<Self> {
        let prefix = "unused";
        if let Ok(endpoint) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Some(Self {
                child: None,
                _data_dir: None,
                endpoint,
            });
        }
        let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| "etcd".into());
        let client_port = free_port()?;
        let peer_port = free_port()?;
        let endpoint = format!("http://127.0.0.1:{client_port}");
        let peer_endpoint = format!("http://127.0.0.1:{peer_port}");
        let data_dir = tempdir().ok()?;
        let child = Command::new(binary)
            .args([
                "--name",
                "chat-cluster-test",
                "--data-dir",
                data_dir.path().to_str()?,
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer_endpoint,
                "--initial-advertise-peer-urls",
                &peer_endpoint,
                "--initial-cluster",
                &format!("chat-cluster-test={peer_endpoint}"),
                "--initial-cluster-state",
                "new",
                "--initial-cluster-token",
                "chat-cluster-tests",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .ok()?;
        let mut fixture = Self {
            child: Some(child),
            _data_dir: Some(data_dir),
            endpoint,
        };
        for _ in 0..120 {
            if let Some(child) = fixture.child.as_mut() {
                if let Some(error) =
                    clustodian_test_support::child_exit_message(child, "chat-cluster etcd")
                {
                    eprintln!("{error}");
                    return None;
                }
            }
            if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: fixture.endpoint.clone(),
                prefix: prefix.into(),
                cluster: "probe".into(),
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

impl Drop for EtcdFixture {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct Processes {
    children: BTreeMap<String, Child>,
}

impl Processes {
    fn new() -> Self {
        Self {
            children: BTreeMap::new(),
        }
    }

    fn insert(&mut self, name: impl Into<String>, child: Child) {
        self.children.insert(name.into(), child);
    }

    fn kill(&mut self, name: &str) {
        if let Some(child) = self.children.get_mut(name) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Processes {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
        }
        for child in self.children.values_mut() {
            let _ = child.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn websocket_client_reconnects_when_active_room_owner_dies() {
    let Some(fixture) = EtcdFixture::new().await else {
        eprintln!("skipping chat integration test: etcd is unavailable");
        return;
    };
    let prefix = format!(
        "chat-cluster-integration-{}-{}",
        std::process::id(),
        NEXT_TEST.fetch_add(1, Ordering::Relaxed)
    );
    let cluster = format!("chat-test-{}", std::process::id());
    let backend = connect(&fixture.endpoint, &prefix, &cluster).await;
    let observer = ClusterObserver::new(backend);
    let binary = std::env::var("CARGO_BIN_EXE_chat-cluster")
        .map(PathBuf::from)
        .expect("Cargo must provide the chat-cluster binary to its integration test");
    let mut processes = Processes::new();

    let common = |command: &mut Command| {
        command
            .env("CHAT_ETCD_ENDPOINT", &fixture.endpoint)
            .env("CHAT_PREFIX", &prefix)
            .env("CHAT_CLUSTER", &cluster)
            .env("CHAT_CONTROLLER_LEASE_TTL_MS", "1000")
            .env("CHAT_PARTICIPANT_LEASE_TTL_MS", "1000")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
    };
    let mut setup = Command::new(&binary);
    setup.arg("admin");
    common(&mut setup);
    let setup_output = setup.output().expect("run admin setup");
    assert!(
        setup_output.status.success(),
        "chat admin setup failed: stdout={} stderr={}",
        String::from_utf8_lossy(&setup_output.stdout),
        String::from_utf8_lossy(&setup_output.stderr)
    );

    let controller_ready = tempdir().expect("controller ready directory");
    let controller_ready_file = controller_ready.path().join("ready");
    let mut controller = Command::new(&binary);
    controller
        .arg("controller")
        .env("CHAT_CONTROLLER_ID", "controller-a");
    common(&mut controller);
    controller.env("CHAT_READY_FILE", &controller_ready_file);
    controller.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    processes.insert("controller", controller.spawn().expect("spawn controller"));
    wait_until("controller ready", || controller_ready_file.exists()).await;

    let mut endpoints = BTreeMap::new();
    for (name, port) in [
        ("node-a", free_port().unwrap()),
        ("node-b", free_port().unwrap()),
        ("node-c", free_port().unwrap()),
    ] {
        let address = format!("127.0.0.1:{port}");
        endpoints.insert(name.to_owned(), address.clone());
        let mut participant = Command::new(&binary);
        participant
            .args(["participant", "--instance", name, "--listen", &address])
            .env("CHAT_INSTANCE_ID", name)
            .env("CHAT_LISTEN", &address);
        common(&mut participant);
        processes.insert(name, participant.spawn().expect("spawn participant"));
    }
    let endpoint_config = endpoints
        .iter()
        .map(|(name, address)| format!("{name}={address}"))
        .collect::<Vec<_>>()
        .join(",");

    let initial = wait_for_settled(&observer, 3).await;
    let room = "lobby";
    let partition = room_partition(room);
    let initial_owner = leader_for_room(&initial, room).expect("lobby has an active owner");
    assert_eq!(
        initial.external_view[RESOURCE][&partition]
            .as_object()
            .unwrap()
            .len(),
        2
    );

    let mut client = Command::new(&binary);
    client.args([
        "client",
        "--room",
        room,
        "--message",
        "hello-after-failover",
        "--wait-for-reconnect",
    ]);
    common(&mut client);
    client.stderr(Stdio::inherit());
    client.env("CHAT_NODE_ENDPOINTS", &endpoint_config);
    let mut client_child = client.spawn().expect("spawn WebSocket client");
    let client_lines = line_receiver(client_child.stdout.take().expect("client stdout"));
    assert!(wait_for_line(&client_lines, "CONNECTED room=lobby")
        .await
        .contains(&initial_owner));
    assert!(wait_for_line(&client_lines, "MESSAGE room=lobby")
        .await
        .contains("hello-after-failover"));
    processes.insert("client", client_child);

    processes.kill(&initial_owner);
    let replacement = wait_for_replacement(&observer, room, &initial_owner).await;
    assert_ne!(replacement, initial_owner);
    wait_for_snapshot(&observer, |snapshot| {
        !snapshot.live_instances.contains_key(&initial_owner)
    })
    .await;
    assert!(wait_for_line(&client_lines, "RECONNECTED room=lobby")
        .await
        .contains(&replacement));

    let final_snapshot = wait_for_settled(&observer, 2).await;
    let replicas = final_snapshot.external_view[RESOURCE][&partition]
        .as_object()
        .expect("settled room has replica states");
    assert_eq!(replicas.len(), 2);
    assert_eq!(
        replicas
            .values()
            .filter(|state| state == &&Value::String("LEADER".into()))
            .count(),
        1
    );
    assert!(final_snapshot.pending_transitions.is_empty());
}

async fn connect(endpoint: &str, prefix: &str, cluster: &str) -> EtcdCoordination {
    EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint: endpoint.into(),
        prefix: prefix.into(),
        cluster: cluster.into(),
    })
    .await
    .expect("connect to test etcd")
}

async fn wait_for_settled(observer: &ClusterObserver, live_count: usize) -> ClusterSnapshot {
    wait_for_snapshot(observer, move |snapshot| {
        if snapshot.live_instances.len() != live_count
            || !snapshot
                .processed_revision
                .is_some_and(|processed| processed >= snapshot.authoritative_revision)
            || !snapshot.pending_transitions.is_empty()
        {
            return false;
        }
        let Some(resources) = snapshot.external_view[RESOURCE].as_object() else {
            return false;
        };
        resources.len() == 4
            && resources.values().all(|partition| {
                let Some(states) = partition.as_object() else {
                    return false;
                };
                states.len() == 2
                    && states.values().any(|state| state == "LEADER")
                    && states.values().any(|state| state == "STANDBY")
            })
    })
    .await
}

async fn wait_for_replacement(observer: &ClusterObserver, room: &str, old_owner: &str) -> String {
    wait_for_snapshot(observer, |snapshot| {
        leader_for_room(snapshot, room).is_some_and(|owner| owner != old_owner)
    })
    .await;
    leader_for_room(&observer.snapshot().await.expect("read replacement"), room)
        .expect("replacement owner")
}

async fn wait_for_snapshot<F>(observer: &ClusterObserver, predicate: F) -> ClusterSnapshot
where
    F: Fn(&ClusterSnapshot) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(snapshot) = observer.snapshot().await {
            if predicate(&snapshot) {
                return snapshot;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for the expected cluster snapshot")
}

async fn wait_until<F>(description: &str, predicate: F)
where
    F: Fn() -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {description}");
}

fn line_receiver(stdout: ChildStdout) -> Receiver<String> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    receiver
}

async fn wait_for_line(receiver: &Receiver<String>, needle: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(line) = receiver.try_recv() {
            if line.contains(needle) {
                return line;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for client output containing {needle}");
}

fn free_port() -> Option<u16> {
    clustodian_test_support::allocate_port().ok()
}
