#![cfg(not(feature = "shuttle"))]

use clustodian::admin::{ClusterAdmin, InstanceSpec, PlacementSpec, ResourceSpec};
use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::ClusterObserver;
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct EtcdFixture {
    child: Option<Child>,
    data_dir: Option<TempDir>,
    endpoint: String,
    prefix: String,
}

impl EtcdFixture {
    async fn new() -> Option<Self> {
        let prefix = format!(
            "replicated-kv-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        );
        if let Ok(endpoint) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Some(Self {
                child: None,
                data_dir: None,
                endpoint,
                prefix,
            });
        }
        let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| String::from("etcd"));
        let client_port = free_port()?;
        let peer_port = free_port()?;
        let endpoint = format!("http://127.0.0.1:{client_port}");
        let peer_endpoint = format!("http://127.0.0.1:{peer_port}");
        let data_dir = tempdir().ok()?;
        let child = Command::new(binary)
            .args([
                "--name",
                "replicated-kv-test",
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
                &format!("replicated-kv-test={peer_endpoint}"),
                "--initial-cluster-state",
                "new",
                "--initial-cluster-token",
                "replicated-kv-tests",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .ok()?;
        let mut fixture = Self {
            child: Some(child),
            data_dir: Some(data_dir),
            endpoint,
            prefix,
        };
        for _ in 0..120 {
            if let Some(child) = fixture.child.as_mut() {
                if let Some(error) =
                    clustodian_test_support::child_exit_message(child, "replicated-kv etcd")
                {
                    panic!("{error}");
                }
            }
            if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: fixture.endpoint.clone(),
                prefix: fixture.prefix.clone(),
                cluster: String::from("replicated-kv"),
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

    async fn backend(&self) -> EtcdCoordination {
        for _ in 0..120 {
            if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: self.endpoint.clone(),
                prefix: self.prefix.clone(),
                cluster: String::from("replicated-kv"),
            })
            .await
            {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return backend;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("etcd is ready")
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

struct Node {
    name: String,
    address: String,
    child: Option<Child>,
}

impl Node {
    fn kill(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_kv_writes_and_survives_leader_failure() {
    let Some(fixture) = EtcdFixture::new().await else {
        panic!("etcd is required; install etcd or set CLUSTODIAN_ETCD_TEST_ENDPOINT");
    };
    let backend = fixture.backend().await;
    let admin = ClusterAdmin::new(backend.clone());
    admin.ensure_cluster("replicated-kv").await.unwrap();
    for name in ["node-a", "node-b", "node-c"] {
        admin
            .put_instance(InstanceSpec {
                instance_id: name.to_owned(),
                zone: String::from("local"),
            })
            .await
            .unwrap();
    }
    let mut preferences = BTreeMap::new();
    preferences.insert(
        String::from("kv_0"),
        vec![
            String::from("node-a"),
            String::from("node-b"),
            String::from("node-c"),
        ],
    );
    admin
        .put_resource(ResourceSpec {
            name: String::from("kv"),
            partitions: 1,
            replicas: 3,
            state_model: String::from("LeaderStandby"),
            placement: PlacementSpec::SemiAuto {
                preference_lists: preferences,
            },
        })
        .await
        .unwrap();

    let binary = std::env::var("CARGO_BIN_EXE_replicated-kv")
        .expect("cargo exposes the example binary to integration tests");
    let common = [
        ("CLUSTODIAN_KV_ETCD_ENDPOINT", fixture.endpoint.as_str()),
        ("CLUSTODIAN_KV_PREFIX", fixture.prefix.as_str()),
        ("CLUSTODIAN_KV_CLUSTER", "replicated-kv"),
    ];
    let ports = [
        free_port().unwrap(),
        free_port().unwrap(),
        free_port().unwrap(),
    ];
    let peers = format!(
        "node-a=127.0.0.1:{},node-b=127.0.0.1:{},node-c=127.0.0.1:{}",
        ports[0], ports[1], ports[2]
    );
    let mut nodes = Vec::new();
    for (index, name) in ["node-a", "node-b", "node-c"].iter().enumerate() {
        let address = format!("127.0.0.1:{}", ports[index]);
        let mut command = Command::new(&binary);
        command.arg("node");
        for (key, value) in common {
            command.env(key, value);
        }
        let child = command
            .env("CLUSTODIAN_KV_INSTANCE_ID", name)
            .env("CLUSTODIAN_KV_LISTEN", &address)
            .env("CLUSTODIAN_KV_PEERS", &peers)
            .env("CLUSTODIAN_KV_PARTICIPANT_LEASE_TTL_MS", "2000")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("participant starts");
        nodes.push(Node {
            name: (*name).to_owned(),
            address,
            child: Some(child),
        });
    }
    for node in &nodes {
        wait_for_data_plane(&node.address).await;
    }

    let mut controller = Command::new(&binary);
    controller.arg("controller");
    for (key, value) in common {
        controller.env(key, value);
    }
    controller
        .env("CLUSTODIAN_KV_CONTROLLER_ID", "controller-1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut controller = controller.spawn().expect("controller starts");

    let observer = ClusterObserver::new(backend.clone());
    let initial = wait_for(&observer, |snapshot| {
        let states = partition_states(&snapshot.external_view);
        snapshot.controllers.active.len() == 1
            && snapshot.live_instances.len() == 3
            && snapshot
                .processed_revision
                .is_some_and(|processed| processed >= snapshot.authoritative_revision)
            && snapshot.pending_transitions.is_empty()
            && states.values().filter(|state| *state == "LEADER").count() == 1
            && states.values().filter(|state| *state == "STANDBY").count() == 2
    })
    .await;
    let leader = partition_states(&initial.external_view)
        .into_iter()
        .find(|(_, state)| state == "LEADER")
        .map(|(name, _)| name)
        .unwrap();
    let leader_index = nodes.iter().position(|node| node.name == leader).unwrap();
    wait_for_role(&nodes[leader_index].address, "LEADER").await;
    let put_response = send(&nodes[leader_index].address, "PUT greeting hello")
        .await
        .unwrap();
    assert!(
        put_response.starts_with("OK"),
        "unexpected PUT response: {put_response}"
    );
    for node in &nodes {
        assert_eq!(
            send(&node.address, "GET greeting").await.unwrap(),
            "VALUE hello",
            "replica {} did not receive the committed value",
            node.name
        );
    }

    nodes[leader_index].kill();
    let after = wait_for(&observer, |snapshot| {
        let states = partition_states(&snapshot.external_view);
        snapshot.live_instances.len() == 2
            && !snapshot.live_instances.contains_key(&leader)
            && snapshot
                .processed_revision
                .is_some_and(|processed| processed >= snapshot.authoritative_revision)
            && snapshot.pending_transitions.is_empty()
            && states.values().filter(|state| *state == "LEADER").count() == 1
            && states.values().filter(|state| *state == "STANDBY").count() == 1
            && !states.contains_key(&leader)
    })
    .await;
    let promoted = partition_states(&after.external_view)
        .into_iter()
        .find(|(_, state)| state == "LEADER")
        .map(|(name, _)| name)
        .unwrap();
    let promoted_index = nodes.iter().position(|node| node.name == promoted).unwrap();
    assert_ne!(promoted, leader);
    assert_eq!(
        send(&nodes[promoted_index].address, "GET greeting")
            .await
            .unwrap(),
        "VALUE hello"
    );
    assert!(send(&nodes[promoted_index].address, "PUT farewell goodbye")
        .await
        .unwrap()
        .starts_with("OK"));
    assert_eq!(
        send(&nodes[promoted_index].address, "GET farewell")
            .await
            .unwrap(),
        "VALUE goodbye"
    );
    controller.kill().unwrap();
    let _ = controller.wait();
}

fn partition_states(view: &Value) -> BTreeMap<String, String> {
    view["kv"]["kv_0"]
        .as_object()
        .map(|entries| {
            entries
                .iter()
                .map(|(instance, state)| (instance.clone(), state.as_str().unwrap().to_owned()))
                .collect()
        })
        .unwrap_or_default()
}

async fn wait_for<F>(
    observer: &ClusterObserver,
    predicate: F,
) -> clustodian::observe::ClusterSnapshot
where
    F: Fn(&clustodian::observe::ClusterSnapshot) -> bool,
{
    let mut last_snapshot = None;
    for _ in 0..240 {
        let snapshot = observer
            .snapshot()
            .await
            .expect("observer snapshot succeeds");
        if predicate(&snapshot) {
            return snapshot;
        }
        last_snapshot = Some(snapshot);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for cluster convergence: {last_snapshot:#?}");
}

async fn send(address: &str, request: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(b"\n").await?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).await?;
    Ok(response.trim_end().to_owned())
}

async fn wait_for_role(address: &str, expected_role: &str) {
    for _ in 0..400 {
        if let Ok(response) = send(address, "STATUS").await {
            if response.split_whitespace().last() == Some(expected_role) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {expected_role} data plane at {address}");
}

async fn wait_for_data_plane(address: &str) {
    for _ in 0..400 {
        if let Ok(response) = send(address, "STATUS").await {
            if response.starts_with("STATUS ") {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for data plane at {address}");
}

fn free_port() -> Option<u16> {
    clustodian_test_support::allocate_port().ok()
}
