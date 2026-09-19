#![cfg(not(feature = "shuttle"))]

use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::{ClusterObserver, ClusterSnapshot};
use serde_json::Value;
use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

const CLUSTER: &str = "job-workers";
const RESOURCE: &str = "job-queues";

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
            "job-workers-test-{}-{}",
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
                "job-workers-test",
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
                &format!("job-workers-test={peer_endpoint}"),
                "--initial-cluster-state",
                "new",
                "--initial-cluster-token",
                "job-workers-tests",
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
                    clustodian_test_support::child_exit_message(child, "job-workers etcd")
                {
                    eprintln!("{error}");
                    return None;
                }
            }
            if let Ok(backend) = fixture.backend_once().await {
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
            if let Ok(backend) = self.backend_once().await {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return backend;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("etcd did not become ready at {}", self.endpoint);
    }

    async fn backend_once(
        &self,
    ) -> Result<EtcdCoordination, clustodian::coordination::etcd::CoordinationError> {
        EtcdCoordination::connect(EtcdCoordinationConfig {
            endpoint: self.endpoint.clone(),
            prefix: self.prefix.clone(),
            cluster: CLUSTER.to_owned(),
        })
        .await
    }

    fn envs(&self) -> [(&str, &str); 3] {
        [
            ("JOB_WORKERS_ETCD_ENDPOINT", self.endpoint.as_str()),
            ("JOB_WORKERS_ETCD_PREFIX", self.prefix.as_str()),
            ("JOB_WORKERS_CLUSTER", CLUSTER),
        ]
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

struct ManagedChild {
    child: Option<Child>,
}

impl ManagedChild {
    fn spawn(command: &mut Command) -> Self {
        let child = command
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("child process starts");
        Self { child: Some(child) }
    }

    fn kill(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        self.kill();
    }
}

struct WorkerProcess {
    name: String,
    address: String,
    child: ManagedChild,
}

impl WorkerProcess {
    fn kill(&mut self) {
        self.child.kill();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_death_promotes_standby_and_new_owner_processes_jobs() {
    let Some(fixture) = EtcdFixture::new().await else {
        panic!("etcd is required; install etcd or set CLUSTODIAN_ETCD_TEST_ENDPOINT");
    };
    let binary = std::env::var("CARGO_BIN_EXE_clustodian-job-workers")
        .expect("cargo exposes the example binary to integration tests");

    assert!(
        command(&binary, &fixture)
            .arg("setup")
            .status()
            .expect("setup command runs")
            .success(),
        "setup command failed"
    );

    let mut controller = ManagedChild::spawn(
        command(&binary, &fixture)
            .arg("controller")
            .env("JOB_WORKERS_CONTROLLER_ID", "controller-1")
            .env("JOB_WORKERS_CONTROLLER_LEASE_TTL_MS", "1000"),
    );

    let ports = [
        free_port().expect("free worker port"),
        free_port().expect("free worker port"),
        free_port().expect("free worker port"),
    ];
    let mut workers = Vec::new();
    for (index, name) in ["worker-a", "worker-b", "worker-c"].into_iter().enumerate() {
        let port = ports[index];
        let child = ManagedChild::spawn(
            command(&binary, &fixture)
                .arg("worker")
                .args(["--instance", name, "--port", &port.to_string()])
                .env("JOB_WORKERS_PARTICIPANT_LEASE_TTL_SECS", "1"),
        );
        let address = format!("127.0.0.1:{port}");
        wait_for_worker_api(&address).await;
        workers.push(WorkerProcess {
            name: name.to_owned(),
            address,
            child,
        });
    }

    let observer = ClusterObserver::new(fixture.backend().await);
    let initial = wait_for(&observer, settled_with_live_workers(3)).await;
    let partition = "job-queues_0";
    let initial_states = partition_states(&initial, partition);
    let initial_owner = worker_in_state(&initial_states, "LEADER");
    let initial_standby = worker_in_state(&initial_states, "STANDBY");

    let owner = workers
        .iter()
        .find(|worker| worker.name == initial_owner)
        .expect("initial owner process exists");
    let standby = workers
        .iter()
        .find(|worker| worker.name == initial_standby)
        .expect("initial standby process exists");
    wait_for_role(&owner.address, partition, "LEADER").await;
    wait_for_role(&standby.address, partition, "STANDBY").await;

    let first = process(&binary, &fixture, &owner.address, partition, "job-1").await;
    assert_eq!(first["status"], "processed");
    assert_eq!(first["instance"], initial_owner);
    assert_eq!(first["partition"], partition);
    assert_eq!(first["job"], "job-1");
    assert_eq!(first["count"], 1);

    let rejected = process(
        &binary,
        &fixture,
        &standby.address,
        partition,
        "job-standby",
    )
    .await;
    assert_eq!(rejected["status"], "not_owner");
    assert_eq!(rejected["instance"], initial_standby);
    assert_eq!(rejected["role"], "STANDBY");

    let owner_index = workers
        .iter()
        .position(|worker| worker.name == initial_owner)
        .expect("owner process index");
    workers[owner_index].kill();

    let after_failure = wait_for(&observer, |snapshot| {
        let states = partition_states(snapshot, partition);
        settled_with_live_workers(2)(snapshot)
            && !snapshot.live_instances.contains_key(&initial_owner)
            && !states.contains_key(&initial_owner)
            && states
                .values()
                .filter(|state| state.as_str() == "LEADER")
                .count()
                == 1
    })
    .await;
    let promoted = worker_in_state(&partition_states(&after_failure, partition), "LEADER");
    assert_eq!(promoted, initial_standby);

    let promoted_worker = workers
        .iter()
        .find(|worker| worker.name == promoted)
        .expect("promoted worker process exists");
    wait_for_role(&promoted_worker.address, partition, "LEADER").await;
    let second = process(
        &binary,
        &fixture,
        &promoted_worker.address,
        partition,
        "job-2",
    )
    .await;
    assert_eq!(second["status"], "processed");
    assert_eq!(second["instance"], promoted);
    assert_eq!(second["partition"], partition);
    assert_eq!(second["job"], "job-2");
    assert_eq!(second["count"], 1);

    controller.kill();
}

fn command(binary: &str, fixture: &EtcdFixture) -> Command {
    let mut command = Command::new(binary);
    for (key, value) in fixture.envs() {
        command.env(key, value);
    }
    command
}

fn settled_with_live_workers(live_workers: usize) -> impl Fn(&ClusterSnapshot) -> bool + Copy {
    move |snapshot| {
        snapshot.controllers.active.len() == 1
            && snapshot.live_instances.len() == live_workers
            && snapshot
                .processed_revision
                .is_some_and(|processed| processed >= snapshot.authoritative_revision)
            && snapshot.pending_transitions.is_empty()
            && external_view_is_settled(snapshot, live_workers)
    }
}

fn external_view_is_settled(snapshot: &ClusterSnapshot, live_workers: usize) -> bool {
    let Some(partitions) = snapshot
        .external_view
        .get(RESOURCE)
        .and_then(Value::as_object)
    else {
        return false;
    };
    if partitions.len() != 3 {
        return false;
    }
    partitions.values().all(|states| {
        let Some(states) = states.as_object() else {
            return false;
        };
        states
            .values()
            .filter(|state| state.as_str() == Some("LEADER"))
            .count()
            == 1
            && states
                .values()
                .filter(|state| state.as_str() == Some("STANDBY"))
                .count()
                <= live_workers.saturating_sub(1).min(1)
            && states.len() <= live_workers
    })
}

fn partition_states(snapshot: &ClusterSnapshot, partition: &str) -> BTreeMap<String, String> {
    snapshot.external_view[RESOURCE][partition]
        .as_object()
        .map(|entries| {
            entries
                .iter()
                .map(|(instance, state)| (instance.clone(), state.as_str().unwrap().to_owned()))
                .collect()
        })
        .unwrap_or_default()
}

fn worker_in_state(states: &BTreeMap<String, String>, expected_state: &str) -> String {
    states
        .iter()
        .find(|(_, state)| state.as_str() == expected_state)
        .map(|(worker, _)| worker.clone())
        .unwrap_or_else(|| panic!("no worker in state {expected_state}: {states:?}"))
}

async fn wait_for<F>(observer: &ClusterObserver, predicate: F) -> ClusterSnapshot
where
    F: Fn(&ClusterSnapshot) -> bool,
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

async fn process(
    binary: &str,
    fixture: &EtcdFixture,
    address: &str,
    partition: &str,
    job: &str,
) -> Value {
    let output = command(binary, fixture)
        .arg("process")
        .args([
            "--port",
            address.rsplit_once(':').expect("address includes port").1,
            "--partition",
            partition,
            "--job",
            job,
        ])
        .output()
        .expect("process command runs");
    assert!(
        output.status.success(),
        "process command failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("process command prints JSON")
}

async fn worker_status(address: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(b"STATUS\n").await?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).await?;
    Ok(serde_json::from_str(response.trim_end())?)
}

async fn wait_for_worker_api(address: &str) {
    for _ in 0..120 {
        if worker_status(address).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for worker data plane at {address}");
}

async fn wait_for_role(address: &str, partition: &str, expected_role: &str) {
    for _ in 0..120 {
        if let Ok(status) = worker_status(address).await {
            if status["roles"][partition].as_str() == Some(expected_role) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {expected_role} on {partition} at {address}");
}

fn free_port() -> Option<u16> {
    clustodian_test_support::allocate_port().ok()
}
