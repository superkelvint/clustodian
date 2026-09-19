use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::Duration;
use tempfile::{tempdir, TempDir};

static NEXT_PREFIX: AtomicU64 = AtomicU64::new(1);
static SHARED_PROCESSES: OnceLock<Mutex<Vec<Weak<SharedEtcd>>>> = OnceLock::new();
static FIXTURE_GATE: OnceLock<FixtureGate> = OnceLock::new();
const READINESS_ATTEMPTS: usize = 1_200;
const READINESS_RETRY: Duration = Duration::from_millis(25);
const LOCAL_FIXTURE_LIMIT: usize = 4;
const RESTART_ATTEMPTS: usize = 3;

struct FixtureGate {
    available: Mutex<usize>,
    changed: Condvar,
}

impl FixtureGate {
    fn new() -> Self {
        Self {
            available: Mutex::new(LOCAL_FIXTURE_LIMIT),
            changed: Condvar::new(),
        }
    }

    fn acquire(&'static self) -> FixturePermit {
        let mut available = self
            .available
            .lock()
            .expect("etcd fixture gate is not poisoned");
        while *available == 0 {
            available = self
                .changed
                .wait(available)
                .expect("etcd fixture gate is not poisoned");
        }
        *available -= 1;
        FixturePermit { gate: self }
    }
}

struct FixturePermit {
    gate: &'static FixtureGate,
}

impl Drop for FixturePermit {
    fn drop(&mut self) {
        let mut available = self
            .gate
            .available
            .lock()
            .expect("etcd fixture gate is not poisoned");
        *available += 1;
        self.gate.changed.notify_one();
    }
}

fn acquire_fixture() -> FixturePermit {
    FIXTURE_GATE.get_or_init(FixtureGate::new).acquire()
}

struct SharedEtcd {
    child: Mutex<Option<Child>>,
    _data_dir: TempDir,
    endpoint: String,
    in_use: AtomicBool,
}

impl SharedEtcd {
    fn is_alive(&self) -> bool {
        let Ok(mut child) = self.child.lock() else {
            return false;
        };
        let Some(child) = child.as_mut() else {
            return false;
        };
        child.try_wait().is_ok_and(|status| status.is_none())
    }

    fn startup_failure(&self) -> Option<String> {
        let Ok(mut child) = self.child.lock() else {
            return Some(String::from("shared etcd child state is poisoned"));
        };
        child
            .as_mut()
            .and_then(|child| clustodian_test_support::child_exit_message(child, "shared etcd"))
    }
}

impl Drop for SharedEtcd {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.lock() {
            if let Some(mut child) = child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

pub(crate) struct EtcdFixture {
    child: Option<Mutex<Option<Child>>>,
    data_dir: Option<TempDir>,
    #[allow(dead_code)] // Used by the restart integration tests.
    binary: Option<String>,
    #[allow(dead_code)] // Used by the restart integration tests.
    peer_endpoint: Option<String>,
    pub(crate) endpoint: String,
    #[allow(dead_code)] // The default namespace is used by the legacy integration test.
    prefix: String,
    _shared: Option<Arc<SharedEtcd>>,
    _permit: Option<FixturePermit>,
}

impl EtcdFixture {
    pub(crate) fn new() -> Option<Self> {
        let prefix = format!(
            "clustodian-tests-{}-{}",
            std::process::id(),
            NEXT_PREFIX.fetch_add(1, Ordering::Relaxed)
        );
        if let Ok(endpoint) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Some(Self {
                child: None,
                data_dir: None,
                binary: None,
                peer_endpoint: None,
                endpoint,
                prefix,
                _shared: None,
                _permit: None,
            });
        }

        let permit = acquire_fixture();
        let shared = shared_process()?;
        Some(Self {
            child: None,
            data_dir: None,
            binary: None,
            peer_endpoint: None,
            endpoint: shared.endpoint.clone(),
            prefix,
            _shared: Some(shared),
            _permit: Some(permit),
        })
    }

    /// Create a fixture with an independently controllable etcd process.
    ///
    /// Tests that stop or restart etcd must use this instead of `new`, so
    /// ordinary integration tests can share one process safely.
    #[allow(dead_code)]
    pub(crate) fn new_isolated() -> Option<Self> {
        let prefix = format!(
            "clustodian-tests-{}-{}",
            std::process::id(),
            NEXT_PREFIX.fetch_add(1, Ordering::Relaxed)
        );
        if let Ok(endpoint) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Some(Self {
                child: None,
                data_dir: None,
                binary: None,
                peer_endpoint: None,
                endpoint,
                prefix,
                _shared: None,
                _permit: None,
            });
        }

        let permit = acquire_fixture();
        let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| String::from("etcd"));
        let client_port = free_port()?;
        let peer_port = free_port()?;
        let endpoint = format!("http://127.0.0.1:{client_port}");
        let peer_endpoint = format!("http://127.0.0.1:{peer_port}");
        let data_dir = tempdir().ok()?;
        let child = Command::new(&binary)
            .args([
                "--name",
                "clustodian-test",
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
                &format!("clustodian-test={peer_endpoint}"),
                "--initial-cluster-state",
                "new",
                "--initial-cluster-token",
                "clustodian-tests",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .ok()?;
        Some(Self {
            child: Some(Mutex::new(Some(child))),
            data_dir: Some(data_dir),
            binary: Some(binary),
            peer_endpoint: Some(peer_endpoint),
            endpoint,
            prefix,
            _shared: None,
            _permit: Some(permit),
        })
    }

    #[allow(dead_code)] // Compatibility adapter for the existing integration test.
    pub(crate) async fn connect(&self) -> Option<EtcdCoordination> {
        self.connect_namespace(&self.prefix, "test").await
    }

    pub(crate) async fn connect_namespace(
        &self,
        prefix: &str,
        cluster: &str,
    ) -> Option<EtcdCoordination> {
        for _ in 0..READINESS_ATTEMPTS {
            if let Some(shared) = &self._shared {
                if let Some(error) = shared.startup_failure() {
                    panic!("{error}");
                }
            }
            if let Some(child) = &self.child {
                let mut child = child.lock().expect("etcd child state is not poisoned");
                if let Some(child) = child.as_mut() {
                    if let Some(error) =
                        clustodian_test_support::child_exit_message(child, "isolated etcd")
                    {
                        panic!("{error}");
                    }
                }
            }
            if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: self.endpoint.clone(),
                prefix: prefix.to_owned(),
                cluster: cluster.to_owned(),
            })
            .await
            {
                if backend.get_metadata("readiness-probe").await.is_ok() {
                    return Some(backend);
                }
            }
            tokio::time::sleep(READINESS_RETRY).await;
        }
        None
    }

    #[allow(dead_code)] // Used by the restart integration tests.
    pub(crate) fn can_control_process(&self) -> bool {
        self._shared.is_none() && self.child.is_some()
    }

    #[allow(dead_code)] // Used by the restart integration tests.
    pub(crate) fn stop_process(&mut self) -> bool {
        let Some(child_state) = &self.child else {
            return false;
        };
        let Some(mut child) = child_state
            .lock()
            .expect("etcd child state is not poisoned")
            .take()
        else {
            return false;
        };
        let _ = child.kill();
        let _ = child.wait();
        true
    }

    #[allow(dead_code)] // Used by the restart integration tests.
    pub(crate) async fn restart_process(&mut self) -> bool {
        if let Some(child_state) = &self.child {
            if child_state
                .lock()
                .expect("etcd child state is not poisoned")
                .is_some()
            {
                return true;
            }
        }
        let (Some(binary), Some(data_dir), Some(peer_endpoint)) =
            (&self.binary, &self.data_dir, &self.peer_endpoint)
        else {
            return false;
        };
        for _ in 0..RESTART_ATTEMPTS {
            let Ok(child) = Command::new(binary)
                .args([
                    "--name",
                    "clustodian-test",
                    "--data-dir",
                    data_dir.path().to_str().unwrap_or_default(),
                    "--listen-client-urls",
                    &self.endpoint,
                    "--advertise-client-urls",
                    &self.endpoint,
                    "--listen-peer-urls",
                    peer_endpoint,
                    "--initial-advertise-peer-urls",
                    peer_endpoint,
                    "--initial-cluster",
                    &format!("clustodian-test={peer_endpoint}"),
                    "--initial-cluster-state",
                    "new",
                    "--initial-cluster-token",
                    "clustodian-tests",
                    "--log-level",
                    "error",
                ])
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
            else {
                tokio::time::sleep(READINESS_RETRY).await;
                continue;
            };
            if let Some(child_state) = &self.child {
                *child_state
                    .lock()
                    .expect("etcd child state is not poisoned") = Some(child);
            }
            for _ in 0..READINESS_ATTEMPTS {
                let child_exited = self.child.as_ref().is_some_and(|child_state| {
                    let mut child = child_state
                        .lock()
                        .expect("etcd child state is not poisoned");
                    child
                        .as_mut()
                        .and_then(|child| child.try_wait().ok().flatten())
                        .is_some()
                });
                if child_exited {
                    break;
                }
                if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
                    endpoint: self.endpoint.clone(),
                    prefix: self.prefix.clone(),
                    cluster: String::from("test"),
                })
                .await
                {
                    if backend
                        .get_metadata("restart-readiness-probe")
                        .await
                        .is_ok()
                    {
                        return true;
                    }
                }
                tokio::time::sleep(READINESS_RETRY).await;
            }
            if let Some(child_state) = &self.child {
                let Some(mut child) = child_state
                    .lock()
                    .expect("etcd child state is not poisoned")
                    .take()
                else {
                    continue;
                };
                let _ = child.kill();
                let _ = child.wait();
            }
            tokio::time::sleep(READINESS_RETRY).await;
        }
        false
    }

    #[allow(dead_code)]
    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    #[allow(dead_code)] // Namespace cleanup is used by the randomized suite.
    pub(crate) async fn cleanup_namespace(&self, prefix: &str) -> Result<(), String> {
        let client = etcd_client::Client::connect([self.endpoint.as_str()], None)
            .await
            .map_err(|error| error.to_string())?;
        client
            .kv_client()
            .delete(
                format!("{prefix}/"),
                Some(etcd_client::DeleteOptions::new().with_prefix()),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

impl Drop for EtcdFixture {
    fn drop(&mut self) {
        if let Some(child_state) = &self.child {
            if let Some(mut child) = child_state
                .lock()
                .expect("etcd child state is not poisoned")
                .take()
            {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if let Some(shared) = &self._shared {
            shared.in_use.store(false, Ordering::Release);
        }
        let _ = self.data_dir.take();
    }
}

fn shared_process() -> Option<Arc<SharedEtcd>> {
    let registry = SHARED_PROCESSES.get_or_init(|| Mutex::new(Vec::new()));
    loop {
        let mut current = registry.lock().ok()?;
        current.retain(|process| {
            let Some(process) = process.upgrade() else {
                return false;
            };
            process.is_alive()
        });
        for process in &current[..] {
            if let Some(process) = process.upgrade() {
                if !process.is_alive() {
                    continue;
                }
                if process
                    .in_use
                    .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    return Some(process);
                }
            }
        }
        if current.len() < LOCAL_FIXTURE_LIMIT {
            let process = spawn_shared_process()?;
            process.in_use.store(true, Ordering::Release);
            current.push(Arc::downgrade(&process));
            return Some(process);
        }
        drop(current);
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn spawn_shared_process() -> Option<Arc<SharedEtcd>> {
    let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| String::from("etcd"));
    let client_port = free_port()?;
    let peer_port = free_port()?;
    let endpoint = format!("http://127.0.0.1:{client_port}");
    let peer_endpoint = format!("http://127.0.0.1:{peer_port}");
    let data_dir = tempdir().ok()?;
    let child = Command::new(&binary)
        .args([
            "--name",
            "clustodian-shared-test",
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
            &format!("clustodian-shared-test={peer_endpoint}"),
            "--initial-cluster-state",
            "new",
            "--initial-cluster-token",
            "clustodian-shared-tests",
            "--log-level",
            "error",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .ok()?;
    Some(Arc::new(SharedEtcd {
        child: Mutex::new(Some(child)),
        _data_dir: data_dir,
        endpoint,
        in_use: AtomicBool::new(false),
    }))
}

fn free_port() -> Option<u16> {
    clustodian_test_support::allocate_port().ok()
}
