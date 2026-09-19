use crate::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tempfile::{tempdir, TempDir};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(1);
static FIXTURE: OnceLock<Option<Arc<Mutex<EtcdFixture>>>> = OnceLock::new();

struct EtcdFixture {
    child: Option<Child>,
    data_dir: Option<TempDir>,
    endpoint: String,
    prefix: String,
}

impl EtcdFixture {
    fn new() -> Option<Self> {
        let prefix = format!(
            "clustodian-library-tests-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
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
        let child = spawn_etcd(
            &binary,
            data_dir.path().to_str()?,
            &endpoint,
            &peer_endpoint,
        )?;
        Some(Self {
            child: Some(child),
            data_dir: Some(data_dir),
            endpoint,
            prefix,
        })
    }

    fn connection(&self, name: &str) -> (String, String) {
        (self.endpoint.clone(), format!("{}/{}", self.prefix, name))
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

pub(crate) async fn connect(name: &str) -> Option<EtcdCoordination> {
    let fixture = fixture()?;
    let (endpoint, prefix) = fixture.lock().ok()?.connection(name);
    connect_at(&endpoint, &prefix).await
}

fn fixture() -> Option<Arc<Mutex<EtcdFixture>>> {
    FIXTURE
        .get_or_init(|| EtcdFixture::new().map(|fixture| Arc::new(Mutex::new(fixture))))
        .as_ref()
        .cloned()
}

async fn connect_at(endpoint: &str, prefix: &str) -> Option<EtcdCoordination> {
    for _ in 0..100 {
        if let Ok(backend) = EtcdCoordination::connect(EtcdCoordinationConfig {
            endpoint: endpoint.to_owned(),
            prefix: prefix.to_owned(),
            cluster: String::from("test"),
        })
        .await
        {
            if backend
                .get_metadata("library-readiness-probe")
                .await
                .is_ok()
            {
                return Some(backend);
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    None
}

fn spawn_etcd(binary: &str, data_dir: &str, endpoint: &str, peer_endpoint: &str) -> Option<Child> {
    Command::new(binary)
        .args([
            "--name",
            "clustodian-library-test",
            "--data-dir",
            data_dir,
            "--listen-client-urls",
            endpoint,
            "--advertise-client-urls",
            endpoint,
            "--listen-peer-urls",
            peer_endpoint,
            "--initial-advertise-peer-urls",
            peer_endpoint,
            "--initial-cluster",
            &format!("clustodian-library-test={peer_endpoint}"),
            "--initial-cluster-state",
            "new",
            "--initial-cluster-token",
            "clustodian-library-tests",
            "--log-level",
            "error",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

fn free_port() -> Option<u16> {
    clustodian_test_support::allocate_port().ok()
}
