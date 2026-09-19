#![cfg(not(feature = "shuttle"))]

use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::ClusterObserver;
use clustodian_multi_zone_db::{address_map, partition_for_key, RESOURCE};
use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};

static RUN: AtomicU64 = AtomicU64::new(1);
fn port() -> u16 {
    clustodian_test_support::allocate_port().expect("allocate coordinated test port")
}
struct Etcd {
    child: Option<Child>,
    dir: Option<TempDir>,
    endpoint: String,
}
impl Etcd {
    async fn start() -> Self {
        if let Ok(e) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Self {
                child: None,
                dir: None,
                endpoint: e,
            };
        }
        let cp = port();
        let pp = port();
        let endpoint = format!("http://127.0.0.1:{cp}");
        let peer = format!("http://127.0.0.1:{pp}");
        let dir = tempdir().unwrap();
        let bin = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| "etcd".into());
        let child = Command::new(bin)
            .args([
                "--name",
                "zonedb-test",
                "--data-dir",
                dir.path().to_str().unwrap(),
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer,
                "--initial-advertise-peer-urls",
                &peer,
                "--initial-cluster",
                &format!("zonedb-test={peer}"),
                "--initial-cluster-state",
                "new",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("etcd or CLUSTODIAN_ETCD_TEST_ENDPOINT is required");
        Self {
            child: Some(child),
            dir: Some(dir),
            endpoint,
        }
    }
    async fn backend(&mut self, prefix: &str) -> EtcdCoordination {
        for _ in 0..160 {
            if let Some(child) = self.child.as_mut() {
                if let Some(error) =
                    clustodian_test_support::child_exit_message(child, "multi-zone-db etcd")
                {
                    panic!("{error}");
                }
            }
            if let Ok(b) = EtcdCoordination::connect(EtcdCoordinationConfig {
                endpoint: self.endpoint.clone(),
                prefix: prefix.into(),
                cluster: "zonedb-integration".into(),
            })
            .await
            {
                if b.get_metadata("probe").await.is_ok() {
                    return b;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await
        }
        panic!("etcd unavailable")
    }
}
impl Drop for Etcd {
    fn drop(&mut self) {
        if let Some(c) = &mut self.child {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = self.dir.take();
    }
}
struct Procs {
    controller: Child,
    nodes: BTreeMap<String, Child>,
}
impl Drop for Procs {
    fn drop(&mut self) {
        let _ = self.controller.kill();
        let _ = self.controller.wait();
        for c in self.nodes.values_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
fn spawn(bin: &str, args: &[&str], envs: &[(&str, &str)]) -> Child {
    let mut c = Command::new(bin);
    c.args(args)
        .envs(envs.iter().copied())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    c.spawn().unwrap()
}
fn settled(s: &clustodian::observe::ClusterSnapshot) -> bool {
    let Some(resource) = s.external_view.get(RESOURCE).and_then(|v| v.as_object()) else {
        return false;
    };
    resource.len() == 6
        && !s.pending_transitions.iter().any(|_| true)
        && resource.values().all(|p| {
            let Some(p) = p.as_object() else { return false };
            p.len() == 3
                && p.values().filter(|v| v.as_str() == Some("LEADER")).count() == 1
                && p.values().filter(|v| v.as_str() == Some("STANDBY")).count() == 2
        })
}
fn degraded_after_zone_b_failure(s: &clustodian::observe::ClusterSnapshot) -> bool {
    let Some(resource) = s.external_view.get(RESOURCE).and_then(|v| v.as_object()) else {
        return false;
    };
    !s.live_instances.contains_key("db-b")
        && resource.len() == 6
        && !s.pending_transitions.iter().any(|_| true)
        && resource.values().all(|p| {
            let Some(p) = p.as_object() else { return false };
            !p.contains_key("db-b")
                && p.len() == 2
                && p.values().filter(|v| v.as_str() == Some("LEADER")).count() == 1
                && p.values().filter(|v| v.as_str() == Some("STANDBY")).count() == 1
        })
}
fn leader_for(s: &clustodian::observe::ClusterSnapshot, partition: &str) -> Option<String> {
    s.external_view[RESOURCE][partition]
        .as_object()?
        .iter()
        .find(|(_, v)| v.as_str() == Some("LEADER"))
        .map(|(i, _)| i.clone())
}
fn key_for_partition(partition: &str) -> String {
    (0..10_000)
        .map(|i| format!("customer-{i}=;"))
        .find(|key| partition_for_key(key) == partition)
        .unwrap_or_else(|| panic!("no test key found for {partition}"))
}
async fn wait(
    observer: &ClusterObserver,
    predicate: impl Fn(&clustodian::observe::ClusterSnapshot) -> bool,
) -> clustodian::observe::ClusterSnapshot {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let s = observer.snapshot().await.unwrap();
        if predicate(&s) {
            return s;
        }
        assert!(
            Instant::now() < until,
            "timed out waiting for cluster convergence: {:?}",
            s.external_view
        );
        tokio::time::sleep(Duration::from_millis(100)).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_zone_db_survives_zone_failure_and_heals() {
    let mut etcd = Etcd::start().await;
    let prefix = format!(
        "zonedb-it-{}-{}",
        std::process::id(),
        RUN.fetch_add(1, Ordering::Relaxed)
    );
    let backend = etcd.backend(&prefix).await;
    let dir = std::env::var("CARGO_BIN_EXE_multi-zone-db").expect("cargo supplies example binary");
    let nodes = format!(
        "db-a=zone-a@127.0.0.1:{},db-b=zone-b@127.0.0.1:{},db-c=zone-c@127.0.0.1:{}",
        port(),
        port(),
        port()
    );
    let envs = [
        ("ZONEDB_ETCD_ENDPOINT", etcd.endpoint.as_str()),
        ("ZONEDB_PREFIX", prefix.as_str()),
        ("ZONEDB_CLUSTER", "zonedb-integration"),
        ("ZONEDB_NODES", nodes.as_str()),
    ];
    let admin = spawn(&dir, &["admin", "init", &nodes], &envs);
    let mut admin = admin;
    assert!(admin.wait().unwrap().success());
    let controller = spawn(&dir, &["controller"], &envs);
    let mut procs = Procs {
        controller,
        nodes: BTreeMap::new(),
    };
    for id in ["db-a", "db-b", "db-c"] {
        procs
            .nodes
            .insert(id.into(), spawn(&dir, &["node", id], &envs));
    }
    let observer = ClusterObserver::new(backend.clone());
    let initial = wait(&observer, settled).await;
    let zone_for = |instance: &str| match instance {
        "db-a" => "zone-a",
        "db-b" => "zone-b",
        "db-c" => "zone-c",
        _ => panic!("unexpected instance {instance}"),
    };
    for partition in initial.external_view[RESOURCE]
        .as_object()
        .unwrap()
        .values()
    {
        let map = partition.as_object().unwrap();
        assert_eq!(
            map.values()
                .filter(|v| v.as_str() == Some("LEADER"))
                .count(),
            1
        );
        assert_eq!(
            map.values()
                .filter(|v| v.as_str() == Some("STANDBY"))
                .count(),
            2
        );
        let zones: std::collections::BTreeSet<_> = map.keys().map(|i| zone_for(i)).collect();
        assert_eq!(zones, ["zone-a", "zone-b", "zone-c"].into_iter().collect());
    }
    let promoted_partition = initial.external_view[RESOURCE]
        .as_object()
        .unwrap()
        .iter()
        .find(|(_, members)| members.as_object().unwrap().contains_key("db-b"))
        .map(|(partition, _)| partition.clone())
        .expect("at least one partition should initially have a zone-b replica");
    let promoted_key = key_for_partition(&promoted_partition);
    let initial_addresses = address_map(&clustodian_multi_zone_db::parse_nodes(&nodes).unwrap());
    let mut put_succeeded = false;
    for _ in 0..100 {
        let current = observer.snapshot().await.unwrap();
        if let Some(leader) = leader_for(&current, &promoted_partition) {
            if clustodian_multi_zone_db::request_at(
                &initial_addresses[&leader],
                &format!("PUT {promoted_key} alice"),
            )
            .unwrap()
                == "OK"
            {
                put_succeeded = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(put_succeeded, "the elected leader did not become ready");
    let mut b = procs.nodes.remove("db-b").unwrap();
    let _ = b.kill();
    let _ = b.wait();
    let after = wait(&observer, degraded_after_zone_b_failure).await;
    let leader = leader_for(&after, &promoted_partition).unwrap();
    assert_ne!(leader, "db-b");
    let address = address_map(&clustodian_multi_zone_db::parse_nodes(&nodes).unwrap());
    assert_eq!(
        clustodian_multi_zone_db::request_at(&address[&leader], &format!("GET {promoted_key}"))
            .unwrap(),
        "VALUE alice"
    );
    let replacement_port = port();
    let healed_nodes = format!("{nodes},db-d=zone-b@127.0.0.1:{replacement_port}");
    let healed_envs = [
        ("ZONEDB_ETCD_ENDPOINT", etcd.endpoint.as_str()),
        ("ZONEDB_PREFIX", prefix.as_str()),
        ("ZONEDB_CLUSTER", "zonedb-integration"),
        ("ZONEDB_NODES", healed_nodes.as_str()),
    ];
    let replacement = format!("db-d=zone-b@127.0.0.1:{replacement_port}");
    let mut add = spawn(&dir, &["admin", "add", &replacement], &healed_envs);
    let _ = add.wait();
    procs
        .nodes
        .insert("db-d".into(), spawn(&dir, &["node", "db-d"], &healed_envs));
    let observer = ClusterObserver::new(backend);
    let healed = wait(&observer, settled).await;
    assert_eq!(healed.external_view[RESOURCE].as_object().unwrap().len(), 6);
    for p in healed.external_view[RESOURCE].as_object().unwrap().values() {
        let map = p.as_object().unwrap();
        assert_eq!(map.len(), 3);
        assert!(!map.contains_key("db-b"));
        assert!(map.contains_key("db-d"));
        assert_eq!(
            map.values()
                .filter(|v| v.as_str() == Some("LEADER"))
                .count(),
            1
        );
        let zones: std::collections::BTreeSet<_> = map
            .keys()
            .map(|i| match i.as_str() {
                "db-a" => "zone-a",
                "db-b" => "zone-b",
                "db-c" => "zone-c",
                "db-d" => "zone-b",
                _ => panic!("unexpected instance"),
            })
            .collect();
        assert_eq!(zones.len(), 3);
    }
    assert_eq!(
        clustodian_multi_zone_db::request_at(
            &address_map(&clustodian_multi_zone_db::parse_nodes(&healed_nodes).unwrap())["db-d"],
            &format!("GET {promoted_key}"),
        )
        .unwrap(),
        "VALUE alice"
    );
}
