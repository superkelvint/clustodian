#![cfg(not(feature = "shuttle"))]

use clustodian::admin::{
    ClusterAdmin, CrushTopologySpec, InstanceSpec, PlacementSpec, ResourceSpec,
};
use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::{ClusterObserver, ClusterSnapshot};
use clustodian_object_store::{partition_for_object, request_at, PARTITION_COUNT, RESOURCE};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tempfile::{tempdir, TempDir};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct Etcd {
    child: Option<Child>,
    data: Option<TempDir>,
    endpoint: String,
    prefix: String,
}
impl Etcd {
    async fn start() -> Option<Self> {
        let prefix = format!(
            "object-store-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        );
        if let Ok(endpoint) = std::env::var("CLUSTODIAN_ETCD_TEST_ENDPOINT") {
            return Some(Self {
                child: None,
                data: None,
                endpoint,
                prefix,
            });
        }
        let client = free_port()?;
        let peer = free_port()?;
        let endpoint = format!("http://127.0.0.1:{client}");
        let peer_endpoint = format!("http://127.0.0.1:{peer}");
        let data = tempdir().ok()?;
        let binary = std::env::var("CLUSTODIAN_ETCD_BIN").unwrap_or_else(|_| "etcd".to_owned());
        let child = Command::new(binary)
            .args([
                "--name",
                "object-store-test",
                "--data-dir",
                data.path().to_str()?,
                "--listen-client-urls",
                &endpoint,
                "--advertise-client-urls",
                &endpoint,
                "--listen-peer-urls",
                &peer_endpoint,
                "--initial-advertise-peer-urls",
                &peer_endpoint,
                "--initial-cluster",
                &format!("object-store-test={peer_endpoint}"),
                "--initial-cluster-state",
                "new",
                "--initial-cluster-token",
                "object-store-tests",
                "--log-level",
                "error",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .ok()?;
        let mut fixture = Self {
            child: Some(child),
            data: Some(data),
            endpoint,
            prefix,
        };
        for _ in 0..160 {
            if let Some(child) = fixture.child.as_mut() {
                if let Some(error) =
                    clustodian_test_support::child_exit_message(child, "object-store etcd")
                {
                    eprintln!("{error}");
                    return None;
                }
            }
            if let Ok(backend) = fixture.backend().await {
                if backend.get_metadata("ready").await.is_ok() {
                    return Some(fixture);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }
    async fn backend(&self) -> Result<EtcdCoordination, Box<dyn std::error::Error>> {
        Ok(EtcdCoordination::connect(EtcdCoordinationConfig {
            endpoint: self.endpoint.clone(),
            prefix: self.prefix.clone(),
            cluster: RESOURCE.to_owned(),
        })
        .await?)
    }
}
impl Drop for Etcd {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.data.take();
    }
}

struct Proc {
    child: Option<Child>,
    name: String,
    address: String,
}
impl Proc {
    fn kill(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }
}
impl Drop for Proc {
    fn drop(&mut self) {
        self.kill();
    }
}

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_ranges_are_zone_diverse_and_recover_after_zone_failure() {
    let Some(etcd) = Etcd::start().await else {
        panic!("etcd is required; install etcd or set CLUSTODIAN_ETCD_TEST_ENDPOINT");
    };
    let backend = etcd.backend().await.unwrap();
    let admin = ClusterAdmin::new(backend.clone());
    admin.ensure_cluster(RESOURCE).await.unwrap();
    let names = ["object-a", "object-b", "object-c", "object-d"];
    let zones = ["zone-a", "zone-b", "zone-c", "zone-a"];
    let zone_by_name = names
        .iter()
        .zip(zones)
        .map(|(name, zone)| ((*name).to_owned(), zone.to_owned()))
        .collect::<BTreeMap<_, _>>();
    for (name, zone) in names.iter().zip(zones) {
        admin
            .put_instance(InstanceSpec {
                instance_id: (*name).to_owned(),
                zone: zone.to_owned(),
            })
            .await
            .unwrap();
    }
    admin
        .put_resource(ResourceSpec {
            name: RESOURCE.to_owned(),
            partitions: PARTITION_COUNT,
            replicas: 3,
            state_model: "LeaderStandby".to_owned(),
            placement: PlacementSpec::CrushWithTopology {
                topology: CrushTopologySpec::new("/zone/instance", "zone", "instance"),
            },
        })
        .await
        .unwrap();
    let binary = std::env::var("CARGO_BIN_EXE_object-store")
        .expect("Cargo exposes object-store to integration tests");
    let common = [
        ("OBJECT_STORE_ETCD_ENDPOINT", etcd.endpoint.as_str()),
        ("OBJECT_STORE_PREFIX", etcd.prefix.as_str()),
        ("OBJECT_STORE_CLUSTER", RESOURCE),
    ];
    let ports = [
        free_port().unwrap(),
        free_port().unwrap(),
        free_port().unwrap(),
        free_port().unwrap(),
    ];
    let addresses = format!(
        "object-a=127.0.0.1:{},object-b=127.0.0.1:{},object-c=127.0.0.1:{},object-d=127.0.0.1:{}",
        ports[0], ports[1], ports[2], ports[3]
    );
    let mut nodes = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let address = format!("127.0.0.1:{}", ports[index]);
        let mut command = Command::new(&binary);
        command.arg("node");
        for (key, value) in common {
            command.env(key, value);
        }
        command
            .env("OBJECT_STORE_INSTANCE_ID", name)
            .env("OBJECT_STORE_LISTEN", &address)
            .env("OBJECT_STORE_PEERS", &addresses)
            .env("OBJECT_STORE_PARTICIPANT_LEASE_TTL_MS", "1500")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        nodes.push(Proc {
            child: Some(command.spawn().unwrap()),
            name: (*name).to_owned(),
            address,
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
        .env("OBJECT_STORE_CONTROLLER_ID", "controller-1")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let _controller = ChildGuard(Some(controller.spawn().unwrap()));
    let observer = ClusterObserver::new(backend.clone());
    let initial = wait_for(&observer, |snapshot| settled(snapshot, 4, 3)).await;
    assert_zone_diversity(&initial.external_view, &zone_by_name);
    let object = "bucket/photo-001";
    let partition = partition_for_object(object);
    let leader = leader_for(&initial.external_view, &partition);
    let leader_address = nodes
        .iter()
        .find(|node| node.name == leader)
        .unwrap()
        .address
        .clone();
    assert!(
        request_at(&leader_address, &format!("PUT {object} payload"))
            .unwrap()
            .starts_with("OK")
    );
    for node in replicas_for(&initial.external_view, &partition) {
        let node = nodes
            .iter()
            .find(|candidate| candidate.name == node)
            .unwrap();
        assert_eq!(
            request_at(&node.address, &format!("GET {object}")).as_deref(),
            Some("VALUE payload")
        );
    }

    let dead_name = replicas_for(&initial.external_view, &partition)
        .into_iter()
        .find(|name| zone_by_name[name] == "zone-a")
        .expect("object partition has a zone-a replica");
    nodes
        .iter_mut()
        .find(|node| node.name == dead_name)
        .unwrap()
        .kill();
    let replaced = wait_for(&observer, |snapshot| {
        settled(snapshot, 3, 3)
            && !snapshot.live_instances.contains_key(&dead_name)
            && !snapshot.external_view.to_string().contains(&dead_name)
    })
    .await;
    assert_zone_diversity(&replaced.external_view, &zone_by_name);
    let replacement = replicas_for(&replaced.external_view, &partition)
        .into_iter()
        .find(|name| zone_by_name[name] == "zone-a")
        .expect("object partition was re-placed into the surviving zone-a node");
    assert_ne!(replacement, dead_name);
    let replacement_address = nodes
        .iter()
        .find(|node| node.name == replacement)
        .unwrap()
        .address
        .clone();
    assert_eq!(
        request_at(&replacement_address, &format!("GET {object}")).as_deref(),
        Some("VALUE payload")
    );

    let zone_a_survivor = replacement.clone();
    nodes
        .iter_mut()
        .find(|node| node.name == zone_a_survivor)
        .unwrap()
        .kill();
    let degraded = wait_for(&observer, |snapshot| {
        settled(snapshot, 2, 2)
            && !snapshot
                .live_instances
                .keys()
                .any(|name| zone_by_name[name] == "zone-a")
            && snapshot.external_view[RESOURCE]
                .as_object()
                .unwrap()
                .values()
                .all(|partition| {
                    partition
                        .as_object()
                        .unwrap()
                        .keys()
                        .all(|name| zone_by_name[name] != "zone-a")
                })
    })
    .await;
    let promoted = leader_for(&degraded.external_view, &partition);
    let promoted_address = nodes
        .iter()
        .find(|node| node.name == promoted)
        .unwrap()
        .address
        .clone();
    assert_eq!(
        request_at(&promoted_address, &format!("GET {object}")).as_deref(),
        Some("VALUE payload")
    );
    for rejoining in [dead_name, zone_a_survivor] {
        let index = names.iter().position(|name| *name == rejoining).unwrap();
        let address = format!("127.0.0.1:{}", ports[index]);
        let mut command = Command::new(&binary);
        command.arg("node");
        for (key, value) in common {
            command.env(key, value);
        }
        command
            .env("OBJECT_STORE_INSTANCE_ID", &rejoining)
            .env("OBJECT_STORE_LISTEN", &address)
            .env("OBJECT_STORE_PEERS", &addresses)
            .env("OBJECT_STORE_PARTICIPANT_LEASE_TTL_MS", "1500")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        nodes[index].child = Some(command.spawn().unwrap());
    }
    let healed = wait_for(&observer, |snapshot| settled(snapshot, 4, 3)).await;
    assert_zone_diversity(&healed.external_view, &zone_by_name);
    for node in replicas_for(&healed.external_view, &partition) {
        let address = nodes
            .iter()
            .find(|candidate| candidate.name == node)
            .unwrap()
            .address
            .clone();
        assert_eq!(
            request_at(&address, &format!("GET {object}")).as_deref(),
            Some("VALUE payload")
        );
    }
}

fn settled(snapshot: &ClusterSnapshot, live: usize, replicas: usize) -> bool {
    snapshot.live_instances.len() == live
        && snapshot.controllers.active.len() == 1
        && snapshot
            .processed_revision
            .is_some_and(|processed| processed >= snapshot.authoritative_revision)
        && snapshot.pending_transitions.is_empty()
        && snapshot.external_view[RESOURCE]
            .as_object()
            .map(|partitions| {
                partitions.len() == PARTITION_COUNT
                    && partitions.values().all(|partition| {
                        partition
                            .as_object()
                            .map(|entries| {
                                entries.len() == replicas
                                    && entries.values().filter(|state| *state == "LEADER").count()
                                        == 1
                            })
                            .unwrap_or(false)
                    })
            })
            .unwrap_or(false)
}
fn assert_zone_diversity(view: &Value, zone_by_name: &BTreeMap<String, String>) {
    for partition in view[RESOURCE].as_object().unwrap().values() {
        let zones = partition
            .as_object()
            .unwrap()
            .keys()
            .map(|name| zone_by_name[name].clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            zones,
            BTreeSet::from_iter(["zone-a", "zone-b", "zone-c"].map(String::from))
        );
    }
}
fn replicas_for(view: &Value, partition: &str) -> Vec<String> {
    view[RESOURCE][partition]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect()
}
fn leader_for(view: &Value, partition: &str) -> String {
    view[RESOURCE][partition]
        .as_object()
        .unwrap()
        .iter()
        .find(|(_, state)| *state == "LEADER")
        .unwrap()
        .0
        .clone()
}
async fn wait_for<F>(observer: &ClusterObserver, predicate: F) -> ClusterSnapshot
where
    F: Fn(&ClusterSnapshot) -> bool,
{
    let mut last = None;
    for _ in 0..300 {
        let snapshot = observer.snapshot().await.unwrap();
        if predicate(&snapshot) {
            return snapshot;
        }
        last = Some(snapshot);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for convergence: {last:#?}");
}
async fn wait_for_data_plane(address: &str) {
    for _ in 0..120 {
        if request_at(address, "PING").as_deref() == Some("PONG") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for object-store data plane at {address}");
}
fn free_port() -> Option<u16> {
    clustodian_test_support::allocate_port().ok()
}
