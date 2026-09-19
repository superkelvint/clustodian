use clustodian::admin::{ClusterAdmin, InstanceSpec, PlacementSpec, ResourceSpec};
use clustodian::coordination::etcd::{CoordinationError, EtcdCoordination, EtcdCoordinationConfig};
use clustodian::observe::ClusterObserver;
use clustodian::runtime::{ControllerRuntime, ControllerRuntimeConfig};
use clustodian::{Cluster, ClusterConfig};
use clustodian_ha_controllers::{ParticipantState, PARTITIONS, RESOURCE};
use std::env;
use std::error::Error;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    match env::args().nth(1).as_deref() {
        Some("setup") => setup().await,
        Some("controller") => controller().await,
        Some("participant") => participant().await,
        Some("status") => status().await,
        _ => Err("usage: ha-controllers <setup|controller|participant|status>".into()),
    }
}

fn endpoint() -> String {
    env::var("HA_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into())
}
fn prefix() -> String {
    env::var("HA_PREFIX")
        .unwrap_or_else(|_| "clustodian-ha-demo".into())
        .trim_end_matches('/')
        .into()
}
fn cluster() -> String {
    env::var("HA_CLUSTER").unwrap_or_else(|_| "ha-control-plane".into())
}
fn required(name: &str) -> Result<String, Box<dyn Error>> {
    Ok(env::var(name).map_err(|_| format!("{name} is required"))?)
}
fn number(name: &str, default: u64) -> Result<u64, Box<dyn Error>> {
    Ok(env::var(name)
        .unwrap_or_else(|_| default.to_string())
        .parse()?)
}
async fn backend() -> Result<EtcdCoordination, Box<dyn Error>> {
    Ok(EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint: endpoint(),
        prefix: prefix(),
        cluster: cluster(),
    })
    .await?)
}

async fn facade_backend() -> Result<Cluster, Box<dyn Error>> {
    Ok(Cluster::connect(
        ClusterConfig::new(cluster())
            .etcd_endpoints([endpoint()])
            .namespace(prefix()),
    )
    .await?)
}

async fn setup() -> Result<(), Box<dyn Error>> {
    let backend = backend().await?;
    let admin = ClusterAdmin::new(backend);
    admin.ensure_cluster(&cluster()).await?;
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
            .await?;
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
        .await?;
    println!("configured four control-work partitions and four participants");
    Ok(())
}

async fn controller() -> Result<(), Box<dyn Error>> {
    let coordination = backend().await?;
    let mut runtime = ControllerRuntime::new(
        coordination,
        ControllerRuntimeConfig {
            cluster: cluster(),
            controller_id: env::var("HA_CONTROLLER_ID").unwrap_or_else(|_| "controller-1".into()),
            lease_ttl_ms: number("HA_CONTROLLER_LEASE_TTL_MS", 1000)?,
        },
    )
    .await?;
    if let (Some(ready), Some(release), Some(outcome)) = (
        env::var_os("HA_STALE_CONTROLLER_READY_FILE"),
        env::var_os("HA_STALE_CONTROLLER_RELEASE_FILE"),
        env::var_os("HA_STALE_CONTROLLER_OUTCOME_FILE"),
    ) {
        let ready = ready.into_string().map_err(|_| "ready path is not utf8")?;
        let release = release
            .into_string()
            .map_err(|_| "release path is not utf8")?;
        let outcome = outcome
            .into_string()
            .map_err(|_| "outcome path is not utf8")?;
        let stale_backend = backend().await?;
        runtime = runtime.on_authority_ready(move |authority| {
            tokio::spawn(async move {
                let marker_key = "demo/controller-1-before-failover";
                if let Err(error) = stale_backend
                    .put_controller_owned(&authority, marker_key, "committed-before-failover")
                    .await
                {
                    let _ = std::fs::write(&outcome, format!("initial-write-error: {error}"));
                    return;
                }
                if std::fs::write(&ready, b"authority-ready").is_err() {
                    return;
                }
                while !std::path::Path::new(&release).exists() {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                match stale_backend
                    .put_controller_owned(
                        &authority,
                        "demo/controller-1-after-failover",
                        "must-not-commit",
                    )
                    .await
                {
                    Err(CoordinationError::StaleController) => {
                        let _ = std::fs::write(&outcome, b"rejected: stale controller authority");
                    }
                    Ok(_) => {
                        let _ = std::fs::write(&outcome, b"ERROR: stale write committed");
                    }
                    Err(error) => {
                        let _ = std::fs::write(&outcome, format!("unexpected-error: {error}"));
                    }
                }
            });
        });
    }
    runtime
        .on_ready(|| {
            println!("controller active");
            Ok::<(), std::io::Error>(())
        })
        .run()
        .await
        .map_err(Into::into)
}

async fn participant() -> Result<(), Box<dyn Error>> {
    let name = required("HA_INSTANCE_ID")?;
    let lease_ttl = Duration::from_millis(number("HA_PARTICIPANT_LEASE_TTL_MS", 1500)?);
    let cluster = facade_backend().await?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    cluster
        .participant(name)
        .resource(RESOURCE, ParticipantState::default())
        .lease_ttl(lease_ttl)
        .on_ready(|| {
            println!("participant active");
            Ok::<(), std::io::Error>(())
        })
        .run_until(async move {
            let _ = terminate.recv().await;
        })
        .await?;
    Ok(())
}

async fn status() -> Result<(), Box<dyn Error>> {
    let snapshot = ClusterObserver::new(backend().await?).snapshot().await?;
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}
