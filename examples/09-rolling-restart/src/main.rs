use clustodian::admin::{ClusterAdmin, InstanceSpec, PlacementSpec, ResourceSpec};
use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::model::InstanceId;
use clustodian::observe::ClusterObserver;
use clustodian::runtime::{ControllerRuntime, ControllerRuntimeConfig};
use clustodian::transition::TransitionMessage;
use clustodian::{Cluster, ClusterConfig};
use clustodian_rolling_restart::{
    preference_lists, FencedCallback, PARTICIPANTS, PARTICIPANT_LEASE_TTL, PARTITION_COUNT,
    REPLICA_COUNT, RESOURCE,
};
use std::env;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:23797";
const DEFAULT_CLUSTER: &str = "rolling-restart-demo";
const DEFAULT_PREFIX: &str = "clustodian-rolling-restart-demo";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("setup") => setup().await?,
        Some("controller") => {
            run_controller(args.next().ok_or("controller requires an id")?).await?
        }
        Some("participant") => {
            run_participant(args.next().ok_or("participant requires an id")?).await?
        }
        Some("observe") => observe().await?,
        Some("inject-stale") => {
            let instance = args.next().ok_or("inject-stale requires an instance")?;
            let session = args
                .next()
                .ok_or("inject-stale requires a session")?
                .parse()?;
            let message = args
                .next()
                .unwrap_or_else(|| "demo-stale-message".to_owned());
            inject_stale(&instance, session, &message).await?;
        }
        Some("session") => {
            let instance = args.next().ok_or("session requires an instance")?;
            let backend = backend().await?;
            let id = InstanceId::new(instance)?;
            println!(
                "{}",
                backend.live_session(&id).await?.map_or_else(
                    || "none".to_owned(),
                    |session| session.wire_value().to_string()
                )
            );
        }
        Some(command) => return Err(format!("unsupported command: {command}").into()),
        None => {
            return Err(
                "expected setup, controller, participant, observe, inject-stale, or session".into(),
            )
        }
    }
    Ok(())
}

fn endpoint() -> String {
    env::var("ROLLING_ETCD_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned())
}

fn cluster() -> String {
    env::var("ROLLING_CLUSTER").unwrap_or_else(|_| DEFAULT_CLUSTER.to_owned())
}

fn prefix() -> String {
    env::var("ROLLING_PREFIX").unwrap_or_else(|_| DEFAULT_PREFIX.to_owned())
}

async fn backend() -> Result<EtcdCoordination, Box<dyn std::error::Error>> {
    Ok(EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint: endpoint(),
        prefix: prefix(),
        cluster: cluster(),
    })
    .await?)
}

async fn facade_backend() -> Result<Cluster, Box<dyn std::error::Error>> {
    Ok(Cluster::connect(
        ClusterConfig::new(cluster())
            .etcd_endpoints([endpoint()])
            .namespace(prefix()),
    )
    .await?)
}

async fn setup() -> Result<(), Box<dyn std::error::Error>> {
    let admin = ClusterAdmin::new(backend().await?);
    admin.ensure_cluster(&cluster()).await?;
    for instance in PARTICIPANTS {
        admin
            .put_instance(InstanceSpec {
                instance_id: instance.to_owned(),
                zone: format!("zone-{instance}"),
            })
            .await?;
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
        .await?;
    println!(
        "configured {RESOURCE}: {PARTITION_COUNT} partitions, RF={REPLICA_COUNT}, fixed SEMI_AUTO placement"
    );
    Ok(())
}

async fn run_controller(id: String) -> Result<(), Box<dyn std::error::Error>> {
    ControllerRuntime::new(
        backend().await?,
        ControllerRuntimeConfig {
            cluster: cluster(),
            controller_id: id,
            lease_ttl_ms: 1_500,
        },
    )
    .await?
    .run()
    .await?;
    Ok(())
}

async fn run_participant(instance: String) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = facade_backend().await?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    cluster
        .participant(instance.clone())
        .resource(RESOURCE, FencedCallback::from_environment(instance.clone()))
        .lease_ttl(PARTICIPANT_LEASE_TTL)
        .on_ready(|| Ok::<(), std::io::Error>(()))
        .run_until(async move {
            let _ = terminate.recv().await;
        })
        .await?;
    println!("participant={instance} stopped gracefully");
    Ok(())
}

async fn observe() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        serde_json::to_string_pretty(&ClusterObserver::new(backend().await?).snapshot().await?)?
    );
    Ok(())
}

async fn inject_stale(
    instance: &str,
    session: u64,
    message_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let backend = backend().await?;
    let snapshot = ClusterObserver::new(backend.clone()).snapshot().await?;
    let partition = snapshot
        .active_current_state
        .get(instance)
        .and_then(|active| active.resources.get(RESOURCE))
        .and_then(|states| states.keys().next())
        .cloned()
        .unwrap_or_else(|| format!("{RESOURCE}_0"));
    let message = TransitionMessage {
        message_id: message_id.to_owned(),
        resource: RESOURCE.to_owned(),
        partition,
        instance: instance.to_owned(),
        target_session: session,
        from: "OFFLINE".to_owned(),
        to: "STANDBY".to_owned(),
        message_type: "STATE_TRANSITION".to_owned(),
    };
    backend.inject_pending_transition(&message).await?;
    println!(
        "injected stale message={} instance={} target_session={session}",
        message_id, instance
    );
    Ok(())
}
