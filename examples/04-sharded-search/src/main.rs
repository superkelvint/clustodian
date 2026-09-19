use clustodian::observe::Snapshot;
use clustodian::{Cluster, ClusterConfig, ClusterSpec, InstanceSpec, Placement, ResourceSpec};
use clustodian_sharded_search::{SearchNodeState, PARTITION_COUNT, REPLICA_COUNT, RESOURCE};
use std::env;
use std::error::Error;
use std::time::Duration;

const DEFAULT_CLUSTER: &str = "sharded-search";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let command = env::args().nth(1).ok_or(
        "usage: sharded-search <setup|add-node|remove-node|controller|participant|status|wait>",
    )?;
    match command.as_str() {
        "setup" => setup().await,
        "add-node" => add_node().await,
        "remove-node" => remove_node().await,
        "controller" => controller().await,
        "participant" => participant().await,
        "status" => status().await,
        "wait" => wait_for_convergence().await,
        other => Err(format!("unknown command: {other}").into()),
    }
}

async fn backend() -> Result<Cluster, Box<dyn Error>> {
    Ok(Cluster::connect(
        ClusterConfig::new(cluster())
            .etcd_endpoints([required("CLUSTODIAN_SEARCH_ETCD_ENDPOINT")?])
            .namespace(required("CLUSTODIAN_SEARCH_PREFIX")?),
    )
    .await?)
}

async fn setup() -> Result<(), Box<dyn Error>> {
    let cluster = backend().await?;
    cluster
        .admin()
        .apply(
            ClusterSpec::new()
                .instances(
                    initial_instances()
                        .into_iter()
                        .map(|instance| InstanceSpec::new(instance).zone("default")),
                )
                .resource(
                    ResourceSpec::leader_standby(RESOURCE)
                        .partitions(PARTITION_COUNT)
                        .replicas(REPLICA_COUNT)
                        .placement(Placement::Crush),
                ),
        )
        .await?;
    println!(
        "configured {RESOURCE}: {PARTITION_COUNT} partitions, RF={REPLICA_COUNT}, placement=CRUSH"
    );
    Ok(())
}

async fn add_node() -> Result<(), Box<dyn Error>> {
    let instance = env::args()
        .nth(2)
        .ok_or("usage: sharded-search add-node <id>")?;
    backend()
        .await?
        .admin()
        .apply(ClusterSpec::new().instances([InstanceSpec::new(instance.clone()).zone("default")]))
        .await?;
    println!("added configured search instance {instance}");
    Ok(())
}

async fn remove_node() -> Result<(), Box<dyn Error>> {
    let instance = env::args()
        .nth(2)
        .ok_or("usage: sharded-search remove-node <id>")?;
    backend().await?.admin().remove_instance(&instance).await?;
    println!("removed configured search instance {instance}");
    Ok(())
}

async fn controller() -> Result<(), Box<dyn Error>> {
    backend()
        .await?
        .controller(
            env::var("CLUSTODIAN_SEARCH_CONTROLLER_ID")
                .unwrap_or_else(|_| String::from("search-controller-1")),
        )
        .lease_ttl(Duration::from_millis(
            env::var("CLUSTODIAN_SEARCH_CONTROLLER_LEASE_TTL_MS")
                .unwrap_or_else(|_| String::from("1500"))
                .parse()?,
        ))
        .run_until_signal()
        .await
        .map_err(Into::into)
}

async fn participant() -> Result<(), Box<dyn Error>> {
    let instance_name = required("CLUSTODIAN_SEARCH_INSTANCE_ID")?;
    let cluster = backend().await?;
    let ready_file = env::var_os("CLUSTODIAN_SEARCH_READY_FILE");
    let state = SearchNodeState::default();
    let ttl = env::var("CLUSTODIAN_SEARCH_PARTICIPANT_LEASE_TTL_MS")
        .unwrap_or_else(|_| String::from("2500"))
        .parse::<u64>()?;
    cluster
        .participant(instance_name.clone())
        .resource(RESOURCE, state.clone())
        .lease_ttl(Duration::from_millis(ttl))
        .on_ready(move || {
            if let Some(path) = ready_file {
                std::fs::write(path, b"ready\n")?;
            }
            println!("search participant {instance_name} ready");
            Ok::<(), std::io::Error>(())
        })
        .run_until_signal()
        .await
        .map_err(Into::into)
}

async fn status() -> Result<(), Box<dyn Error>> {
    let snapshot = backend().await?.observer().snapshot().await?;
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}

async fn wait_for_convergence() -> Result<(), Box<dyn Error>> {
    let expected_live = env::args()
        .nth(2)
        .ok_or("usage: sharded-search wait <expected-live-instances>")?
        .parse::<usize>()?;
    let observer = backend().await?.observer();
    observer
        .wait_until(Duration::from_secs(30), |snapshot| {
            converged(snapshot, expected_live)
        })
        .await?;
    println!(
        "converged: {expected_live} live instances, {PARTITION_COUNT} partitions, RF={REPLICA_COUNT}"
    );
    Ok(())
}

fn converged(snapshot: &Snapshot, expected_live: usize) -> bool {
    if !snapshot.pending_transitions().is_empty()
        || snapshot
            .processed_revision()
            .map(|revision| revision.value())
            < Some(snapshot.authoritative_revision().value())
        || snapshot.instances().len() != expected_live
    {
        return false;
    }
    (0..PARTITION_COUNT).all(|index| {
        let partition = format!("{RESOURCE}_{index}");
        snapshot
            .routing()
            .leader(RESOURCE, &partition)
            .ok()
            .flatten()
            .is_some()
            && snapshot
                .routing()
                .replicas(RESOURCE, &partition)
                .is_ok_and(|replicas| replicas.len() == REPLICA_COUNT)
    })
}

fn initial_instances() -> Vec<String> {
    env::var("CLUSTODIAN_SEARCH_INITIAL_INSTANCES")
        .unwrap_or_else(|_| String::from("node-a,node-b,node-c"))
        .split(',')
        .filter(|name| !name.trim().is_empty())
        .map(|name| name.trim().to_owned())
        .collect()
}

fn cluster() -> String {
    env::var("CLUSTODIAN_SEARCH_CLUSTER").unwrap_or_else(|_| DEFAULT_CLUSTER.to_owned())
}

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    Ok(env::var(name).map_err(|_| format!("{name} is required"))?)
}
