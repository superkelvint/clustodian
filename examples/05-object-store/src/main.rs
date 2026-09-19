use clustodian::{
    Cluster, ClusterConfig, ClusterSpec, InstanceSpec, Placement, ResourceSpec, Topology,
};
use clustodian_object_store::{
    parse_addresses, parse_nodes, partition_for_object, request_at, required, serve, ObjectState,
    ObjectTransitionHandler, PARTITION_COUNT, REPLICATION_FACTOR, RESOURCE,
};
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match env::args().nth(1).as_deref() {
        Some("setup") => setup().await?,
        Some("controller") => controller().await?,
        Some("node") => node().await?,
        Some("status") => status().await?,
        Some("get") | Some("put") => client().await?,
        _ => return Err("usage: object-store <setup|controller|node|status|get|put>".into()),
    }
    Ok(())
}

async fn backend() -> Result<Cluster, Box<dyn std::error::Error>> {
    Ok(Cluster::connect(
        ClusterConfig::new(cluster())
            .etcd_endpoints([required("OBJECT_STORE_ETCD_ENDPOINT")?])
            .namespace(required("OBJECT_STORE_PREFIX")?),
    )
    .await?)
}

fn cluster() -> String {
    env::var("OBJECT_STORE_CLUSTER").unwrap_or_else(|_| RESOURCE.to_owned())
}

async fn setup() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = backend().await?;
    let nodes = parse_nodes(&required("OBJECT_STORE_NODES")?)?;
    if nodes.len() < REPLICATION_FACTOR
        || nodes
            .values()
            .map(|node| &node.zone)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            < REPLICATION_FACTOR
    {
        return Err("setup requires three instances in three distinct zones".into());
    }
    let mut instances = Vec::new();
    for (instance, node) in nodes {
        instances.push(InstanceSpec::new(instance).zone(node.zone));
    }
    cluster
        .admin()
        .apply(
            ClusterSpec::new().instances(instances).resource(
                ResourceSpec::leader_standby(RESOURCE)
                    .partitions(PARTITION_COUNT)
                    .replicas(REPLICATION_FACTOR)
                    .placement(Placement::crush().topology(Topology::zones())),
            ),
        )
        .await?;
    println!("configured {PARTITION_COUNT} object ranges at RF={REPLICATION_FACTOR} across zones");
    Ok(())
}

async fn controller() -> Result<(), Box<dyn std::error::Error>> {
    backend()
        .await?
        .controller(
            env::var("OBJECT_STORE_CONTROLLER_ID").unwrap_or_else(|_| "controller-1".to_owned()),
        )
        .lease_ttl(Duration::from_millis(
            env::var("OBJECT_STORE_CONTROLLER_LEASE_TTL_MS")
                .unwrap_or_else(|_| "1000".to_owned())
                .parse()?,
        ))
        .run_until_signal()
        .await?;
    Ok(())
}

async fn node() -> Result<(), Box<dyn std::error::Error>> {
    let instance_name = required("OBJECT_STORE_INSTANCE_ID")?;
    let peers = parse_addresses(&required("OBJECT_STORE_PEERS")?)?;
    let state = ObjectState::new(instance_name.clone());
    serve(
        &required("OBJECT_STORE_LISTEN")?,
        state.clone(),
        peers.clone(),
    )?;
    let cluster = backend().await?;
    let ttl_ms: u64 = env::var("OBJECT_STORE_PARTICIPANT_LEASE_TTL_MS")
        .unwrap_or_else(|_| "1500".to_owned())
        .parse()?;
    cluster
        .participant(instance_name)
        .resource(RESOURCE, ObjectTransitionHandler::new(state, peers))
        .lease_ttl(Duration::from_millis(ttl_ms))
        .run_until_signal()
        .await?;
    Ok(())
}

async fn status() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}",
        serde_json::to_string_pretty(&backend().await?.observer().snapshot().await?)?
    );
    Ok(())
}

async fn client() -> Result<(), Box<dyn std::error::Error>> {
    let object = env::args().nth(2).ok_or("missing object name")?;
    let command = match env::args().nth(1).as_deref() {
        Some("get") => format!("GET {object}"),
        Some("put") => format!(
            "PUT {object} {}",
            env::args().nth(3).ok_or("missing value")?
        ),
        _ => unreachable!(),
    };
    let observer = backend().await?.observer();
    let partition = partition_for_object(&object);
    let nodes = parse_addresses(&required("OBJECT_STORE_PEERS")?)?;
    for _ in 0..200 {
        let snapshot = observer.snapshot().await?;
        if let Some(instance) = snapshot.routing().leader(RESOURCE, &partition)? {
            if let Some(address) = nodes.get(instance.as_str()) {
                if let Some(response) = request_at(address, &command) {
                    if !response.starts_with("ERR ") {
                        println!("{response}");
                        return Ok(());
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!("no reachable leader for {partition}").into())
}
