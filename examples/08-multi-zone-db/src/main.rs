use clustodian::{
    Cluster, ClusterConfig, ClusterSpec, InstanceSpec, Placement, ResourceSpec, Topology,
};
use clustodian_multi_zone_db::{
    address_map, endpoint_config, parse_nodes, request_at, DbHandler, DbState, PARTITION_COUNT,
    REPLICA_COUNT, RESOURCE,
};
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let command = args
        .next()
        .ok_or("expected admin, controller, node, status, leader, put, or get")?;
    match command.as_str() {
        "admin" => admin(args.next().as_deref(), args.collect()).await?,
        "controller" => controller().await?,
        "node" => node(args.next().ok_or("node requires instance")?).await?,
        "status" => println!("{}", serde_json::to_string_pretty(&snapshot().await?)?),
        "leader" => println!(
            "{}",
            leader(
                &snapshot().await?,
                &args.next().ok_or("leader requires partition")?
            )?
            .ok_or("no leader")?
        ),
        "put" => client("PUT", args.collect()).await?,
        "get" => client("GET", args.collect()).await?,
        other => return Err(format!("unknown command {other}").into()),
    }
    Ok(())
}
async fn backend() -> Result<Cluster, Box<dyn std::error::Error>> {
    let (endpoint, prefix, cluster) = endpoint_config()?;
    Ok(Cluster::connect(
        ClusterConfig::new(cluster)
            .etcd_endpoints([endpoint])
            .namespace(prefix),
    )
    .await?)
}
async fn controller() -> Result<(), Box<dyn std::error::Error>> {
    backend()
        .await?
        .controller(env::var("ZONEDB_CONTROLLER_ID").unwrap_or_else(|_| "zonedb-controller".into()))
        .lease_ttl(Duration::from_millis(1500))
        .run_until_signal()
        .await?;
    Ok(())
}
async fn node(instance: String) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = backend().await?;
    let nodes = parse_nodes(&env::var("ZONEDB_NODES")?)?;
    let state = DbState::default();
    let listen = nodes
        .get(&instance)
        .ok_or("instance missing from ZONEDB_NODES")?
        .1
        .clone();
    clustodian_multi_zone_db::spawn_server(&listen, state.clone(), address_map(&nodes))?;
    let peers = address_map(&nodes)
        .into_iter()
        .filter(|(name, _)| name != &instance)
        .collect();
    cluster
        .participant(instance.clone())
        .resource(RESOURCE, DbHandler::new(state, instance, peers))
        .lease_ttl(Duration::from_millis(1200))
        .run_until_signal()
        .await?;
    Ok(())
}
async fn admin(
    operation: Option<&str>,
    args: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = backend().await?;
    let (nodespec, _) = (
        args.first()
            .ok_or("admin init/add requires node specification")?,
        args.get(1),
    );
    match operation.ok_or("admin requires init, add, or remove")? {
        "init" => {
            let nodes = parse_nodes(nodespec)?;
            let instances = nodes
                .into_iter()
                .map(|(id, (zone, _))| InstanceSpec::new(id).zone(zone))
                .collect::<Vec<_>>();
            cluster
                .admin()
                .apply(
                    ClusterSpec::new().instances(instances).resource(
                        ResourceSpec::leader_standby(RESOURCE)
                            .partitions(PARTITION_COUNT)
                            .replicas(REPLICA_COUNT)
                            .placement(Placement::crush().topology(Topology::zones())),
                    ),
                )
                .await?;
            println!("configured {PARTITION_COUNT} partitions RF={REPLICA_COUNT} across zones");
        }
        "add" => {
            let n = parse_nodes(nodespec)?;
            let (i, (z, _)) = n.into_iter().next().ok_or("missing node")?;
            cluster
                .admin()
                .apply(ClusterSpec::new().instances([InstanceSpec::new(i).zone(z)]))
                .await?;
        }
        "remove" => {
            cluster.admin().remove_instance(nodespec).await?;
        }
        _ => return Err("unknown admin operation".into()),
    }
    Ok(())
}
async fn snapshot() -> Result<clustodian::Snapshot, Box<dyn std::error::Error>> {
    Ok(backend().await?.observer().snapshot().await?)
}
fn leader(s: &clustodian::Snapshot, p: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    Ok(s.routing()
        .leader(RESOURCE, p)?
        .map(|instance| instance.to_string()))
}
async fn client(op: &str, args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let key = args.first().ok_or("missing key")?;
    let value = args.get(1);
    let nodes = address_map(&parse_nodes(&env::var("ZONEDB_NODES")?)?);
    let p = clustodian_multi_zone_db::partition_for_key(key);
    for _ in 0..80 {
        if let Some(i) = leader(&snapshot().await?, &p)? {
            if let Some(a) = nodes.get(&i) {
                let cmd = if op == "PUT" {
                    format!("PUT {key} {}", value.ok_or("missing value")?)
                } else {
                    format!("GET {key}")
                };
                if let Ok(r) = request_at(a, &cmd) {
                    if !r.starts_with("ERR") {
                        println!("{r}");
                        return Ok(());
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await
    }
    Err("no reachable leader".into())
}
