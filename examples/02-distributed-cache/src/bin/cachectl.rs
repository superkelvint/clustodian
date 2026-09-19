use clustodian::{Cluster, ClusterSpec, InstanceSpec, Placement, ResourceSpec};
use clustodian_distributed_cache::{
    connect_with_retry, endpoint_config, env_required, parse_nodes, partition_for_key, request_at,
    PARTITION_COUNT, RESOURCE,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = std::env::args()
        .nth(1)
        .ok_or("usage: cachectl <init|add-node|remove-node|status|owner|put|get>")?;
    let (endpoint, prefix, cluster) = endpoint_config()?;
    let cluster_handle = connect_with_retry(endpoint, prefix, cluster).await?;
    match command.as_str() {
        "init" => {
            let nodes = parse_nodes(
                &std::env::args()
                    .nth(2)
                    .ok_or("init needs INSTANCE=ADDRESS,...")?,
            )?;
            cluster_handle
                .admin()
                .apply(
                    ClusterSpec::new()
                        .instances(nodes.keys().map(|id| InstanceSpec::new(id).zone("local")))
                        .resource(
                            ResourceSpec::leader_standby(RESOURCE)
                                .partitions(PARTITION_COUNT)
                                .replicas(2)
                                .placement(Placement::Crush),
                        ),
                )
                .await?;
            println!("configured {PARTITION_COUNT} cache partitions at RF=2");
        }
        "add-node" => {
            let nodes = parse_nodes(
                &std::env::args()
                    .nth(2)
                    .ok_or("add-node needs INSTANCE=ADDRESS")?,
            )?;
            let (instance_id, _) = nodes.into_iter().next().ok_or("missing node")?;
            cluster_handle
                .admin()
                .apply(ClusterSpec::new().instances([InstanceSpec::new(instance_id).zone("local")]))
                .await?;
            println!("added instance");
        }
        "remove-node" => {
            cluster_handle
                .admin()
                .remove_instance(
                    &std::env::args()
                        .nth(2)
                        .ok_or("remove-node needs INSTANCE")?,
                )
                .await?;
            println!("removed instance configuration");
        }
        "status" => {
            let snapshot = cluster_handle.observer().snapshot().await?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        "owner" => {
            let key = std::env::args().nth(2).ok_or("owner needs KEY")?;
            let owner = owner(&cluster_handle, &key).await?;
            println!("{owner}");
        }
        "put" => {
            let key = std::env::args().nth(2).ok_or("put needs KEY VALUE")?;
            let value = std::env::args().nth(3).ok_or("put needs KEY VALUE")?;
            println!(
                "{}",
                route(&cluster_handle, &key, &format!("PUT {key} {value}")).await?
            );
        }
        "get" => {
            let key = std::env::args().nth(2).ok_or("get needs KEY")?;
            println!(
                "{}",
                route(&cluster_handle, &key, &format!("GET {key}")).await?
            );
        }
        _ => return Err(format!("unknown command {command}").into()),
    }
    Ok(())
}

async fn route(
    cluster: &Cluster,
    key: &str,
    command: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let nodes = parse_nodes(&env_required("CACHE_NODES")?)?;
    let partition = partition_for_key(key);
    let observer = cluster.observer();
    for _ in 0..40 {
        let snapshot = observer.snapshot().await?;
        let leader = snapshot.routing().leader(RESOURCE, &partition)?;
        if let Some(address) = leader
            .as_ref()
            .and_then(|instance| nodes.get(instance.as_str()))
        {
            if let Ok(response) = request_at(address, command) {
                if !response.starts_with("ERR ") {
                    return Ok(response);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err(format!("no reachable leader for partition {partition}").into())
}

async fn owner(cluster: &Cluster, key: &str) -> Result<String, Box<dyn std::error::Error>> {
    let partition = partition_for_key(key);
    let observer = cluster.observer();
    for _ in 0..100 {
        let snapshot = observer.snapshot().await?;
        if let Some(leader) = snapshot.routing().leader(RESOURCE, &partition)? {
            return Ok(leader.to_string());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err(format!("no leader for partition {partition}").into())
}
