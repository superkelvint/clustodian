use clustodian_distributed_cache::{
    connect_with_retry, endpoint_config, env_required, parse_nodes, parse_u64, spawn_server,
    CacheState,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, prefix, cluster) = endpoint_config()?;
    let instance_name = env_required("CACHE_INSTANCE_ID")?;
    let listen = env_required("CACHE_LISTEN")?;
    let peers = parse_nodes(&std::env::var("CACHE_NODES").unwrap_or_default())?;
    let state = CacheState::with_peers(
        peers
            .iter()
            .filter(|(_, address)| *address != &listen)
            .map(|(instance, address)| (instance.clone(), address.clone()))
            .collect(),
    );
    spawn_server(&listen, state.clone(), peers)?;

    let cluster = connect_with_retry(endpoint, prefix, cluster).await?;
    let ttl_ms = parse_u64(
        std::env::var("CACHE_PARTICIPANT_LEASE_TTL_MS").ok(),
        "CACHE_PARTICIPANT_LEASE_TTL_MS",
        1_500,
    )?;
    cluster
        .participant(instance_name)
        .resource("cache", state)
        .lease_ttl(Duration::from_millis(ttl_ms))
        .run_until_signal()
        .await?;
    Ok(())
}
