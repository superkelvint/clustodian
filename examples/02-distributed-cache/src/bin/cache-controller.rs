use clustodian_distributed_cache::{connect_with_retry, endpoint_config, env_required, parse_u64};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, prefix, cluster) = endpoint_config()?;
    let controller_id = env_required("CACHE_CONTROLLER_ID")?;
    let lease_ttl_ms = parse_u64(
        std::env::var("CACHE_CONTROLLER_LEASE_TTL_MS").ok(),
        "CACHE_CONTROLLER_LEASE_TTL_MS",
        1_500,
    )?;
    connect_with_retry(endpoint, prefix, cluster)
        .await?
        .controller(controller_id)
        .lease_ttl(std::time::Duration::from_millis(lease_ttl_ms))
        .run_until_signal()
        .await?;
    Ok(())
}
