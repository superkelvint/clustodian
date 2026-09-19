use anyhow::{Context, Result};
use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::runtime::{ControllerRuntime, ControllerRuntimeConfig};
use std::env;
#[tokio::main]
async fn main() -> Result<()> {
    let endpoint=req("CLUSTODIAN_M12_ETCD_ENDPOINT")?; let prefix=req("CLUSTODIAN_M12_ETCD_PREFIX")?; let cluster=req("CLUSTODIAN_M12_CLUSTER")?; let controller_id=req("CLUSTODIAN_M12_CONTROLLER_ID")?;
    let lease_ttl_ms:u64=env::var("CLUSTODIAN_M12_CONTROLLER_LEASE_TTL_MS").unwrap_or_else(|_|"1500".into()).parse()?;
    let coordination=EtcdCoordination::connect(EtcdCoordinationConfig{endpoint,prefix,cluster:cluster.clone()}).await?;
    let runtime=ControllerRuntime::new(coordination,ControllerRuntimeConfig{cluster,controller_id,lease_ttl_ms}).await?;
    runtime.run().await.context("controller runtime exited")
}
fn req(name:&str)->Result<String>{env::var(name).with_context(||format!("missing {name}"))}
