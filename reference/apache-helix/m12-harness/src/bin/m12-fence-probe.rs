use anyhow::{Context,Result};
use clustodian::coordination::etcd::{EtcdCoordination,EtcdCoordinationConfig};
use clustodian::election::{ControllerElection,ControllerElectionConfig};
use serde_json::json;
use std::{env,path::PathBuf};
use tokio::time::{sleep,Duration};
#[tokio::main]
async fn main()->Result<()>{let endpoint=req("CLUSTODIAN_M12_ETCD_ENDPOINT")?;let prefix=req("CLUSTODIAN_M12_ETCD_PREFIX")?;let cluster=req("CLUSTODIAN_M12_CLUSTER")?;let id=req("CLUSTODIAN_M12_CONTROLLER_ID")?;let dir=PathBuf::from(req("CLUSTODIAN_M12_FENCE_DIR")?);tokio::fs::create_dir_all(&dir).await?;
 let coordination=EtcdCoordination::connect(EtcdCoordinationConfig{endpoint,prefix,cluster:cluster.clone()}).await?;let election=ControllerElection::new(coordination.clone(),ControllerElectionConfig{cluster,controller_id:id,lease_ttl_ms:1000}).await?;let leadership=election.acquire().await?;tokio::fs::write(dir.join("acquired"),b"1").await?;while !dir.join("attempt").exists(){sleep(Duration::from_millis(20)).await;}
 let accepted=coordination.put_controller_owned(&leadership.authority(),"diagnostics/m12-fence-probe",b"stale").await.is_ok();println!("{}",json!({"accepted":accepted}));if accepted{anyhow::bail!("stale controller authority unexpectedly accepted")}Ok(())}
fn req(name:&str)->Result<String>{env::var(name).with_context(||format!("missing {name}"))}
