use anyhow::{bail,Context,Result};
use clustodian::admin::{ClusterAdmin,InstanceSpec,PlacementSpec,ResourceSpec};
use clustodian::coordination::etcd::{EtcdCoordination,EtcdCoordinationConfig};
use clustodian::observe::ClusterObserver;
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeMap,env,fs};
#[derive(Deserialize)]struct ResourceJson{name:String,partitions:usize,replicas:usize,rebalance:String,#[serde(default)]preference_lists:BTreeMap<String,Vec<String>>}
#[tokio::main]
async fn main()->Result<()>{let mut args=env::args().skip(1);let cmd=args.next().context("missing command")?;let endpoint=req("CLUSTODIAN_M12_ETCD_ENDPOINT")?;let prefix=req("CLUSTODIAN_M12_ETCD_PREFIX")?;let cluster=req("CLUSTODIAN_M12_CLUSTER")?;let coordination=EtcdCoordination::connect(EtcdCoordinationConfig{endpoint,prefix,cluster:cluster.clone()}).await?;
 match cmd.as_str(){
  "ensure-cluster"=>ClusterAdmin::new(coordination).ensure_cluster(&cluster).await?,
  "put-instance"=>{let id=args.next().context("instance id")?;let zone=args.next().context("zone")?;ClusterAdmin::new(coordination).put_instance(InstanceSpec{instance_id:id,zone}).await?;},
  "put-resource-json"=>{let path=args.next().context("resource json path")?;let r:ResourceJson=serde_json::from_slice(&fs::read(path)?)?;let placement=match r.rebalance.as_str(){"CRUSH"=>PlacementSpec::Crush,"SEMI_AUTO"=>PlacementSpec::SemiAuto{preference_lists:r.preference_lists},other=>bail!("unsupported placement {other}")};ClusterAdmin::new(coordination).put_resource(ResourceSpec{name:r.name,partitions:r.partitions,replicas:r.replicas,state_model:"LeaderStandby".into(),placement}).await?;},
  "snapshot"=>{let snap=ClusterObserver::new(coordination).snapshot().await?;println!("{}",serde_json::to_string(&snap)?);},
  "raw-session-state-present"=>{let instance=args.next().context("instance")?;let session=args.next().context("session")?;let present=ClusterObserver::new(coordination).session_current_state_exists(&instance,&session).await?;println!("{}",json!({"present":present}));},
  other=>bail!("unknown command {other}")}
 Ok(())}
fn req(name:&str)->Result<String>{env::var(name).with_context(||format!("missing {name}"))}
