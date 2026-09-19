use clustodian::{Cluster, ClusterConfig, ClusterSpec, InstanceSpec, Placement, ResourceSpec};
use clustodian_game_servers::{
    GameServerHandler, PARTICIPANT_LEASE_TTL_SECONDS, REPLICA_COUNT, RESOURCE, WORLD_COUNT,
};
use std::env;
use std::time::Duration;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:23797";
const DEFAULT_CLUSTER: &str = "game-demo";
const DEFAULT_PREFIX: &str = "clustodian-game-demo";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let command = args
        .next()
        .ok_or("expected controller, server, admin, observe, or leader")?;
    match command.as_str() {
        "controller" => run_controller().await?,
        "server" => run_server(args.next().ok_or("server requires an instance id")?).await?,
        "admin" => run_admin(args.next().as_deref(), args.collect()).await?,
        "observe" => print_snapshot().await?,
        "leader" => print_leader(args.next().ok_or("leader requires a partition")?).await?,
        other => return Err(format!("unsupported command: {other}").into()),
    }
    Ok(())
}

fn endpoint() -> String {
    env::var("CLUSTODIAN_ETCD_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned())
}

fn cluster() -> String {
    env::var("CLUSTODIAN_GAME_CLUSTER").unwrap_or_else(|_| DEFAULT_CLUSTER.to_owned())
}

fn prefix() -> String {
    env::var("CLUSTODIAN_GAME_PREFIX").unwrap_or_else(|_| DEFAULT_PREFIX.to_owned())
}

async fn backend() -> Result<Cluster, Box<dyn std::error::Error>> {
    Ok(Cluster::connect(
        ClusterConfig::new(cluster())
            .etcd_endpoints([endpoint()])
            .namespace(prefix()),
    )
    .await?)
}

async fn run_controller() -> Result<(), Box<dyn std::error::Error>> {
    backend()
        .await?
        .controller(
            env::var("CLUSTODIAN_GAME_CONTROLLER_ID")
                .unwrap_or_else(|_| "game-controller".to_owned()),
        )
        .lease_ttl(Duration::from_millis(1_500))
        .run_until_signal()
        .await?;
    Ok(())
}

async fn run_server(instance: String) -> Result<(), Box<dyn std::error::Error>> {
    backend()
        .await?
        .participant(instance.clone())
        .resource(RESOURCE, GameServerHandler::new(instance.clone()))
        .lease_ttl(Duration::from_secs(PARTICIPANT_LEASE_TTL_SECONDS))
        .run_until_signal()
        .await?;
    println!("server={} stopped", instance);
    Ok(())
}

async fn run_admin(
    operation: Option<&str>,
    args: Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cluster = backend().await?;
    match operation.ok_or("admin requires init, add, or remove")? {
        "init" => {
            cluster
                .admin()
                .apply(
                    ClusterSpec::new()
                        .instances([
                            InstanceSpec::new("game-a").zone("zone-game-a"),
                            InstanceSpec::new("game-b").zone("zone-game-b"),
                            InstanceSpec::new("game-c").zone("zone-game-c"),
                        ])
                        .resource(
                            ResourceSpec::leader_standby(RESOURCE)
                                .partitions(WORLD_COUNT)
                                .replicas(REPLICA_COUNT)
                                .placement(Placement::Crush),
                        ),
                )
                .await?;
            println!("configured {WORLD_COUNT} game worlds with RF={REPLICA_COUNT}");
        }
        "add" => {
            let instance = args.first().ok_or("admin add requires an instance id")?;
            let zone = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| format!("zone-{instance}"));
            cluster
                .admin()
                .apply(
                    ClusterSpec::new().instances([InstanceSpec::new(instance.clone()).zone(zone)]),
                )
                .await?;
            println!("configured new game server {instance}");
        }
        "remove" => {
            let instance = args.first().ok_or("admin remove requires an instance id")?;
            cluster.admin().remove_instance(instance).await?;
            println!("removed game server {instance} from placement configuration");
        }
        other => return Err(format!("unsupported admin operation: {other}").into()),
    }
    Ok(())
}

async fn snapshot() -> Result<clustodian::Snapshot, Box<dyn std::error::Error>> {
    Ok(backend().await?.observer().snapshot().await?)
}

async fn print_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(&snapshot().await?)?);
    Ok(())
}

async fn print_leader(partition: String) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot = snapshot().await?;
    let leader = snapshot.routing().leader(RESOURCE, &partition)?;
    if let Some(leader) = leader {
        println!("{leader}");
    } else {
        return Err(format!("no leader for {partition}").into());
    }
    Ok(())
}
