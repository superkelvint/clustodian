use clustodian::{Cluster, ClusterConfig, ClusterSpec, InstanceSpec, Placement, ResourceSpec};
use clustodian_replicated_kv::{KvTransitionHandler, NodeState};
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const RESOURCE: &str = "kv";
const PARTITION: &str = "kv_0";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let command = env::args()
        .nth(1)
        .ok_or("usage: replicated-kv <setup|controller|node|client|status>")?;
    match command.as_str() {
        "setup" => setup().await,
        "controller" => controller().await,
        "node" => node().await,
        "client" => client().await,
        "status" => status().await,
        other => Err(format!("unknown command: {other}").into()),
    }
}

async fn cluster_handle() -> Result<Cluster, Box<dyn Error>> {
    Ok(Cluster::connect(
        ClusterConfig::new(cluster())
            .etcd_endpoints([required("CLUSTODIAN_KV_ETCD_ENDPOINT")?])
            .namespace(required("CLUSTODIAN_KV_PREFIX")?),
    )
    .await?)
}

async fn setup() -> Result<(), Box<dyn Error>> {
    let cluster = cluster_handle().await?;
    let preference_lists = std::iter::once((
        PARTITION.to_owned(),
        vec![
            String::from("node-a"),
            String::from("node-b"),
            String::from("node-c"),
        ],
    ))
    .collect();
    cluster
        .admin()
        .apply(
            ClusterSpec::new()
                .instances([
                    InstanceSpec::new("node-a").zone("local"),
                    InstanceSpec::new("node-b").zone("local"),
                    InstanceSpec::new("node-c").zone("local"),
                ])
                .resource(
                    ResourceSpec::leader_standby(RESOURCE)
                        .partitions(1)
                        .replicas(3)
                        .placement(Placement::semi_auto(preference_lists)),
                ),
        )
        .await?;
    println!("configured kv_0 with replicas [node-a, node-b, node-c]");
    Ok(())
}

async fn controller() -> Result<(), Box<dyn Error>> {
    cluster_handle()
        .await?
        .controller(
            env::var("CLUSTODIAN_KV_CONTROLLER_ID")
                .unwrap_or_else(|_| String::from("controller-1")),
        )
        .lease_ttl(Duration::from_millis(
            env::var("CLUSTODIAN_KV_CONTROLLER_LEASE_TTL_MS")
                .unwrap_or_else(|_| String::from("1000"))
                .parse()?,
        ))
        .run_until_signal()
        .await
        .map_err(Into::into)
}

async fn node() -> Result<(), Box<dyn Error>> {
    let instance_name = required("CLUSTODIAN_KV_INSTANCE_ID")?;
    let listen: SocketAddr = required("CLUSTODIAN_KV_LISTEN")?.parse()?;
    let peers = parse_peers(&env::var("CLUSTODIAN_KV_PEERS").unwrap_or_default())?;
    let state = NodeState::new(instance_name.clone());
    let listener = TcpListener::bind(listen).await?;
    let server_state = state.clone();
    let server = tokio::spawn(async move { serve(listener, server_state, peers).await });

    let cluster = cluster_handle().await?;
    let ttl = env::var("CLUSTODIAN_KV_PARTICIPANT_LEASE_TTL_MS")
        .unwrap_or_else(|_| String::from("2000"))
        .parse::<u64>()?;
    let ready_file = env::var_os("CLUSTODIAN_KV_READY_FILE");
    let participant = cluster
        .participant(instance_name.clone())
        .resource(RESOURCE, KvTransitionHandler::new(state))
        .lease_ttl(Duration::from_millis(ttl))
        .on_ready(move || {
            if let Some(path) = ready_file {
                std::fs::write(path, b"ready\n")?;
            }
            Ok::<(), std::io::Error>(())
        });
    let result = participant.run_until_signal().await;
    server.abort();
    result?;
    Ok(())
}

async fn serve(
    listener: TcpListener,
    state: NodeState,
    peers: Vec<(String, SocketAddr)>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let state = Arc::new(state);
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        let peers = peers.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, state, peers).await {
                eprintln!("client connection failed: {error}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    state: Arc<NodeState>,
    peers: Vec<(String, SocketAddr)>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        let response = handle_request(&line, &state, &peers).await;
        writer.write_all(response.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }
    Ok(())
}

async fn handle_request(
    request: &str,
    state: &NodeState,
    peers: &[(String, SocketAddr)],
) -> String {
    let mut fields = request.splitn(3, ' ');
    match fields.next().unwrap_or_default() {
        "GET" => {
            if !matches!(state.role().as_str(), "LEADER" | "STANDBY") {
                return String::from("NOT_REPLICA");
            }
            match fields.next().and_then(|key| state.get(key)) {
                Some(value) => format!("VALUE {value}"),
                None => String::from("NOT_FOUND"),
            }
        }
        "STATUS" => format!("STATUS {} {}", state.instance(), state.role()),
        "REPLICATE" => {
            if !matches!(state.role().as_str(), "LEADER" | "STANDBY") {
                return String::from("NOT_REPLICA");
            }
            match (fields.next(), fields.next()) {
                (Some(key), Some(value)) => {
                    state.put_replica(key, value);
                    String::from("OK")
                }
                _ => String::from("ERROR usage: REPLICATE <key> <value>"),
            }
        }
        "PUT" => {
            let (Some(key), Some(value)) = (fields.next(), fields.next()) else {
                return String::from("ERROR usage: PUT <key> <value>");
            };
            if state.role() != "LEADER" {
                return format!("NOT_LEADER {}", state.instance());
            }
            state.put_replica(key, value);
            let mut replicated = 0usize;
            for (name, address) in peers {
                if name == state.instance() {
                    continue;
                }
                if send_line(*address, &format!("REPLICATE {key} {value}"))
                    .await
                    .is_ok()
                {
                    replicated += 1;
                }
            }
            if state.role() != "LEADER" {
                return String::from("NOT_LEADER");
            }
            format!("OK replicated={replicated}")
        }
        _ => String::from("ERROR unknown command"),
    }
}

async fn send_line(
    address: SocketAddr,
    request: &str,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(b"\n").await?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).await?;
    Ok(response.trim_end().to_owned())
}

async fn client() -> Result<(), Box<dyn Error>> {
    let address: SocketAddr = required("CLUSTODIAN_KV_ADDRESS")?.parse()?;
    let request = env::args().skip(2).collect::<Vec<_>>().join(" ");
    if request.is_empty() {
        return Err("usage: replicated-kv client GET <key> | PUT <key> <value>".into());
    }
    println!(
        "{}",
        send_line(address, &request)
            .await
            .map_err(|error| error.to_string())?
    );
    Ok(())
}

async fn status() -> Result<(), Box<dyn Error>> {
    let snapshot = cluster_handle().await?.observer().snapshot().await?;
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}

fn parse_peers(value: &str) -> Result<Vec<(String, SocketAddr)>, Box<dyn Error>> {
    value
        .split(',')
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let (name, address) = entry.split_once('=').ok_or("peer must be name=address")?;
            Ok((name.to_owned(), address.parse()?))
        })
        .collect()
}

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    Ok(env::var(name).map_err(|_| format!("{name} is required"))?)
}

fn cluster() -> String {
    env::var("CLUSTODIAN_KV_CLUSTER").unwrap_or_else(|_| String::from("replicated-kv"))
}
