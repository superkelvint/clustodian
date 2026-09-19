use clustodian::model::PartitionId;
use clustodian::{Cluster, ClusterConfig, ClusterSpec, Placement, ResourceSpec};
use futures_util::{SinkExt, StreamExt};
use std::env;
use std::error::Error;
use std::io::Write;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::{accept_hdr_async, connect_async, tungstenite::Message};

use clustodian_chat_cluster::{
    leader_for_snapshot, parse_endpoints, room_from_path, room_partition, ChatMessage, Ownership,
    Redirect, Welcome, PARTITION_COUNT, REPLICA_COUNT, RESOURCE,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    match env::args().nth(1).as_deref() {
        Some("admin") => admin().await,
        Some("controller") => controller().await,
        Some("participant") => participant().await,
        Some("client") => client().await,
        Some("observe") => observe().await,
        _ => {
            eprintln!(
                "usage: chat-cluster <admin|controller|participant|client|observe> [options]"
            );
            Err("invalid command".into())
        }
    }
}

async fn admin() -> Result<(), Box<dyn Error>> {
    let cluster = backend().await?;
    let cluster_name = env::var("CHAT_CLUSTER").unwrap_or_else(|_| "chat-demo".into());
    cluster
        .admin()
        .apply(
            ClusterSpec::new()
                .instances(["node-a", "node-b", "node-c"])
                .resource(
                    ResourceSpec::leader_standby(RESOURCE)
                        .partitions(PARTITION_COUNT)
                        .replicas(REPLICA_COUNT)
                        .placement(Placement::Crush),
                ),
        )
        .await?;
    println!(
        "configured cluster={} resource={} partitions={} replicas={}",
        cluster_name, RESOURCE, PARTITION_COUNT, REPLICA_COUNT
    );
    Ok(())
}

async fn controller() -> Result<(), Box<dyn Error>> {
    let cluster = backend().await?;
    let controller_id = env::var("CHAT_CONTROLLER_ID").unwrap_or_else(|_| "controller-a".into());
    let ready_file = env::var_os("CHAT_READY_FILE");
    cluster
        .controller(controller_id)
        .lease_ttl(Duration::from_millis(env_u64(
            "CHAT_CONTROLLER_LEASE_TTL_MS",
            1_500,
        )))
        .on_ready(move || {
            if let Some(path) = ready_file {
                std::fs::write(path, b"ready\n")?;
            }
            Ok::<(), std::io::Error>(())
        })
        .run_until_signal()
        .await
        .map_err(|error| error.to_string().into())
}

async fn participant() -> Result<(), Box<dyn Error>> {
    let instance_name = required("CHAT_INSTANCE_ID")?;
    let listen = required("CHAT_LISTEN")?;
    let cluster = backend().await?;
    let ownership = Ownership::default();
    let listener = TcpListener::bind(&listen).await?;
    let rooms: RoomChannels = Arc::new(Mutex::new(Default::default()));
    let server_ownership = ownership.clone();
    let server_instance = instance_name.clone();
    let server_task = tokio::spawn(async move {
        websocket_server(listener, server_instance, server_ownership, rooms).await
    });

    let ready_file = env::var_os("CHAT_READY_FILE");
    let result = cluster
        .participant(instance_name.clone())
        .resource(RESOURCE, ownership)
        .lease_ttl(Duration::from_millis(
            env_u64("CHAT_PARTICIPANT_LEASE_TTL_MS", 1_000).max(1_000),
        ))
        .on_ready(move || {
            if let Some(path) = ready_file {
                std::fs::write(path, b"ready\n")?;
            }
            Ok::<(), std::io::Error>(())
        })
        .run_until_signal()
        .await
        .map_err(|error| error.to_string().into());
    server_task.abort();
    result
}

type RoomChannels = Arc<Mutex<std::collections::HashMap<String, broadcast::Sender<String>>>>;

async fn websocket_server(
    listener: TcpListener,
    instance: String,
    ownership: Ownership,
    rooms: RoomChannels,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let instance = instance.clone();
        let ownership = ownership.clone();
        let rooms = rooms.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, instance, ownership, rooms).await {
                eprintln!("chat connection {peer}: {error}");
            }
        });
    }
}

#[allow(clippy::result_large_err)]
async fn handle_connection(
    stream: TcpStream,
    instance: String,
    ownership: Ownership,
    rooms: RoomChannels,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let requested_path = Arc::new(StdMutex::new(None::<String>));
    let path_for_callback = requested_path.clone();
    let mut socket = accept_hdr_async(stream, move |request: &Request, response: Response| {
        *path_for_callback
            .lock()
            .expect("websocket request mutex is not poisoned") =
            Some(request.uri().path().to_owned());
        Ok(response)
    })
    .await?;
    let path = requested_path
        .lock()
        .expect("websocket request mutex is not poisoned")
        .clone()
        .unwrap_or_default();
    let room = room_from_path(&path)
        .ok_or_else(|| "WebSocket URL must use /room/<room>".to_owned())?
        .to_owned();
    let partition = PartitionId::new(room_partition(&room))?;
    if !ownership.is_leader(&partition) {
        socket
            .send(Message::Text(
                serde_json::to_string(&Redirect {
                    r#type: "redirect",
                    room: &room,
                    reason: "room is not active on this server",
                })?
                .into(),
            ))
            .await?;
        return Ok(());
    }
    let sender = {
        let mut channels = rooms.lock().await;
        channels
            .entry(room.clone())
            .or_insert_with(|| broadcast::channel(32).0)
            .clone()
    };
    let mut receiver = sender.subscribe();
    let mut ownership_changes = ownership.subscribe();
    socket
        .send(Message::Text(
            serde_json::to_string(&Welcome {
                r#type: "welcome",
                room: &room,
                owner: &instance,
                partition: partition.to_string(),
            })?
            .into(),
        ))
        .await?;
    loop {
        if !ownership.is_leader(&partition) {
            break;
        }
        tokio::select! {
            changed = ownership_changes.changed() => {
                if changed.is_err() || !ownership.is_leader(&partition) {
                    break;
                }
            }
            incoming = socket.next() => {
                match incoming.transpose()? {
                    Some(Message::Text(text)) => {
                        // Revalidate after receiving the message. A role
                        // transition may have happened while select! was
                        // waiting, and a demoted owner must not broadcast it.
                        if !ownership.is_leader(&partition) {
                            break;
                        }
                        let event = ChatMessage {
                            r#type: "message",
                            room: &room,
                            from: &instance,
                            text: &text,
                        };
                        sender.send(serde_json::to_string(&event)?)?;
                    }
                    Some(Message::Ping(payload)) => socket.send(Message::Pong(payload)).await?,
                    Some(Message::Close(_)) | None => break,
                    _ => {}
                }
            }
            event = receiver.recv() => {
                if let Ok(event) = event {
                    socket.send(Message::Text(event.into())).await?;
                }
            }
        }
    }
    Ok(())
}

async fn client() -> Result<(), Box<dyn Error>> {
    let room = option_value("--room")?.unwrap_or_else(|| "lobby".into());
    let message = option_value("--message")?;
    let wait_for_reconnect = has_flag("--wait-for-reconnect");
    let endpoints = parse_endpoints(&required("CHAT_NODE_ENDPOINTS")?)?;
    let observer = backend().await?.observer();
    let mut sent = false;
    let mut connected_once = false;
    loop {
        let (owner, address) = loop {
            let snapshot = observer.snapshot().await?;
            if let Some(owner) = leader_for_snapshot(&snapshot, &room)? {
                if let Some(address) = endpoints.get(&owner) {
                    break (owner, address.clone());
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        let url = format!("ws://{address}/room/{room}");
        let (mut socket, _) = match connect_async(url).await {
            Ok(connection) => connection,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let welcome = match socket.next().await {
            Some(Ok(Message::Text(welcome))) => welcome,
            Some(Ok(_)) | Some(Err(_)) | None => continue,
        };
        let welcome_json: serde_json::Value = serde_json::from_str(&welcome)?;
        if welcome_json.get("type").and_then(|value| value.as_str()) != Some("welcome") {
            continue;
        }
        emit(if connected_once {
            format!("RECONNECTED room={room} owner={owner}")
        } else {
            format!("CONNECTED room={room} owner={owner}")
        });
        connected_once = true;
        if let Some(message) = message.as_deref() {
            if !sent {
                socket
                    .send(Message::Text(message.to_owned().into()))
                    .await?;
                sent = true;
                emit(format!("SENT room={room} text={message}"));
            }
        }
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(value))) => {
                    if let Ok(event) = serde_json::from_str::<serde_json::Value>(&value) {
                        if event.get("type").and_then(|item| item.as_str()) == Some("message") {
                            emit(format!("MESSAGE room={room} text={}", event["text"]));
                            if !wait_for_reconnect {
                                return Ok(());
                            }
                        }
                    }
                }
                Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                _ => {}
            }
        }
        if !wait_for_reconnect {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn observe() -> Result<(), Box<dyn Error>> {
    let snapshot = backend().await?.observer().snapshot().await?;
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}

async fn backend() -> Result<Cluster, Box<dyn Error>> {
    let endpoint =
        env::var("CHAT_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".into());
    let prefix = required("CHAT_PREFIX")?;
    let cluster = env::var("CHAT_CLUSTER").unwrap_or_else(|_| "chat-demo".into());
    Ok(Cluster::connect(
        ClusterConfig::new(cluster)
            .etcd_endpoints([endpoint])
            .namespace(prefix),
    )
    .await?)
}

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    env::var(name).map_err(|_| format!("{name} is required").into())
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn option_value(name: &str) -> Result<Option<String>, Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let Some(index) = args.iter().position(|arg| arg == name) else {
        return Ok(None);
    };
    args.get(index + 1)
        .cloned()
        .map(Some)
        .ok_or_else(|| format!("{name} requires a value").into())
}

fn has_flag(name: &str) -> bool {
    env::args().any(|arg| arg == name)
}

fn emit(line: String) {
    println!("{line}");
    let _ = std::io::stdout().flush();
}
