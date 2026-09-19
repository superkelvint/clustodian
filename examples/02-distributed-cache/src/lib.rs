use clustodian::{
    ApplicationError, Cluster, ClusterConfig, ResourceHandler, ResourceState, ResourceTransition,
    TransitionContext, TransitionError,
};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

pub const RESOURCE: &str = "cache";
pub const PARTITION_COUNT: usize = 12;
const CONNECT_RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(30);

/// Connect to the control plane while allowing a busy local etcd to recover.
pub async fn connect_with_retry(
    endpoint: String,
    prefix: String,
    cluster: String,
) -> Result<Cluster, ApplicationError> {
    let config = ClusterConfig::new(cluster)
        .etcd_endpoints([endpoint])
        .namespace(prefix);
    let deadline = tokio::time::Instant::now() + CONNECT_RETRY_WINDOW;
    loop {
        match Cluster::connect(config.clone()).await {
            Ok(cluster) => return Ok(cluster),
            Err(ApplicationError::Coordination(error))
                if error.is_transient() && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Process-local cache data and the roles realized by the participant.
#[derive(Clone, Default)]
pub struct CacheState {
    roles: Arc<Mutex<BTreeMap<String, String>>>,
    shards: Arc<Mutex<BTreeMap<String, BTreeMap<String, String>>>>,
    peers: BTreeMap<String, String>,
}

impl CacheState {
    pub fn with_peers(peers: BTreeMap<String, String>) -> Self {
        Self {
            roles: Arc::new(Mutex::new(BTreeMap::new())),
            shards: Arc::new(Mutex::new(BTreeMap::new())),
            peers,
        }
    }

    pub fn role(&self, partition: &str) -> Option<String> {
        self.roles
            .lock()
            .expect("role mutex is not poisoned")
            .get(partition)
            .cloned()
    }

    pub fn value(&self, partition: &str, key: &str) -> Option<String> {
        self.shards
            .lock()
            .expect("shard mutex is not poisoned")
            .get(partition)
            .and_then(|shard| shard.get(key).cloned())
    }

    pub fn put_local(&self, partition: &str, key: &str, value: &str) {
        self.shards
            .lock()
            .expect("shard mutex is not poisoned")
            .entry(partition.to_owned())
            .or_default()
            .insert(key.to_owned(), value.to_owned());
    }

    fn partition_snapshot(&self, partition: &str) -> BTreeMap<String, String> {
        self.shards
            .lock()
            .expect("shard mutex is not poisoned")
            .get(partition)
            .cloned()
            .unwrap_or_default()
    }

    fn hydrate_partition(&self, partition: &str) -> Result<bool, TransitionError> {
        let mut inactive_peers = 0;
        let mut unavailable_peer = false;
        for address in self.peers.values() {
            let Some(response) = send(address, &format!("DUMP {partition}")) else {
                unavailable_peer = true;
                continue;
            };
            if response == "ERR not-replica" {
                inactive_peers += 1;
                continue;
            }
            let Some(payload) = response.strip_prefix("DUMP ") else {
                return Err(TransitionError::new(format!(
                    "cannot activate cache partition {partition}: peer returned an invalid snapshot"
                )));
            };
            let Ok(values) = serde_json::from_str::<BTreeMap<String, String>>(payload) else {
                return Err(TransitionError::new(format!(
                    "cannot activate cache partition {partition}: peer returned an invalid snapshot"
                )));
            };
            self.shards
                .lock()
                .expect("shard mutex is not poisoned")
                .insert(partition.to_owned(), values);
            return Ok(false);
        }
        Ok(unavailable_peer || inactive_peers == self.peers.len())
    }

    pub fn role_count(&self) -> usize {
        self.roles.lock().expect("role mutex is not poisoned").len()
    }
}

impl ResourceHandler for CacheState {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        let partition = transition.partition().to_string();
        let target = transition.target();
        if matches!(target, ResourceState::Dropped | ResourceState::Offline) {
            self.roles
                .lock()
                .expect("role mutex is not poisoned")
                .remove(&partition);
            return Ok(());
        }
        if matches!(target, ResourceState::Standby)
            || (matches!(target, ResourceState::Leader)
                && matches!(transition.source(), ResourceState::Offline))
        {
            // A fresh process must hydrate before becoming active. A
            // STANDBY -> LEADER promotion already has the bootstrapped local
            // copy and must remain possible when the old leader is down.
            let hydration_partition = partition.clone();
            let state = self.clone();
            let no_active_peer =
                tokio::task::spawn_blocking(move || state.hydrate_partition(&hydration_partition))
                    .await
                    .map_err(|error| {
                        TransitionError::new(format!("cache hydration task failed: {error}"))
                    })??;
            if no_active_peer
                && !(matches!(target, ResourceState::Standby)
                    && transition.source() == ResourceState::Offline)
            {
                return Err(TransitionError::new(format!(
                    "cannot activate cache partition {partition}: no active replica supplied a snapshot"
                )));
            }
        }
        self.shards
            .lock()
            .expect("shard mutex is not poisoned")
            .entry(partition.clone())
            .or_default();
        self.roles
            .lock()
            .expect("role mutex is not poisoned")
            .insert(partition, target.to_string());
        Ok(())
    }
}

/// Stable FNV-1a routing keeps a key on the same control-plane partition.
pub fn partition_for_key(key: &str) -> String {
    let mut hash = 2_166_136_261_u32;
    for byte in key.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    format!("{RESOURCE}_{}", hash as usize % PARTITION_COUNT)
}

pub fn parse_nodes(value: &str) -> Result<BTreeMap<String, String>, String> {
    value
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (name, address) = entry
                .split_once('=')
                .ok_or_else(|| format!("node must be INSTANCE=ADDRESS: {entry}"))?;
            if name.is_empty() || address.is_empty() {
                return Err(format!("node must have an instance and address: {entry}"));
            }
            Ok((name.to_owned(), address.to_owned()))
        })
        .collect()
}

pub fn spawn_server(
    address: &str,
    state: CacheState,
    peers: BTreeMap<String, String>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(address)?;
    let peers: BTreeMap<String, String> = peers
        .into_iter()
        .filter(|(_, peer_address)| peer_address != address)
        .collect();
    thread::Builder::new()
        .name("cache-listener".to_owned())
        .spawn(move || {
            for stream in listener.incoming().flatten() {
                let state = state.clone();
                let peers = peers.clone();
                let _ = thread::Builder::new()
                    .name("cache-client".to_owned())
                    .spawn(move || handle_client(stream, state, peers));
            }
        })?;
    Ok(())
}

fn handle_client(stream: TcpStream, state: CacheState, peers: BTreeMap<String, String>) {
    let reader_stream = match stream.try_clone() {
        Ok(stream) => stream,
        Err(_) => return,
    };
    let mut reader = BufReader::new(reader_stream);
    let mut writer = BufWriter::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                let response = request(line.trim_end(), &state, &peers);
                if writeln!(writer, "{response}")
                    .and_then(|_| writer.flush())
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

fn request(line: &str, state: &CacheState, peers: &BTreeMap<String, String>) -> String {
    let mut words = line.splitn(3, ' ');
    match words.next() {
        Some("PING") => "PONG".to_owned(),
        Some("STATUS") => serde_json::json!({"roles": state.role_count()}).to_string(),
        Some("GET") => {
            let Some(key) = words.next() else {
                return "ERR missing-key".to_owned();
            };
            let partition = partition_for_key(key);
            let roles = state.roles.lock().expect("role mutex is not poisoned");
            if !matches!(
                roles.get(&partition).map(String::as_str),
                Some("LEADER" | "STANDBY")
            ) {
                return "ERR not-replica".to_owned();
            }
            match state
                .shards
                .lock()
                .expect("shard mutex is not poisoned")
                .get(&partition)
                .and_then(|shard| shard.get(key).cloned())
            {
                Some(value) => format!("VALUE {value}"),
                None => "NOT_FOUND".to_owned(),
            }
        }
        Some("PUT") => {
            let Some(key) = words.next() else {
                return "ERR missing-key".to_owned();
            };
            let Some(value) = words.next() else {
                return "ERR missing-value".to_owned();
            };
            let partition = partition_for_key(key);
            // Hold the role lock through the write so a demotion either wins
            // before this request or is published after its side effects.
            let roles = state.roles.lock().expect("role mutex is not poisoned");
            if roles.get(&partition).map(String::as_str) != Some("LEADER") {
                return "ERR not-leader".to_owned();
            }
            let replicas = peers
                .values()
                .filter(|address| {
                    send(address, &format!("REPLICATE {partition} {key} {value}"))
                        .is_some_and(|response| response == "OK")
                })
                .count();
            if replicas == 0 {
                return "ERR no-standby".to_owned();
            }
            state.put_local(&partition, key, value);
            "OK".to_owned()
        }
        Some("REPLICATE") => {
            let mut words = line.splitn(4, ' ');
            let _ = words.next();
            let Some(partition) = words.next() else {
                return "ERR missing-partition".to_owned();
            };
            let Some(key) = words.next() else {
                return "ERR missing-key".to_owned();
            };
            let Some(value) = words.next() else {
                return "ERR missing-value".to_owned();
            };
            match state.role(partition).as_deref() {
                Some("STANDBY") | Some("LEADER") => {
                    state.put_local(partition, key, value);
                    "OK".to_owned()
                }
                _ => "ERR not-replica".to_owned(),
            }
        }
        Some("DUMP") => {
            let Some(partition) = words.next() else {
                return "ERR missing-partition".to_owned();
            };
            match state.role(partition).as_deref() {
                Some("STANDBY") | Some("LEADER") => {
                    format!(
                        "DUMP {}",
                        serde_json::to_string(&state.partition_snapshot(partition)).unwrap()
                    )
                }
                _ => "ERR not-replica".to_owned(),
            }
        }
        _ => "ERR unknown-command".to_owned(),
    }
}

fn send(address: &str, command: &str) -> Option<String> {
    let stream = TcpStream::connect(address).ok()?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .ok()?;
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(2)))
        .ok()?;
    let mut writer = BufWriter::new(stream.try_clone().ok()?);
    writeln!(writer, "{command}").ok()?;
    writer.flush().ok()?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).ok()?;
    Some(response.trim_end().to_owned())
}

pub fn request_at(address: &str, command: &str) -> Result<String, String> {
    send(address, command).ok_or_else(|| format!("unable to contact cache node at {address}"))
}

pub fn env_required(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("missing environment variable {name}"))
}

pub fn parse_u64(value: Option<String>, name: &str, default: u64) -> Result<u64, String> {
    value
        .unwrap_or_else(|| default.to_string())
        .parse()
        .map_err(|_| format!("{name} must be an integer"))
}

pub fn endpoint_config() -> Result<(String, String, String), String> {
    Ok((
        env_required("CACHE_ETCD_ENDPOINT")?,
        env_required("CACHE_PREFIX")?,
        env_required("CACHE_CLUSTER")?,
    ))
}
