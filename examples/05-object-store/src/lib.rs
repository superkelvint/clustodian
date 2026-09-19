//! A small object-range coordinator backed by Clustodian.
//!
//! The object bytes live in process-local memory.  Clustodian owns membership,
//! topology-aware placement, and role transitions; the demo protocol performs
//! synchronous replication between the active replicas.

use clustodian::{
    ResourceHandler, ResourceState, ResourceTransition, TransitionContext, TransitionError,
};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

pub const RESOURCE: &str = "objects";
pub const PARTITION_COUNT: usize = 12;
pub const REPLICATION_FACTOR: usize = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeEndpoint {
    pub address: String,
    pub zone: String,
}

/// Parse `instance@zone=host:port,...` configuration used by setup and the demo.
pub fn parse_nodes(value: &str) -> Result<BTreeMap<String, NodeEndpoint>, String> {
    value
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (identity, address) = entry
                .split_once('=')
                .ok_or_else(|| format!("node must be INSTANCE@ZONE=ADDRESS: {entry}"))?;
            let (instance, zone) = identity
                .split_once('@')
                .ok_or_else(|| format!("node must be INSTANCE@ZONE=ADDRESS: {entry}"))?;
            if instance.is_empty() || zone.is_empty() || address.is_empty() {
                return Err(format!("node has an empty field: {entry}"));
            }
            Ok((
                instance.to_owned(),
                NodeEndpoint {
                    address: address.to_owned(),
                    zone: zone.to_owned(),
                },
            ))
        })
        .collect()
}

pub fn parse_addresses(value: &str) -> Result<BTreeMap<String, String>, String> {
    value
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (name, address) = entry
                .split_once('=')
                .ok_or_else(|| format!("peer must be INSTANCE=ADDRESS: {entry}"))?;
            if name.is_empty() || address.is_empty() {
                return Err(format!("peer has an empty field: {entry}"));
            }
            Ok((name.to_owned(), address.to_owned()))
        })
        .collect()
}

pub fn partition_for_object(object: &str) -> String {
    let mut hash = 2_166_136_261_u32;
    for byte in object.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    format!("{RESOURCE}_{}", hash as usize % PARTITION_COUNT)
}

#[derive(Clone, Default)]
pub struct ObjectState {
    instance: Arc<String>,
    roles: Arc<Mutex<BTreeMap<String, String>>>,
    objects: Arc<Mutex<BTreeMap<String, BTreeMap<String, String>>>>,
}

impl ObjectState {
    pub fn new(instance: impl Into<String>) -> Self {
        Self {
            instance: Arc::new(instance.into()),
            roles: Arc::new(Mutex::new(BTreeMap::new())),
            objects: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn instance(&self) -> &str {
        self.instance.as_str()
    }

    pub fn role(&self, partition: &str) -> Option<String> {
        self.roles
            .lock()
            .expect("roles mutex")
            .get(partition)
            .cloned()
    }

    pub fn roles(&self) -> BTreeMap<String, String> {
        self.roles.lock().expect("roles mutex").clone()
    }

    pub fn get(&self, partition: &str, object: &str) -> Option<String> {
        self.objects
            .lock()
            .expect("objects mutex")
            .get(partition)
            .and_then(|values| values.get(object).cloned())
    }

    pub fn put(&self, partition: &str, object: &str, value: &str) {
        self.objects
            .lock()
            .expect("objects mutex")
            .entry(partition.to_owned())
            .or_default()
            .insert(object.to_owned(), value.to_owned());
    }

    fn dump(&self, partition: &str) -> BTreeMap<String, String> {
        self.objects
            .lock()
            .expect("objects mutex")
            .get(partition)
            .cloned()
            .unwrap_or_default()
    }

    fn replace(&self, partition: &str, values: BTreeMap<String, String>) {
        self.objects
            .lock()
            .expect("objects mutex")
            .insert(partition.to_owned(), values);
    }

    fn set_role(&self, partition: &str, state: &str) {
        if state == "DROPPED" {
            self.roles.lock().expect("roles mutex").remove(partition);
        } else {
            self.roles
                .lock()
                .expect("roles mutex")
                .insert(partition.to_owned(), state.to_owned());
        }
    }

    fn sync_partition(
        &self,
        partition: &str,
        peers: &BTreeMap<String, String>,
    ) -> Result<bool, TransitionError> {
        let mut peer_count = 0;
        let mut inactive_peers = 0;
        let mut unavailable_peer = false;
        for (name, address) in peers {
            if name == self.instance() {
                continue;
            }
            peer_count += 1;
            let Some(response) = request_sync(address, &format!("DUMP {partition}")) else {
                unavailable_peer = true;
                continue;
            };
            if response == "ERR not-replica" {
                inactive_peers += 1;
                continue;
            }
            if let Ok(values) = serde_json::from_str::<BTreeMap<String, String>>(&response) {
                self.replace(partition, values);
                return Ok(false);
            }
            return Err(TransitionError::new(format!(
                "cannot activate object partition {partition}: peer returned an invalid snapshot"
            )));
        }
        Ok(unavailable_peer || inactive_peers == peer_count)
    }
}

pub struct ObjectTransitionHandler {
    state: ObjectState,
    peers: BTreeMap<String, String>,
}

impl ObjectTransitionHandler {
    pub fn new(state: ObjectState, peers: BTreeMap<String, String>) -> Self {
        Self { state, peers }
    }
}

impl ResourceHandler for ObjectTransitionHandler {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        let partition = transition.partition().to_string();
        let target = transition.target();
        if matches!(target, ResourceState::Standby)
            || (matches!(target, ResourceState::Leader)
                && matches!(transition.source(), ResourceState::Offline))
        {
            // State transfer completes before a fresh process advertises the
            // active role. A standby promotion uses its already synced copy.
            let no_active_peer = self.state.sync_partition(&partition, &self.peers)?;
            if no_active_peer
                && !(matches!(target, ResourceState::Standby)
                    && transition.source() == ResourceState::Offline)
            {
                return Err(TransitionError::new(format!(
                    "cannot activate object partition {partition}: no active replica supplied a snapshot"
                )));
            }
        }
        self.state.set_role(&partition, target.as_str());
        Ok(())
    }
}

pub fn serve(
    address: &str,
    state: ObjectState,
    peers: BTreeMap<String, String>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(address)?;
    thread::Builder::new()
        .name("object-store-listener".to_owned())
        .spawn(move || {
            for stream in listener.incoming().flatten() {
                let state = state.clone();
                let peers = peers.clone();
                let _ = thread::Builder::new()
                    .name("object-store-client".to_owned())
                    .spawn(move || handle_client(stream, state, peers));
            }
        })?;
    Ok(())
}

fn handle_client(stream: TcpStream, state: ObjectState, peers: BTreeMap<String, String>) {
    let reader_stream = match stream.try_clone() {
        Ok(stream) => stream,
        Err(_) => return,
    };
    let mut reader = BufReader::new(reader_stream);
    let mut writer = stream;
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

fn request(line: &str, state: &ObjectState, peers: &BTreeMap<String, String>) -> String {
    let mut fields = line.splitn(3, ' ');
    match fields.next().unwrap_or_default() {
        "PING" => "PONG".to_owned(),
        "STATUS" => {
            serde_json::json!({"instance": state.instance(), "roles": state.roles()}).to_string()
        }
        "DUMP" => {
            let Some(partition) = fields.next() else {
                return "ERR missing-partition".to_owned();
            };
            match state.role(partition).as_deref() {
                Some("LEADER") | Some("STANDBY") => serde_json::to_string(&state.dump(partition))
                    .unwrap_or_else(|_| "{}".to_owned()),
                _ => "ERR not-replica".to_owned(),
            }
        }
        "GET" => {
            let Some(object) = fields.next() else {
                return "ERR missing-object".to_owned();
            };
            let partition = partition_for_object(object);
            match state.role(&partition).as_deref() {
                Some("LEADER") | Some("STANDBY") => match state.get(&partition, object) {
                    Some(value) => format!("VALUE {value}"),
                    None => "NOT_FOUND".to_owned(),
                },
                _ => "ERR not-replica".to_owned(),
            }
        }
        "PUT" => {
            let Some(object) = fields.next() else {
                return "ERR missing-object".to_owned();
            };
            let Some(value) = fields.next() else {
                return "ERR missing-value".to_owned();
            };
            let partition = partition_for_object(object);
            // Serialize the complete write with the role transition. If the
            // demotion acquires the role lock first this request is rejected;
            // otherwise the write linearizes before the demotion.
            let roles = state.roles.lock().expect("roles mutex");
            if roles.get(&partition).map(String::as_str) != Some("LEADER") {
                return "ERR not-leader".to_owned();
            }
            let replicated = peers
                .iter()
                .filter(|(name, _)| name.as_str() != state.instance())
                .map(|(_, address)| address)
                .filter(|address| {
                    request_sync(address, &format!("REPLICATE {partition} {object} {value}"))
                        == Some(String::from("OK"))
                })
                .count();
            if replicated == 0 {
                return "ERR no-surviving-standby".to_owned();
            }
            state.put(&partition, object, value);
            format!("OK partition={partition} replicas={}", replicated + 1)
        }
        "REPLICATE" => {
            let mut values = line.splitn(4, ' ');
            let _ = values.next();
            let (Some(partition), Some(object), Some(value)) =
                (values.next(), values.next(), values.next())
            else {
                return "ERR usage: REPLICATE PARTITION OBJECT VALUE".to_owned();
            };
            match state.role(partition).as_deref() {
                Some("LEADER") | Some("STANDBY") => {
                    state.put(partition, object, value);
                    "OK".to_owned()
                }
                _ => "ERR not-replica".to_owned(),
            }
        }
        _ => "ERR unknown-command".to_owned(),
    }
}

pub fn request_at(address: &str, command: &str) -> Option<String> {
    request_sync(address, command)
}

fn request_sync(address: &str, command: &str) -> Option<String> {
    let mut stream = TcpStream::connect(address).ok()?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .ok()?;
    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(2)))
        .ok()?;
    writeln!(stream, "{command}").ok()?;
    stream.flush().ok()?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).ok()?;
    Some(response.trim_end().to_owned())
}

pub fn required(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is required"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_node_syntax_and_hash_are_deterministic() {
        let nodes = parse_nodes("a@zone-a=127.0.0.1:1,b@zone-b=127.0.0.1:2").unwrap();
        assert_eq!(nodes["a"].zone, "zone-a");
        assert_eq!(
            partition_for_object("photo/1"),
            partition_for_object("photo/1")
        );
    }

    #[test]
    fn transitions_and_local_objects_are_visible() {
        let state = ObjectState::new("a");
        state.set_role("objects_0", "STANDBY");
        state.put("objects_0", "x", "y");
        assert_eq!(state.role("objects_0").as_deref(), Some("STANDBY"));
        assert_eq!(state.get("objects_0", "x").as_deref(), Some("y"));
    }
}
