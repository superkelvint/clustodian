//! A deliberately small replicated database data plane coordinated by Clustodian.

use clustodian::{
    ResourceHandler, ResourceState, ResourceTransition, TransitionContext, TransitionError,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

pub const RESOURCE: &str = "zoned-db";
pub const PARTITION_COUNT: usize = 6;
pub const REPLICA_COUNT: usize = 3;

#[derive(Clone, Default)]
pub struct DbState {
    roles: Arc<Mutex<BTreeMap<String, String>>>,
    values: Arc<Mutex<BTreeMap<String, BTreeMap<String, String>>>>,
}

impl DbState {
    pub fn role(&self, partition: &str) -> Option<String> {
        self.roles.lock().unwrap().get(partition).cloned()
    }
    pub fn put_local(&self, partition: &str, key: &str, value: &str) {
        self.values
            .lock()
            .unwrap()
            .entry(partition.to_owned())
            .or_default()
            .insert(key.to_owned(), value.to_owned());
    }
    pub fn get_local(&self, partition: &str, key: &str) -> Option<String> {
        self.values
            .lock()
            .unwrap()
            .get(partition)
            .and_then(|m| m.get(key).cloned())
    }
    pub fn snapshot(&self, partition: &str) -> Vec<(String, String)> {
        self.values
            .lock()
            .unwrap()
            .get(partition)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    fn replace_snapshot(&self, partition: &str, values: BTreeMap<String, String>) {
        self.values
            .lock()
            .unwrap()
            .insert(partition.to_owned(), values);
    }
    pub fn apply_role(&self, partition: &str, role: &str) {
        if role == "DROPPED" {
            self.roles.lock().unwrap().remove(partition);
        } else {
            self.values
                .lock()
                .unwrap()
                .entry(partition.to_owned())
                .or_default();
            self.roles
                .lock()
                .unwrap()
                .insert(partition.to_owned(), role.to_owned());
        }
    }
    pub fn roles(&self) -> BTreeMap<String, String> {
        self.roles.lock().unwrap().clone()
    }
}

pub struct DbHandler {
    state: DbState,
    peers: BTreeMap<String, String>,
    instance: String,
}

impl DbHandler {
    pub fn new(
        state: DbState,
        instance: impl Into<String>,
        peers: BTreeMap<String, String>,
    ) -> Self {
        Self {
            state,
            instance: instance.into(),
            peers,
        }
    }

    fn sync_partition(&self, partition: &str) -> Result<bool, TransitionError> {
        let mut inactive_peers = 0;
        let mut unavailable_peer = false;
        for address in self.peers.values() {
            let Some(response) = send(address, &format!("SYNC {partition}")) else {
                unavailable_peer = true;
                continue;
            };
            if response == "ERR not-replica" {
                inactive_peers += 1;
                continue;
            }
            let Some(payload) = response.strip_prefix("SNAPSHOT ") else {
                return Err(TransitionError::new(format!(
                    "cannot activate database partition {partition}: peer returned an invalid snapshot"
                )));
            };
            let Ok(values) = serde_json::from_str::<BTreeMap<String, String>>(payload) else {
                return Err(TransitionError::new(format!(
                    "cannot activate database partition {partition}: peer returned an invalid snapshot"
                )));
            };
            self.state.replace_snapshot(partition, values);
            return Ok(false);
        }
        Ok(unavailable_peer || inactive_peers == self.peers.len())
    }
}

impl ResourceHandler for DbHandler {
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
            // A fresh process must sync before becoming active. A standby
            // promotion uses its already synced local copy.
            let no_active_peer = self.sync_partition(&partition)?;
            if no_active_peer
                && !(matches!(target, ResourceState::Standby)
                    && transition.source() == ResourceState::Offline)
            {
                return Err(TransitionError::new(format!(
                    "cannot activate database partition {partition}: no active replica supplied a snapshot"
                )));
            }
        }
        self.state.apply_role(&partition, target.as_str());
        let _ = &self.instance;
        Ok(())
    }
}

pub fn partition_for_key(key: &str) -> String {
    let mut hash = 2_166_136_261_u32;
    for byte in key.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    format!("{RESOURCE}_{}", hash as usize % PARTITION_COUNT)
}

pub fn parse_nodes(value: &str) -> Result<BTreeMap<String, (String, String)>, String> {
    value
        .split(',')
        .filter(|v| !v.trim().is_empty())
        .map(|entry| {
            let (instance, rest) = entry
                .split_once('=')
                .ok_or_else(|| format!("node must be INSTANCE=ZONE@ADDRESS: {entry}"))?;
            let (zone, address) = rest
                .split_once('@')
                .ok_or_else(|| format!("node must be INSTANCE=ZONE@ADDRESS: {entry}"))?;
            if instance.is_empty() || zone.is_empty() || address.is_empty() {
                return Err(format!("invalid node: {entry}"));
            }
            Ok((instance.to_owned(), (zone.to_owned(), address.to_owned())))
        })
        .collect()
}

pub fn address_map(nodes: &BTreeMap<String, (String, String)>) -> BTreeMap<String, String> {
    nodes
        .iter()
        .map(|(i, (_, a))| (i.clone(), a.clone()))
        .collect()
}

pub fn spawn_server(
    address: &str,
    state: DbState,
    peers: BTreeMap<String, String>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(address)?;
    let peers: BTreeMap<String, String> = peers
        .into_iter()
        .filter(|(_, peer_address)| peer_address != address)
        .collect();
    thread::Builder::new()
        .name("db-listener".into())
        .spawn(move || {
            for stream in listener.incoming().flatten() {
                let state = state.clone();
                let peers = peers.clone();
                let _ = thread::spawn(move || handle_client(stream, state, peers));
            }
        })?;
    Ok(())
}

fn handle_client(stream: TcpStream, state: DbState, peers: BTreeMap<String, String>) {
    let reader = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut reader = BufReader::new(reader);
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

fn request(line: &str, state: &DbState, peers: &BTreeMap<String, String>) -> String {
    let mut words = line.splitn(3, ' ');
    match words.next() {
        Some("PING") => "PONG".into(),
        Some("ROLE") => serde_json::to_string(&state.roles()).unwrap_or_else(|_| "{}".into()),
        Some("SYNC") => {
            let Some(partition) = words.next() else {
                return "ERR missing-partition".into();
            };
            match state.role(partition).as_deref() {
                Some("LEADER") | Some("STANDBY") => {
                    let snapshot = state
                        .snapshot(partition)
                        .into_iter()
                        .collect::<BTreeMap<_, _>>();
                    format!(
                        "SNAPSHOT {}",
                        serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".into())
                    )
                }
                _ => "ERR not-replica".into(),
            }
        }
        Some("GET") => {
            let Some(key) = words.next() else {
                return "ERR missing-key".into();
            };
            let partition = partition_for_key(key);
            let roles = state.roles.lock().unwrap();
            if !matches!(
                roles.get(&partition).map(String::as_str),
                Some("LEADER" | "STANDBY")
            ) {
                return "ERR not-replica".into();
            }
            match state.get_local(&partition, key) {
                Some(v) => format!("VALUE {v}"),
                None => "NOT_FOUND".into(),
            }
        }
        Some("PUT") => {
            let Some(key) = words.next() else {
                return "ERR missing-key".into();
            };
            let Some(value) = words.next() else {
                return "ERR missing-value".into();
            };
            let partition = partition_for_key(key);
            // Hold the role lock through replication and the local write so a
            // demotion cannot leave this process with an un-fenced commit.
            let roles = state.roles.lock().unwrap();
            if roles.get(&partition).map(String::as_str) != Some("LEADER") {
                return "ERR not-leader".into();
            }
            let mut acknowledgements = 0;
            for address in peers.values() {
                if send(address, &format!("REPLICATE {partition} {key} {value}")).as_deref()
                    == Some("OK")
                {
                    acknowledgements += 1;
                }
            }
            if acknowledgements == 0 {
                return "ERR no-replica".into();
            }
            state.put_local(&partition, key, value);
            "OK".into()
        }
        Some("REPLICATE") => {
            let mut p = line.splitn(4, ' ');
            let _ = p.next();
            let Some(partition) = p.next() else {
                return "ERR missing-partition".into();
            };
            let Some(key) = p.next() else {
                return "ERR missing-key".into();
            };
            let Some(value) = p.next() else {
                return "ERR missing-value".into();
            };
            match state.role(partition).as_deref() {
                Some("LEADER") | Some("STANDBY") => {
                    state.put_local(partition, key, value);
                    "OK".into()
                }
                _ => "ERR not-replica".into(),
            }
        }
        _ => "ERR unknown-command".into(),
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
    send(address, command).ok_or_else(|| format!("unable to contact {address}"))
}

pub fn endpoint_config() -> Result<(String, String, String), String> {
    Ok((
        std::env::var("ZONEDB_ETCD_ENDPOINT")
            .map_err(|_| "missing ZONEDB_ETCD_ENDPOINT".to_owned())?,
        std::env::var("ZONEDB_PREFIX").map_err(|_| "missing ZONEDB_PREFIX".to_owned())?,
        std::env::var("ZONEDB_CLUSTER").map_err(|_| "missing ZONEDB_CLUSTER".to_owned())?,
    ))
}

pub fn configured_instances(nodes: &BTreeMap<String, (String, String)>) -> BTreeSet<String> {
    nodes.keys().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn routing_is_stable() {
        assert_eq!(
            partition_for_key("customer-1"),
            partition_for_key("customer-1")
        );
    }
    #[test]
    fn topology_nodes_parse() {
        let n = parse_nodes("a=zone-a@127.0.0.1:1,b=zone-b@127.0.0.1:2").unwrap();
        assert_eq!(n["a"].0, "zone-a");
        assert_eq!(n["b"].1, "127.0.0.1:2");
    }

    #[test]
    fn snapshots_are_json_encoded_and_reads_require_replica_ownership() {
        let state = DbState::default();
        let peers = BTreeMap::new();
        let partition = "zoned-db_0";
        state.apply_role(partition, "LEADER");
        state.put_local(partition, "key=with;delimiters", "value=with;delimiters");

        let response = request(&format!("SYNC {partition}"), &state, &peers);
        let payload = response.strip_prefix("SNAPSHOT ").unwrap();
        let snapshot = serde_json::from_str::<BTreeMap<String, String>>(payload).unwrap();
        assert_eq!(snapshot["key=with;delimiters"], "value=with;delimiters");

        state.apply_role(partition, "OFFLINE");
        assert_eq!(
            request("GET key=with;delimiters", &state, &peers),
            "ERR not-replica"
        );
    }
}
