use clustodian::model::{PartitionId, State};
use clustodian::observe::{ClusterSnapshot, Snapshot};
use clustodian::{
    ResourceHandler, ResourceState, ResourceTransition, TransitionContext, TransitionError,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub const RESOURCE: &str = "chat";
pub const PARTITION_COUNT: usize = 4;
pub const REPLICA_COUNT: usize = 2;

/// The partition assignment used by both the server and the client.
pub fn room_partition(room: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in room.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{RESOURCE}_{}", hash as usize % PARTITION_COUNT)
}

/// Return the currently observed leader for a room, if one is published.
pub fn leader_for_snapshot(
    snapshot: &Snapshot,
    room: &str,
) -> Result<Option<String>, clustodian::routing::RoutingError> {
    let partition = room_partition(room);
    Ok(snapshot
        .routing()
        .leader(RESOURCE, partition)?
        .map(|instance| instance.to_string()))
}

pub fn leader_for_room(snapshot: &ClusterSnapshot, room: &str) -> Option<String> {
    let partition = room_partition(room);
    snapshot
        .routing_results
        .iter()
        .find(|result| {
            result.resource == RESOURCE && result.partition == partition && result.state == "LEADER"
        })
        .and_then(|result| result.instances.first().cloned())
}

/// Parse `node-a=127.0.0.1:9001,node-b=...` endpoint configuration.
pub fn parse_endpoints(value: &str) -> Result<HashMap<String, String>, String> {
    value
        .split(',')
        .filter(|entry| !entry.trim().is_empty())
        .map(|entry| {
            let (name, address) = entry
                .split_once('=')
                .ok_or_else(|| format!("endpoint must be name=address: {entry}"))?;
            if name.trim().is_empty() || address.trim().is_empty() {
                return Err(format!("endpoint has an empty name or address: {entry}"));
            }
            Ok((name.trim().to_owned(), address.trim().to_owned()))
        })
        .collect()
}

#[derive(Clone)]
pub struct Ownership {
    states: Arc<Mutex<HashMap<PartitionId, State>>>,
    changes: watch::Sender<Option<PartitionId>>,
}

impl Default for Ownership {
    fn default() -> Self {
        let (changes, _) = watch::channel(None);
        Self {
            states: Arc::new(Mutex::new(HashMap::new())),
            changes,
        }
    }
}

impl Ownership {
    pub fn is_leader(&self, partition: &PartitionId) -> bool {
        self.states
            .lock()
            .expect("ownership mutex is not poisoned")
            .get(partition)
            .is_some_and(State::is_leader)
    }

    pub fn state(&self, partition: &PartitionId) -> Option<String> {
        self.states
            .lock()
            .expect("ownership mutex is not poisoned")
            .get(partition)
            .map(ToString::to_string)
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<PartitionId>> {
        self.changes.subscribe()
    }
}

impl ResourceHandler for Ownership {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        {
            let mut states = self
                .states
                .lock()
                .map_err(|_| TransitionError::new("ownership mutex is poisoned"))?;
            if matches!(transition.target(), ResourceState::Dropped) {
                states.remove(transition.partition());
            } else {
                states.insert(
                    transition.partition().clone(),
                    clustodian::model::State::try_from(transition.target().as_str())
                        .expect("facade state is valid"),
                );
            }
        }
        let _ = self.changes.send(Some(transition.partition().clone()));
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct Welcome<'a> {
    pub r#type: &'static str,
    pub room: &'a str,
    pub owner: &'a str,
    pub partition: String,
}

#[derive(Debug, Serialize)]
pub struct Redirect<'a> {
    pub r#type: &'static str,
    pub room: &'a str,
    pub reason: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ChatMessage<'a> {
    pub r#type: &'static str,
    pub room: &'a str,
    pub from: &'a str,
    pub text: &'a str,
}

pub fn room_from_path(path: &str) -> Option<&str> {
    let room = path.strip_prefix("/room/")?;
    (!room.is_empty() && !room.contains('/')).then_some(room)
}

pub fn json_type(value: &Value) -> Option<&str> {
    value.get("type")?.as_str()
}

#[cfg(test)]
mod tests {
    use super::{leader_for_room, parse_endpoints, room_from_path, room_partition};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn room_hash_is_stable_and_partition_bounded() {
        assert_eq!(room_partition("lobby"), room_partition("lobby"));
        assert!(room_partition("lobby").starts_with("chat_"));
    }

    #[test]
    fn endpoint_and_path_parsing_are_strict() {
        assert_eq!(
            parse_endpoints("a=127.0.0.1:1,b=127.0.0.1:2")
                .unwrap()
                .len(),
            2
        );
        assert!(parse_endpoints("not-an-endpoint").is_err());
        assert_eq!(room_from_path("/room/general"), Some("general"));
        assert_eq!(room_from_path("/room/"), None);
    }

    #[test]
    fn leader_lookup_uses_routing_results() {
        let snapshot = clustodian::observe::ClusterSnapshot {
            observer_revision: clustodian::observe::ObserverRevision::from_value(1),
            authoritative_revision: clustodian::observe::ObserverRevision::from_value(1),
            processed_revision: None,
            controllers: clustodian::observe::ControllerMembership {
                active: vec![],
                standby: vec![],
            },
            live_instances: BTreeMap::new(),
            active_current_state: BTreeMap::new(),
            external_view: json!({}),
            pending_transitions: vec![],
            routing_results: vec![clustodian::observe::RoutingResult {
                resource: "chat".into(),
                partition: room_partition("lobby"),
                state: "LEADER".into(),
                instances: vec!["node-a".into()],
            }],
        };
        assert_eq!(leader_for_room(&snapshot, "lobby"), Some("node-a".into()));
    }
}
