//! Application-side state for the game-world allocator example.
//!
//! Clustodian owns membership, placement, and state transitions. This module
//! owns only the deliberately small game-server data plane: a process-local
//! registry of worlds and whether each world is active or standing by.

use clustodian::{
    ResourceHandler, ResourceState, ResourceTransition, TransitionContext, TransitionError,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

pub const RESOURCE: &str = "game-worlds";
pub const WORLD_COUNT: usize = 6;
pub const REPLICA_COUNT: usize = 2;
pub const PARTICIPANT_LEASE_TTL_SECONDS: u64 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorldRole {
    Active,
    Standby,
}

impl WorldRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Standby => "STANDBY",
        }
    }
}

/// A transition handler suitable for a game-server participant.
///
/// The registry is intentionally process-local. A real game server would
/// load/save world state in its own data plane; this sample focuses on the
/// ownership contract exposed by Clustodian.
#[derive(Clone)]
pub struct GameServerHandler {
    instance: String,
    worlds: Arc<Mutex<BTreeMap<String, WorldRole>>>,
}

impl GameServerHandler {
    pub fn new(instance: impl Into<String>) -> Self {
        Self {
            instance: instance.into(),
            worlds: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }

    pub fn worlds(&self) -> BTreeMap<String, WorldRole> {
        self.worlds
            .lock()
            .expect("game-world registry is not poisoned")
            .clone()
    }
}

impl ResourceHandler for GameServerHandler {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        let partition = transition.partition().to_string();
        let target = transition.target();
        let mut worlds = self
            .worlds
            .lock()
            .map_err(|_| TransitionError::new("game-world registry is poisoned"))?;
        if matches!(target, ResourceState::Leader) {
            worlds.insert(partition.clone(), WorldRole::Active);
        } else if matches!(target, ResourceState::Standby) {
            worlds.insert(partition.clone(), WorldRole::Standby);
        } else if matches!(target, ResourceState::Offline | ResourceState::Dropped) {
            worlds.remove(&partition);
        } else {
            return Err(TransitionError::new(format!(
                "unsupported game-world target state {target}"
            )));
        }
        println!(
            "server={} world={} {} -> {} role={}",
            self.instance,
            partition,
            transition.source(),
            target,
            worlds
                .get(&partition)
                .map_or("OFFLINE", |role| role.as_str())
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants_describe_the_demo_shape() {
        assert_eq!(RESOURCE, "game-worlds");
        assert_eq!(WORLD_COUNT, 6);
        assert_eq!(REPLICA_COUNT, 2);
    }
}
