//! A tiny replicated key/value data plane driven by Clustodian's
//! LeaderStandby state model.

use clustodian::{ResourceHandler, ResourceTransition, TransitionContext, TransitionError};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// The application state held by one demo node.
#[derive(Clone)]
pub struct NodeState {
    instance: String,
    inner: Arc<Mutex<NodeStateInner>>,
}

#[derive(Default)]
struct NodeStateInner {
    role: String,
    values: BTreeMap<String, String>,
}

impl NodeState {
    /// Construct a node in the state-model's initial OFFLINE role.
    pub fn new(instance: impl Into<String>) -> Self {
        Self {
            instance: instance.into(),
            inner: Arc::new(Mutex::new(NodeStateInner {
                role: String::from("OFFLINE"),
                values: BTreeMap::new(),
            })),
        }
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }

    pub fn role(&self) -> String {
        self.inner
            .lock()
            .expect("node state lock is not poisoned")
            .role
            .clone()
    }

    pub fn set_role(&self, role: &str) {
        self.inner
            .lock()
            .expect("node state lock is not poisoned")
            .role = role.to_owned();
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.inner
            .lock()
            .expect("node state lock is not poisoned")
            .values
            .get(key)
            .cloned()
    }

    pub fn put_replica(&self, key: impl Into<String>, value: impl Into<String>) {
        self.inner
            .lock()
            .expect("node state lock is not poisoned")
            .values
            .insert(key.into(), value.into());
    }
}

/// Transition handler that makes role changes visible to the data plane.
pub struct KvTransitionHandler {
    state: NodeState,
}

impl KvTransitionHandler {
    pub fn new(state: NodeState) -> Self {
        Self { state }
    }
}

impl ResourceHandler for KvTransitionHandler {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        if transition.partition().as_str() != "kv_0" {
            return Err(TransitionError::new("unexpected replicated-kv partition"));
        }
        let target = transition.target();
        self.state.set_role(target.as_str());
        Ok(())
    }
}

/// Return a deterministic partition for future extension to multiple shards.
pub fn partition_for_key(_key: &str) -> &'static str {
    "kv_0"
}

#[cfg(test)]
mod tests {
    use super::NodeState;

    #[test]
    fn replica_values_are_visible_after_role_changes() {
        let state = NodeState::new("node-a");
        assert_eq!(state.role(), "OFFLINE");
        state.set_role("STANDBY");
        state.put_replica("greeting", "hello");
        assert_eq!(state.role(), "STANDBY");
        assert_eq!(state.get("greeting").as_deref(), Some("hello"));
    }
}
