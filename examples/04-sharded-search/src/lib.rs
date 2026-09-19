//! Application-side state for the sharded-search showcase.
//!
//! Clustodian owns membership, placement, and state transitions.  This module
//! only records which partitions this process has been asked to host so that
//! the participant transitions are visible in the demo.

use clustodian::model::{PartitionId, State};
use clustodian::{ResourceHandler, ResourceTransition, TransitionContext, TransitionError};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

pub const RESOURCE: &str = "search";
pub const PARTITION_COUNT: usize = 24;
pub const REPLICA_COUNT: usize = 2;

#[derive(Clone, Default)]
pub struct SearchNodeState {
    states: Arc<Mutex<BTreeMap<PartitionId, State>>>,
}

impl SearchNodeState {
    pub fn state(&self, partition: &PartitionId) -> Option<State> {
        self.states
            .lock()
            .expect("search node state lock is not poisoned")
            .get(partition)
            .cloned()
    }

    pub fn hosted_partitions(&self) -> BTreeMap<String, String> {
        self.states
            .lock()
            .expect("search node state lock is not poisoned")
            .iter()
            .map(|(partition, state)| (partition.to_string(), state.to_string()))
            .collect()
    }
}

impl ResourceHandler for SearchNodeState {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        let mut states = self
            .states
            .lock()
            .map_err(|_| TransitionError::new("search node state lock is poisoned"))?;
        if matches!(transition.target(), clustodian::ResourceState::Dropped) {
            states.remove(transition.partition());
        } else {
            states.insert(
                transition.partition().clone(),
                clustodian::model::State::try_from(transition.target().as_str())
                    .expect("facade state is valid"),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SearchNodeState;

    #[test]
    fn new_search_node_has_no_hosted_partitions() {
        let state = SearchNodeState::default();
        assert!(state.hosted_partitions().is_empty());
    }
}
