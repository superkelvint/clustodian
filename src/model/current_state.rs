use super::replica_state::{ReplicaStateError, ReplicaStates};
use super::{InstanceId, PartitionId, State};
use std::collections::BTreeMap;

/// Immutable observed replica states for one or more partitions.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CurrentState {
    states: ReplicaStates,
}

impl CurrentState {
    /// Start constructing an observed-state map.
    pub fn builder() -> CurrentStateBuilder {
        CurrentStateBuilder::default()
    }

    /// Look up an observed replica state.
    pub fn state(&self, partition: &PartitionId, instance: &InstanceId) -> Option<&State> {
        self.states.get(partition, instance)
    }

    pub fn entries(&self) -> &BTreeMap<PartitionId, BTreeMap<InstanceId, State>> {
        self.states.entries()
    }

    pub(crate) fn into_entries(self) -> BTreeMap<PartitionId, BTreeMap<InstanceId, State>> {
        self.states.into_entries()
    }
}

/// Builder for an immutable [`CurrentState`].
#[derive(Clone, Debug, Default)]
pub struct CurrentStateBuilder {
    states: ReplicaStates,
}

impl CurrentStateBuilder {
    /// Add one observed replica state.
    pub fn set_state(
        &mut self,
        partition: PartitionId,
        instance: InstanceId,
        state: State,
    ) -> Result<&mut Self, ReplicaStateError> {
        self.states.insert(partition, instance, state)?;
        Ok(self)
    }

    /// Publish the immutable observed-state map.
    pub fn build(self) -> CurrentState {
        CurrentState {
            states: self.states,
        }
    }
}
