use super::replica_state::{ReplicaStateError, ReplicaStates};
use super::{InstanceId, PartitionId, State};

/// Immutable desired replica states supplied to transition generation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BestPossibleState {
    states: ReplicaStates,
}

impl BestPossibleState {
    /// Start constructing a desired-state map.
    pub fn builder() -> BestPossibleStateBuilder {
        BestPossibleStateBuilder::default()
    }

    /// Look up a desired replica state.
    pub fn state(&self, partition: &PartitionId, instance: &InstanceId) -> Option<&State> {
        self.states.get(partition, instance)
    }

    /// Return the deterministic partition/instance assignment view.
    pub fn entries(
        &self,
    ) -> &std::collections::BTreeMap<PartitionId, std::collections::BTreeMap<InstanceId, State>>
    {
        self.states.entries()
    }
}

/// Builder for an immutable [`BestPossibleState`].
#[derive(Clone, Debug, Default)]
pub struct BestPossibleStateBuilder {
    states: ReplicaStates,
}

impl BestPossibleStateBuilder {
    /// Add one desired replica state.
    pub fn set_state(
        &mut self,
        partition: PartitionId,
        instance: InstanceId,
        state: State,
    ) -> Result<&mut Self, ReplicaStateError> {
        self.states.insert(partition, instance, state)?;
        Ok(self)
    }

    /// Publish the immutable desired-state map.
    pub fn build(self) -> BestPossibleState {
        BestPossibleState {
            states: self.states,
        }
    }
}
