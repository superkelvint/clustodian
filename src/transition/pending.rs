use crate::model::{InstanceId, PartitionId, State};

/// A semantic transition already in flight for a replica.
///
/// M5 models only the state information consumed by Helix's message
/// selection stage.  Message identity, sessions, and delivery metadata are
/// deliberately outside this type.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PendingTransition {
    partition: PartitionId,
    instance: InstanceId,
    source_state: State,
    target_state: State,
}

impl PendingTransition {
    /// Construct a pending state transition.
    pub fn new(
        partition: PartitionId,
        instance: InstanceId,
        source_state: State,
        target_state: State,
    ) -> Self {
        Self {
            partition,
            instance,
            source_state,
            target_state,
        }
    }

    /// Return the partition whose replica is transitioning.
    pub fn partition(&self) -> &PartitionId {
        &self.partition
    }

    /// Return the transitioning instance.
    pub fn instance(&self) -> &InstanceId {
        &self.instance
    }

    /// Return the pending transition's source state.
    pub fn source_state(&self) -> &State {
        &self.source_state
    }

    /// Return the source state using the established transition terminology.
    pub fn from(&self) -> &State {
        self.source_state()
    }

    /// Return the pending transition's destination state.
    pub fn target_state(&self) -> &State {
        &self.target_state
    }

    /// Return the destination state using the established transition terminology.
    pub fn to(&self) -> &State {
        self.target_state()
    }
}

#[cfg(test)]
mod tests {
    use super::PendingTransition;
    use crate::model::{InstanceId, PartitionId, State};

    #[test]
    fn exposes_pending_transition_identities_and_aliases() {
        let pending = PendingTransition::new(
            PartitionId::new("p0").unwrap(),
            InstanceId::new("node-a").unwrap(),
            State::new("OFFLINE").unwrap(),
            State::new("STANDBY").unwrap(),
        );
        assert_eq!(pending.partition().as_str(), "p0");
        assert_eq!(pending.instance().as_str(), "node-a");
        assert_eq!(pending.source_state().as_str(), "OFFLINE");
        assert_eq!(pending.from().as_str(), "OFFLINE");
        assert_eq!(pending.target_state().as_str(), "STANDBY");
        assert_eq!(pending.to().as_str(), "STANDBY");
    }
}
