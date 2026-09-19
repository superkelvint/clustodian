use crate::model::{InstanceId, PartitionId, ResourceId, State};

/// A semantic request for one replica to take its next state-model transition.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransitionRequest {
    resource: ResourceId,
    partition: PartitionId,
    instance: InstanceId,
    source_state: State,
    target_state: State,
}

impl TransitionRequest {
    /// Construct a semantic transition request.
    pub fn new(
        resource: ResourceId,
        partition: PartitionId,
        instance: InstanceId,
        source_state: State,
        target_state: State,
    ) -> Self {
        Self {
            resource,
            partition,
            instance,
            source_state,
            target_state,
        }
    }

    /// Return the resource identity.
    pub fn resource(&self) -> &ResourceId {
        &self.resource
    }

    /// Return the partition identity.
    pub fn partition(&self) -> &PartitionId {
        &self.partition
    }

    /// Return the target instance identity.
    pub fn instance(&self) -> &InstanceId {
        &self.instance
    }

    /// Return the observed state.
    pub fn source_state(&self) -> &State {
        &self.source_state
    }

    /// Return the observed state using the established transition terminology.
    pub fn from(&self) -> &State {
        self.source_state()
    }

    /// Return the observed source state using the explicit request terminology.
    pub fn from_state(&self) -> &State {
        self.source_state()
    }

    /// Return the next state-model state.
    pub fn target_state(&self) -> &State {
        &self.target_state
    }

    /// Return the next state using the established transition terminology.
    pub fn to(&self) -> &State {
        self.target_state()
    }

    /// Return the target state using the explicit request terminology.
    pub fn to_state(&self) -> &State {
        self.target_state()
    }
}

#[cfg(test)]
mod tests {
    use super::TransitionRequest;
    use crate::model::{InstanceId, PartitionId, ResourceId, State};

    #[test]
    fn exposes_request_identities_and_state_aliases() {
        let request = TransitionRequest::new(
            ResourceId::new("documents").unwrap(),
            PartitionId::new("p0").unwrap(),
            InstanceId::new("node-a").unwrap(),
            State::new("OFFLINE").unwrap(),
            State::new("STANDBY").unwrap(),
        );
        assert_eq!(request.resource().as_str(), "documents");
        assert_eq!(request.partition().as_str(), "p0");
        assert_eq!(request.instance().as_str(), "node-a");
        assert_eq!(request.source_state().as_str(), "OFFLINE");
        assert_eq!(request.from().as_str(), "OFFLINE");
        assert_eq!(request.from_state().as_str(), "OFFLINE");
        assert_eq!(request.target_state().as_str(), "STANDBY");
        assert_eq!(request.to().as_str(), "STANDBY");
        assert_eq!(request.to_state().as_str(), "STANDBY");
    }
}
