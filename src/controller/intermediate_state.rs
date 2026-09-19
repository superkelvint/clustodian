use crate::model::{InstanceId, PartitionId, ResourceId, State};
use std::collections::BTreeMap;

/// The state map published by the supported intermediate-state calculation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IntermediateState {
    states: BTreeMap<ResourceId, BTreeMap<PartitionId, BTreeMap<InstanceId, State>>>,
}

impl IntermediateState {
    pub(crate) fn from_states(
        states: BTreeMap<ResourceId, BTreeMap<PartitionId, BTreeMap<InstanceId, State>>>,
    ) -> Self {
        Self { states }
    }

    /// Look up an intermediate replica state.
    pub fn state(
        &self,
        resource: &ResourceId,
        partition: &PartitionId,
        instance: &InstanceId,
    ) -> Option<&State> {
        self.states.get(resource)?.get(partition)?.get(instance)
    }

    /// Return the deterministic resource/partition/instance state map.
    pub fn entries(
        &self,
    ) -> &BTreeMap<ResourceId, BTreeMap<PartitionId, BTreeMap<InstanceId, State>>> {
        &self.states
    }
}

#[cfg(test)]
mod tests {
    use super::IntermediateState;
    use crate::model::{InstanceId, PartitionId, ResourceId, State};
    use std::collections::BTreeMap;

    #[test]
    fn looks_up_and_exposes_deterministic_entries() {
        let resource = ResourceId::new("documents").unwrap();
        let partition = PartitionId::new("p0").unwrap();
        let instance = InstanceId::new("node-a").unwrap();
        let state = State::new("STANDBY").unwrap();
        let entries = BTreeMap::from([(
            resource.clone(),
            BTreeMap::from([(
                partition.clone(),
                BTreeMap::from([(instance.clone(), state.clone())]),
            )]),
        )]);
        let intermediate = IntermediateState::from_states(entries);
        assert_eq!(
            intermediate.state(&resource, &partition, &instance),
            Some(&state)
        );
        assert!(intermediate
            .state(&resource, &PartitionId::new("missing").unwrap(), &instance)
            .is_none());
        assert_eq!(intermediate.entries().len(), 1);
    }
}
