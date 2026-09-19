use super::{InstanceId, PartitionId, State};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaStateError {
    Duplicate {
        partition: PartitionId,
        instance: InstanceId,
    },
}

impl fmt::Display for ReplicaStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate {
                partition,
                instance,
            } => write!(
                formatter,
                "duplicate state for partition {partition} and instance {instance}"
            ),
        }
    }
}

impl std::error::Error for ReplicaStateError {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReplicaStates {
    states: BTreeMap<PartitionId, BTreeMap<InstanceId, State>>,
}

impl ReplicaStates {
    pub(crate) fn insert(
        &mut self,
        partition: PartitionId,
        instance: InstanceId,
        state: State,
    ) -> Result<(), ReplicaStateError> {
        let instances = self.states.entry(partition.clone()).or_default();
        if instances.insert(instance.clone(), state).is_some() {
            return Err(ReplicaStateError::Duplicate {
                partition,
                instance,
            });
        }
        Ok(())
    }

    pub(crate) fn get(&self, partition: &PartitionId, instance: &InstanceId) -> Option<&State> {
        self.states.get(partition)?.get(instance)
    }

    pub(crate) fn entries(&self) -> &BTreeMap<PartitionId, BTreeMap<InstanceId, State>> {
        &self.states
    }

    pub(crate) fn into_entries(self) -> BTreeMap<PartitionId, BTreeMap<InstanceId, State>> {
        self.states
    }
}

#[cfg(test)]
mod tests {
    use super::{ReplicaStateError, ReplicaStates};
    use crate::model::{InstanceId, PartitionId, State};

    #[test]
    fn stores_and_rejects_duplicate_replica_states() {
        let partition = PartitionId::new("p0").unwrap();
        let instance = InstanceId::new("node-a").unwrap();
        let state = State::new("LEADER").unwrap();
        let mut states = ReplicaStates::default();
        states
            .insert(partition.clone(), instance.clone(), state.clone())
            .unwrap();
        assert_eq!(states.get(&partition, &instance), Some(&state));
        assert_eq!(states.entries().len(), 1);
        assert!(matches!(
            states.insert(partition.clone(), instance.clone(), state),
            Err(ReplicaStateError::Duplicate { .. })
        ));
        assert!(states
            .get(&PartitionId::new("missing").unwrap(), &instance)
            .is_none());
        assert_eq!(
            ReplicaStateError::Duplicate {
                partition,
                instance
            }
            .to_string(),
            "duplicate state for partition p0 and instance node-a"
        );
        assert_eq!(states.into_entries().len(), 1);
    }
}
