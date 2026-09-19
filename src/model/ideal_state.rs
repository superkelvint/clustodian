use super::{InstanceId, PartitionId, ResourceId};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// An explicit SEMI_AUTO placement specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdealState {
    resource: ResourceId,
    replicas: usize,
    preference_lists: BTreeMap<PartitionId, Vec<InstanceId>>,
}

impl IdealState {
    /// Start constructing an explicit placement specification.
    pub fn builder(resource: ResourceId, replicas: usize) -> IdealStateBuilder {
        IdealStateBuilder {
            resource,
            replicas,
            preference_lists: BTreeMap::new(),
        }
    }

    /// Return the resource covered by this specification.
    pub fn resource(&self) -> &ResourceId {
        &self.resource
    }

    /// Return the configured replica count.
    pub fn replicas(&self) -> usize {
        self.replicas
    }

    /// Return the ordered preference list for a partition.
    pub fn preference_list(&self, partition: &PartitionId) -> Option<&[InstanceId]> {
        self.preference_lists.get(partition).map(Vec::as_slice)
    }

    /// Return all partitions and their ordered preference lists.
    pub fn preference_lists(&self) -> &BTreeMap<PartitionId, Vec<InstanceId>> {
        &self.preference_lists
    }
}

/// Mutable construction state for an immutable [`IdealState`].
#[derive(Clone, Debug)]
pub struct IdealStateBuilder {
    resource: ResourceId,
    replicas: usize,
    preference_lists: BTreeMap<PartitionId, Vec<InstanceId>>,
}

impl IdealStateBuilder {
    /// Add one partition's ordered preference list.
    pub fn set_preference_list(
        &mut self,
        partition: PartitionId,
        instances: Vec<InstanceId>,
    ) -> Result<&mut Self, IdealStateError> {
        if self.preference_lists.contains_key(&partition) {
            return Err(IdealStateError::DuplicatePartition(partition));
        }
        if instances.len() != self.replicas {
            return Err(IdealStateError::PreferenceListReplicaCount {
                partition,
                expected: self.replicas,
                actual: instances.len(),
            });
        }
        let unique_instances = instances.iter().collect::<BTreeSet<_>>();
        if unique_instances.len() != instances.len() {
            return Err(IdealStateError::DuplicatePreferenceInstance { partition });
        }
        self.preference_lists.insert(partition, instances);
        Ok(self)
    }

    /// Publish the immutable placement specification.
    pub fn build(self) -> Result<IdealState, IdealStateError> {
        if self.preference_lists.is_empty() {
            return Err(IdealStateError::NoPartitions);
        }
        Ok(IdealState {
            resource: self.resource,
            replicas: self.replicas,
            preference_lists: self.preference_lists,
        })
    }
}

/// Invalid explicit SEMI_AUTO placement input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdealStateError {
    DuplicatePartition(PartitionId),
    DuplicatePreferenceInstance {
        partition: PartitionId,
    },
    NoPartitions,
    PreferenceListReplicaCount {
        partition: PartitionId,
        expected: usize,
        actual: usize,
    },
}

impl fmt::Display for IdealStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicatePartition(partition) => {
                write!(formatter, "duplicate ideal-state partition: {partition}")
            }
            Self::DuplicatePreferenceInstance { partition } => write!(
                formatter,
                "preference list contains a duplicate instance for partition {partition}"
            ),
            Self::NoPartitions => formatter.write_str("ideal state must contain a partition"),
            Self::PreferenceListReplicaCount {
                partition,
                expected,
                actual,
            } => write!(
                formatter,
                "preference list for partition {partition} has {actual} entries; expected {expected}"
            ),
        }
    }
}

impl std::error::Error for IdealStateError {}

#[cfg(test)]
mod tests {
    use super::{IdealState, IdealStateError};
    use crate::model::{InstanceId, PartitionId, ResourceId};

    fn instance(name: &str) -> InstanceId {
        InstanceId::new(name).expect("valid instance")
    }

    fn partition(name: &str) -> PartitionId {
        PartitionId::new(name).expect("valid partition")
    }

    #[test]
    fn preserves_ordered_preference_lists() {
        let mut builder = IdealState::builder(ResourceId::new("documents").unwrap(), 2);
        builder
            .set_preference_list(
                partition("p0"),
                vec![instance("node-b"), instance("node-a")],
            )
            .unwrap();
        let ideal = builder.build().unwrap();
        assert_eq!(ideal.replicas(), 2);
        assert_eq!(
            ideal.preference_list(&partition("p0")).unwrap()[0],
            instance("node-b")
        );
    }

    #[test]
    fn rejects_duplicate_or_wrong_sized_preference_lists() {
        let mut builder = IdealState::builder(ResourceId::new("documents").unwrap(), 2);
        assert!(matches!(
            builder.set_preference_list(partition("p0"), vec![instance("node-a")]),
            Err(IdealStateError::PreferenceListReplicaCount {
                partition: failed_partition,
                expected: 2,
                actual: 1,
            }) if failed_partition == partition("p0")
        ));
        builder
            .set_preference_list(
                partition("p0"),
                vec![instance("node-a"), instance("node-b")],
            )
            .unwrap();
        assert!(matches!(
            builder.set_preference_list(
                partition("p0"),
                vec![instance("node-a"), instance("node-b")]
            ),
            Err(IdealStateError::DuplicatePartition(_))
        ));
        let mut duplicate = IdealState::builder(ResourceId::new("documents").unwrap(), 2);
        assert!(matches!(
            duplicate.set_preference_list(
                partition("p0"),
                vec![instance("node-a"), instance("node-a")]
            ),
            Err(IdealStateError::DuplicatePreferenceInstance { .. })
        ));
        assert_eq!(
            IdealState::builder(ResourceId::new("documents").unwrap(), 1)
                .build()
                .unwrap_err()
                .to_string(),
            "ideal state must contain a partition"
        );
    }

    #[test]
    fn exposes_resource_and_all_preference_lists() {
        let mut builder = IdealState::builder(ResourceId::new("documents").unwrap(), 1);
        builder
            .set_preference_list(partition("p0"), vec![instance("node-a")])
            .unwrap();
        let ideal = builder.build().unwrap();
        assert_eq!(ideal.resource().as_str(), "documents");
        assert_eq!(ideal.preference_lists().len(), 1);
        assert!(ideal.preference_list(&partition("missing")).is_none());
        assert_eq!(
            IdealStateError::PreferenceListReplicaCount {
                partition: partition("p0"),
                expected: 2,
                actual: 1,
            }
            .to_string(),
            "preference list for partition p0 has 1 entries; expected 2"
        );
        assert_eq!(
            IdealStateError::DuplicatePartition(partition("p0")).to_string(),
            "duplicate ideal-state partition: p0"
        );
        assert_eq!(
            IdealStateError::DuplicatePreferenceInstance {
                partition: partition("p0"),
            }
            .to_string(),
            "preference list contains a duplicate instance for partition p0"
        );
    }
}
