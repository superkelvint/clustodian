use crate::model::{ExternalView, InstanceId, PartitionId, ResourceId, State};
use std::collections::BTreeSet;
use std::fmt;

/// An invalid or ambiguous application routing result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutingError {
    InvalidResource(String),
    InvalidPartition(String),
    MultipleLeaders {
        resource: String,
        partition: String,
        instances: BTreeSet<InstanceId>,
    },
}

impl fmt::Display for RoutingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidResource(value) => write!(formatter, "invalid resource {value:?}"),
            Self::InvalidPartition(value) => write!(formatter, "invalid partition {value:?}"),
            Self::MultipleLeaders {
                resource,
                partition,
                instances,
            } => write!(
                formatter,
                "resource {resource} partition {partition} has multiple leaders: {instances:?}"
            ),
        }
    }
}

impl std::error::Error for RoutingError {}

/// Immutable EXTERNALVIEW-backed routing state.
///
/// The configured instance set represents the InstanceConfig membership used by
/// Helix's EXTERNALVIEW routing path. It is intentionally separate from live
/// sessions and does not perform participant/session filtering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingSnapshot {
    external_view: ExternalView,
    configured_instances: BTreeSet<InstanceId>,
}

impl RoutingSnapshot {
    /// Build a routing snapshot from an aggregated external view and the
    /// instances that have a corresponding routing configuration.
    pub fn from_external_view(
        external_view: ExternalView,
        configured_instances: impl IntoIterator<Item = InstanceId>,
    ) -> Self {
        Self {
            external_view,
            configured_instances: configured_instances.into_iter().collect(),
        }
    }

    /// Return the immutable ExternalView held by this snapshot.
    pub fn external_view(&self) -> &ExternalView {
        &self.external_view
    }

    /// Return the configured InstanceConfig membership represented by this snapshot.
    pub fn configured_instances(&self) -> &BTreeSet<InstanceId> {
        &self.configured_instances
    }

    /// Return configured instances advertising `state` for a resource partition.
    ///
    /// Missing resources, partitions, and states all produce an empty set.
    pub fn instances_for(
        &self,
        resource: &ResourceId,
        partition: &PartitionId,
        state: &State,
    ) -> BTreeSet<InstanceId> {
        let Some(instance_states) = self
            .external_view
            .entries()
            .get(resource)
            .and_then(|partitions| partitions.get(partition))
        else {
            return BTreeSet::new();
        };

        instance_states
            .iter()
            .filter(|(instance, advertised_state)| {
                *advertised_state == state && self.configured_instances.contains(*instance)
            })
            .map(|(instance, _)| instance.clone())
            .collect()
    }

    /// Return the sole configured leader, or `None` if no leader is visible.
    pub fn leader(
        &self,
        resource: impl AsRef<str>,
        partition: impl AsRef<str>,
    ) -> Result<Option<InstanceId>, RoutingError> {
        let resource_name = resource.as_ref();
        let partition_name = partition.as_ref();
        let resource_id = ResourceId::new(resource_name.to_owned())
            .map_err(|_| RoutingError::InvalidResource(resource_name.to_owned()))?;
        let partition_id = PartitionId::new(partition_name.to_owned())
            .map_err(|_| RoutingError::InvalidPartition(partition_name.to_owned()))?;
        let leader = State::try_from("LEADER").expect("LEADER is a valid state");
        let leaders = self.instances_for(&resource_id, &partition_id, &leader);
        match leaders.len() {
            0 => Ok(None),
            1 => Ok(leaders.into_iter().next()),
            _ => Err(RoutingError::MultipleLeaders {
                resource: resource_id.to_string(),
                partition: partition_id.to_string(),
                instances: leaders,
            }),
        }
    }

    /// Return all configured Leader and Standby replicas for a partition.
    pub fn replicas(
        &self,
        resource: impl AsRef<str>,
        partition: impl AsRef<str>,
    ) -> Result<BTreeSet<InstanceId>, RoutingError> {
        let resource_name = resource.as_ref();
        let partition_name = partition.as_ref();
        let resource_id = ResourceId::new(resource_name.to_owned())
            .map_err(|_| RoutingError::InvalidResource(resource_name.to_owned()))?;
        let partition_id = PartitionId::new(partition_name.to_owned())
            .map_err(|_| RoutingError::InvalidPartition(partition_name.to_owned()))?;
        let leader = State::try_from("LEADER").expect("LEADER is a valid state");
        let standby = State::try_from("STANDBY").expect("STANDBY is a valid state");
        let mut replicas = self.instances_for(&resource_id, &partition_id, &leader);
        replicas.extend(self.instances_for(&resource_id, &partition_id, &standby));
        Ok(replicas)
    }
}

#[cfg(test)]
mod tests {
    use super::RoutingSnapshot;
    use crate::model::{CurrentState, ExternalView, ResourceId};
    use std::collections::{BTreeMap, BTreeSet};

    fn id<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: std::fmt::Debug,
    {
        value
            .to_owned()
            .try_into()
            .expect("test identifier should be valid")
    }

    fn snapshot() -> RoutingSnapshot {
        let mut current = CurrentState::builder();
        current
            .set_state(id("p0"), id("node-a"), id("LEADER"))
            .unwrap()
            .set_state(id("p0"), id("node-b"), id("LEADER"))
            .unwrap()
            .set_state(id("p1"), id("node-a"), id("ERROR"))
            .unwrap();

        let mut resources = BTreeMap::new();
        resources.insert(id::<ResourceId>("documents"), current.build());
        let external_view = ExternalView::from_current_states(resources);
        RoutingSnapshot::from_external_view(external_view, [id("node-a"), id("node-c")])
    }

    #[test]
    fn routes_only_instances_with_configuration_membership() {
        let routing = snapshot();

        assert_eq!(
            routing.instances_for(&id("documents"), &id("p0"), &id("LEADER")),
            BTreeSet::from([id("node-a")])
        );
        assert_eq!(
            routing.instances_for(&id("documents"), &id("p1"), &id("ERROR")),
            BTreeSet::from([id("node-a")])
        );
    }

    #[test]
    fn unknown_resource_partition_or_state_is_empty() {
        let routing = snapshot();

        assert!(routing
            .instances_for(&id("missing"), &id("p0"), &id("LEADER"))
            .is_empty());
        assert!(routing
            .instances_for(&id("documents"), &id("missing"), &id("LEADER"))
            .is_empty());
        assert!(routing
            .instances_for(&id("documents"), &id("p0"), &id("WARMING_UP"))
            .is_empty());
    }

    #[test]
    fn configured_membership_is_deterministic_and_resource_scoped() {
        let routing = snapshot();
        assert_eq!(routing.configured_instances().len(), 2);
        assert_eq!(routing.external_view().entries().len(), 1);
        assert_ne!(
            routing.instances_for(&id("documents"), &id("p0"), &id("LEADER")),
            routing.instances_for(&id("other"), &id("p0"), &id("LEADER"))
        );
    }

    #[test]
    fn leader_and_replicas_are_typed_and_unambiguous() {
        let routing = snapshot();
        assert_eq!(
            routing.leader("documents", "p0").unwrap(),
            Some(id("node-a"))
        );
        assert_eq!(
            routing.replicas("documents", "p0").unwrap(),
            BTreeSet::from([id("node-a")])
        );
    }

    #[test]
    fn multiple_leaders_are_an_error() {
        let mut current = CurrentState::builder();
        current
            .set_state(id("p0"), id("node-a"), id("LEADER"))
            .unwrap()
            .set_state(id("p0"), id("node-b"), id("LEADER"))
            .unwrap();
        let mut resources = BTreeMap::new();
        resources.insert(id::<ResourceId>("documents"), current.build());
        let routing = RoutingSnapshot::from_external_view(
            ExternalView::from_current_states(resources),
            [id("node-a"), id("node-b")],
        );

        assert!(matches!(
            routing.leader("documents", "p0"),
            Err(super::RoutingError::MultipleLeaders { .. })
        ));
    }
}
