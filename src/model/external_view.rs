use super::{CurrentState, InstanceId, PartitionId, ResourceId, State};
use std::collections::BTreeMap;

/// Immutable aggregation of observed replica states grouped by resource.
///
/// An external view is derived exclusively from [`CurrentState`] snapshots. It
/// deliberately does not consult desired placement or instance membership;
/// routing applies that separate membership filter when a snapshot is built.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExternalView {
    states: BTreeMap<ResourceId, BTreeMap<PartitionId, BTreeMap<InstanceId, State>>>,
}

impl ExternalView {
    /// Aggregate one observed state snapshot for each resource.
    pub fn from_current_states(current_states: BTreeMap<ResourceId, CurrentState>) -> Self {
        let states = current_states
            .into_iter()
            .map(|(resource, current_state)| (resource, current_state.into_entries()))
            .collect();
        Self { states }
    }

    /// Return the complete deterministic resource/partition/instance view.
    pub fn entries(
        &self,
    ) -> &BTreeMap<ResourceId, BTreeMap<PartitionId, BTreeMap<InstanceId, State>>> {
        &self.states
    }

    /// Look up an observed state in the aggregated view.
    pub fn state(
        &self,
        resource: &ResourceId,
        partition: &PartitionId,
        instance: &InstanceId,
    ) -> Option<&State> {
        self.states.get(resource)?.get(partition)?.get(instance)
    }
}

#[cfg(test)]
mod tests {
    use super::ExternalView;
    use crate::model::{CurrentState, ResourceId, State};
    use std::collections::BTreeMap;

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

    fn current_state(entries: &[(&str, &str, &str)]) -> CurrentState {
        let mut builder = CurrentState::builder();
        for (partition, instance, state) in entries {
            builder
                .set_state(id(partition), id(instance), id(state))
                .expect("test state should be unique");
        }
        builder.build()
    }

    #[test]
    fn aggregates_actual_states_without_desired_state() {
        let mut resources = BTreeMap::new();
        resources.insert(
            id::<ResourceId>("documents"),
            current_state(&[("documents_0", "node-b", "STANDBY")]),
        );
        resources.insert(
            id::<ResourceId>("profiles"),
            current_state(&[("profiles_0", "node-a", "ERROR")]),
        );

        let view = ExternalView::from_current_states(resources);

        assert_eq!(
            view.state(&id("documents"), &id("documents_0"), &id("node-b")),
            Some(&State::new("STANDBY").unwrap())
        );
        assert_eq!(view.entries().len(), 2);
        assert_eq!(
            view.state(&id("documents"), &id("documents_0"), &id("node-a")),
            None
        );
    }

    #[test]
    fn preserves_offline_and_dropped_observations() {
        let mut current = CurrentState::builder();
        current
            .set_state(id("p0"), id("node-a"), id("OFFLINE"))
            .unwrap()
            .set_state(id("p0"), id("node-b"), id("DROPPED"))
            .unwrap();

        let mut resources = BTreeMap::new();
        resources.insert(id("documents"), current.build());
        let view = ExternalView::from_current_states(resources);

        assert_eq!(view.entries()[&id("documents")][&id("p0")].len(), 2);
    }
}
