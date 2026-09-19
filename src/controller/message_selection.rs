use crate::model::{
    CurrentState, IdealState, InstanceId, ResourceId, StateCardinality, StateModelDefinition,
};
use crate::transition::{PendingTransition, TransitionRequest};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Errors for malformed input to M5 message selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MessageSelectionError {
    CandidateResourceMismatch {
        expected: ResourceId,
        actual: ResourceId,
    },
    UnknownCandidatePartition(crate::model::PartitionId),
    UnknownPendingPartition(crate::model::PartitionId),
    DuplicatePendingTransition {
        partition: crate::model::PartitionId,
        instance: InstanceId,
    },
}

impl fmt::Display for MessageSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CandidateResourceMismatch { expected, actual } => write!(
                formatter,
                "candidate transition belongs to resource {actual}, expected {expected}"
            ),
            Self::UnknownCandidatePartition(partition) => {
                write!(
                    formatter,
                    "candidate transition references unknown partition {partition}"
                )
            }
            Self::UnknownPendingPartition(partition) => {
                write!(
                    formatter,
                    "pending transition references unknown partition {partition}"
                )
            }
            Self::DuplicatePendingTransition {
                partition,
                instance,
            } => write!(
                formatter,
                "duplicate pending transition for partition {partition} and instance {instance}"
            ),
        }
    }
}

impl std::error::Error for MessageSelectionError {}

/// Select the state transitions that fit the state-model cardinality bounds.
///
/// This is the semantic M5 subset of Helix's `MessageSelectionStage`.  It
/// counts live current states and pending transition endpoints, groups
/// candidates by the state model's transition priority, and greedily accepts
/// candidates without exceeding a constrained destination-state bound.
pub fn select_transitions(
    resource: &ResourceId,
    ideal_state: &IdealState,
    current: &CurrentState,
    live_instances: &BTreeSet<InstanceId>,
    candidates: &[TransitionRequest],
    pending: &[PendingTransition],
    state_model: &StateModelDefinition,
) -> Result<Vec<TransitionRequest>, MessageSelectionError> {
    let mut candidates_by_partition: BTreeMap<_, Vec<&TransitionRequest>> = BTreeMap::new();
    for candidate in candidates {
        if candidate.resource() != resource {
            return Err(MessageSelectionError::CandidateResourceMismatch {
                expected: resource.clone(),
                actual: candidate.resource().clone(),
            });
        }
        if ideal_state.preference_list(candidate.partition()).is_none()
            && !is_drop_path(
                candidate.partition(),
                candidate.source_state(),
                candidate.target_state(),
                current,
                state_model,
            )
        {
            return Err(MessageSelectionError::UnknownCandidatePartition(
                candidate.partition().clone(),
            ));
        }
        candidates_by_partition
            .entry(candidate.partition().clone())
            .or_default()
            .push(candidate);
    }

    let mut pending_by_partition: BTreeMap<_, Vec<&PendingTransition>> = BTreeMap::new();
    let mut pending_keys = BTreeSet::new();
    for transition in pending {
        if ideal_state
            .preference_list(transition.partition())
            .is_none()
            && !is_drop_path(
                transition.partition(),
                transition.source_state(),
                transition.target_state(),
                current,
                state_model,
            )
        {
            return Err(MessageSelectionError::UnknownPendingPartition(
                transition.partition().clone(),
            ));
        }
        if !pending_keys.insert((
            transition.partition().clone(),
            transition.instance().clone(),
        )) {
            return Err(MessageSelectionError::DuplicatePendingTransition {
                partition: transition.partition().clone(),
                instance: transition.instance().clone(),
            });
        }
        pending_by_partition
            .entry(transition.partition().clone())
            .or_default()
            .push(transition);
    }

    let bounds = state_bounds(state_model, live_instances.len());
    let mut selected = Vec::new();
    let mut partitions = ideal_state
        .preference_lists()
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    partitions.extend(candidates_by_partition.keys().cloned());
    for partition in partitions {
        let Some(partition_candidates) = candidates_by_partition.get(&partition) else {
            continue;
        };
        let mut state_counts = BTreeMap::new();
        for instance in live_instances {
            let state = current
                .state(&partition, instance)
                .unwrap_or_else(|| state_model.initial_state());
            increase_state_count(&bounds, state, &mut state_counts);
        }
        if let Some(partition_pending) = pending_by_partition.get(&partition) {
            for transition in partition_pending {
                increase_state_count(&bounds, transition.target_state(), &mut state_counts);
                increase_state_count(&bounds, transition.source_state(), &mut state_counts);
            }
        }

        let mut grouped: BTreeMap<usize, Vec<&TransitionRequest>> = BTreeMap::new();
        for candidate in partition_candidates {
            let priority = state_model
                .transitions_in_priority_order()
                .iter()
                .position(|transition| {
                    transition.source() == candidate.source_state()
                        && transition.target() == candidate.target_state()
                })
                .unwrap_or(usize::MAX);
            grouped.entry(priority).or_default().push(candidate);
        }

        for group in grouped.into_values() {
            for candidate in group {
                if let Some(upper_bound) = bounds.get(candidate.target_state()) {
                    let new_count = state_counts
                        .get(candidate.target_state())
                        .copied()
                        .unwrap_or(0)
                        + 1;
                    if new_count > *upper_bound {
                        continue;
                    }
                }
                increase_state_count(&bounds, candidate.target_state(), &mut state_counts);
                selected.push(candidate.clone());
            }
        }
    }
    Ok(selected)
}

fn is_drop_path(
    partition: &crate::model::PartitionId,
    source: &crate::model::State,
    target: &crate::model::State,
    current: &CurrentState,
    state_model: &StateModelDefinition,
) -> bool {
    let Some(dropped) = state_model
        .states_in_priority_order()
        .iter()
        .find(|state| state.as_str() == "DROPPED")
    else {
        return false;
    };
    current.entries().contains_key(partition)
        && state_model.next_state_toward(source, dropped) == Some(target)
}

fn state_bounds(
    state_model: &StateModelDefinition,
    live_instance_count: usize,
) -> BTreeMap<crate::model::State, usize> {
    state_model
        .states_in_priority_order()
        .iter()
        .filter_map(|state| {
            let bound = match state_model.cardinality_for(state)? {
                StateCardinality::Exact(value) => usize::try_from(value).ok(),
                StateCardinality::NodeCount => Some(live_instance_count),
                // Helix's MessageSelectionStage leaves the dynamic `R`
                // bound unresolved. It constrains fixed and all-live state
                // cardinalities, but not the all-replicas state.
                StateCardinality::ReplicaCount => None,
                StateCardinality::Unbounded => None,
            }?;
            Some((state.clone(), bound))
        })
        .collect()
}

fn increase_state_count(
    bounds: &BTreeMap<crate::model::State, usize>,
    state: &crate::model::State,
    state_counts: &mut BTreeMap<crate::model::State, usize>,
) {
    if bounds.contains_key(state) {
        *state_counts.entry(state.clone()).or_default() += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::select_transitions;
    use crate::model::{
        leader_standby, CurrentState, IdealState, InstanceId, PartitionId, ResourceId, State,
    };
    use crate::transition::{PendingTransition, TransitionRequest};
    use std::collections::BTreeSet;

    fn state(name: &str) -> State {
        State::new(name).expect("valid state")
    }

    fn instance(name: &str) -> InstanceId {
        InstanceId::new(name).expect("valid instance")
    }

    fn partition(name: &str) -> PartitionId {
        PartitionId::new(name).expect("valid partition")
    }

    fn resource() -> ResourceId {
        ResourceId::new("documents").expect("valid resource")
    }

    fn ideal(partitions: &[&str]) -> IdealState {
        let mut builder = IdealState::builder(resource(), 2);
        for name in partitions {
            builder
                .set_preference_list(
                    partition(name),
                    vec![instance("node-a"), instance("node-b")],
                )
                .expect("unique partition");
        }
        builder.build().expect("valid ideal state")
    }

    fn single_replica_ideal() -> IdealState {
        let mut builder = IdealState::builder(resource(), 1);
        builder
            .set_preference_list(partition("p0"), vec![instance("node-a")])
            .expect("unique partition");
        builder.build().expect("valid ideal state")
    }

    fn live() -> BTreeSet<InstanceId> {
        [instance("node-a"), instance("node-b")]
            .into_iter()
            .collect()
    }

    fn current(partition_name: &str, a: &str, b: &str) -> CurrentState {
        let mut builder = CurrentState::builder();
        builder
            .set_state(partition(partition_name), instance("node-a"), state(a))
            .expect("unique state");
        builder
            .set_state(partition(partition_name), instance("node-b"), state(b))
            .expect("unique state");
        builder.build()
    }

    fn candidate(
        partition_name: &str,
        instance_name: &str,
        from: &str,
        to: &str,
    ) -> TransitionRequest {
        TransitionRequest::new(
            resource(),
            partition(partition_name),
            instance(instance_name),
            state(from),
            state(to),
        )
    }

    #[test]
    fn selects_safe_promotion_and_blocks_existing_top_state() {
        let ideal = ideal(&["p0"]);
        let promotion = candidate("p0", "node-b", "STANDBY", "LEADER");
        let selected = select_transitions(
            &resource(),
            &ideal,
            &current("p0", "STANDBY", "STANDBY"),
            &live(),
            std::slice::from_ref(&promotion),
            &[],
            &leader_standby(),
        )
        .expect("valid selection");
        assert_eq!(selected, vec![promotion.clone()]);

        let blocked = select_transitions(
            &resource(),
            &ideal,
            &current("p0", "LEADER", "STANDBY"),
            &live(),
            std::slice::from_ref(&promotion),
            &[],
            &leader_standby(),
        )
        .expect("valid selection");
        assert!(blocked.is_empty());
    }

    #[test]
    fn pending_endpoints_reserve_state_capacity() {
        let ideal = ideal(&["p0"]);
        let promotion = candidate("p0", "node-b", "STANDBY", "LEADER");
        let pending = PendingTransition::new(
            partition("p0"),
            instance("node-a"),
            state("STANDBY"),
            state("LEADER"),
        );
        let selected = select_transitions(
            &resource(),
            &ideal,
            &current("p0", "STANDBY", "STANDBY"),
            &live(),
            std::slice::from_ref(&promotion),
            std::slice::from_ref(&pending),
            &leader_standby(),
        )
        .expect("valid selection");
        assert!(selected.is_empty());
    }

    #[test]
    fn dynamic_replica_count_cardinality_is_not_bounded_by_selection() {
        let ideal = single_replica_ideal();
        let current = current("p0", "STANDBY", "OFFLINE");
        let candidate = candidate("p0", "node-b", "OFFLINE", "STANDBY");
        let selected = select_transitions(
            &resource(),
            &ideal,
            &current,
            &live(),
            std::slice::from_ref(&candidate),
            &[],
            &leader_standby(),
        )
        .expect("valid selection");
        assert_eq!(selected, vec![candidate]);
    }

    #[test]
    fn duplicate_pending_replica_entries_are_rejected() {
        let ideal = ideal(&["p0"]);
        let pending = PendingTransition::new(
            partition("p0"),
            instance("node-a"),
            state("OFFLINE"),
            state("STANDBY"),
        );
        let error = select_transitions(
            &resource(),
            &ideal,
            &current("p0", "OFFLINE", "OFFLINE"),
            &live(),
            &[],
            &[pending.clone(), pending],
            &leader_standby(),
        )
        .expect_err("duplicate pending replica must be rejected");
        assert!(matches!(
            error,
            super::MessageSelectionError::DuplicatePendingTransition { .. }
        ));
    }

    #[test]
    fn selects_transition_for_a_partition_removed_from_the_ideal_state() {
        let ideal = single_replica_ideal();
        let removed = partition("p-removed");
        let current = {
            let mut builder = CurrentState::builder();
            builder
                .set_state(removed.clone(), instance("node-a"), state("OFFLINE"))
                .unwrap();
            builder.build()
        };
        let candidate = TransitionRequest::new(
            resource(),
            removed,
            instance("node-a"),
            state("OFFLINE"),
            state("DROPPED"),
        );
        let selected = select_transitions(
            &resource(),
            &ideal,
            &current,
            &BTreeSet::from([instance("node-a")]),
            std::slice::from_ref(&candidate),
            &[],
            &leader_standby(),
        )
        .unwrap();
        assert_eq!(selected, vec![candidate]);
    }

    #[test]
    fn demotion_priority_precedes_promotion_and_partitions_are_independent() {
        let ideal = ideal(&["p0", "p1"]);
        let demotion = candidate("p0", "node-a", "LEADER", "STANDBY");
        let promotion = candidate("p0", "node-b", "STANDBY", "LEADER");
        let other = candidate("p1", "node-a", "STANDBY", "LEADER");
        let current = {
            let mut builder = CurrentState::builder();
            builder
                .set_state(partition("p0"), instance("node-a"), state("LEADER"))
                .unwrap();
            builder
                .set_state(partition("p0"), instance("node-b"), state("STANDBY"))
                .unwrap();
            builder
                .set_state(partition("p1"), instance("node-a"), state("STANDBY"))
                .unwrap();
            builder
                .set_state(partition("p1"), instance("node-b"), state("STANDBY"))
                .unwrap();
            builder.build()
        };
        let selected = select_transitions(
            &resource(),
            &ideal,
            &current,
            &live(),
            &[promotion, demotion.clone(), other.clone()],
            &[],
            &leader_standby(),
        )
        .expect("valid selection");
        assert!(selected.contains(&demotion));
        assert!(selected.contains(&other));
        assert!(!selected
            .iter()
            .any(|request| request.partition() == &partition("p0")
                && request.target_state() == &state("LEADER")));
    }

    #[test]
    fn repeated_selection_is_deterministic() {
        let ideal = ideal(&["p0"]);
        let candidates = vec![
            candidate("p0", "node-a", "OFFLINE", "STANDBY"),
            candidate("p0", "node-b", "OFFLINE", "STANDBY"),
        ];
        let current = current("p0", "OFFLINE", "OFFLINE");
        let first = select_transitions(
            &resource(),
            &ideal,
            &current,
            &live(),
            &candidates,
            &[],
            &leader_standby(),
        )
        .expect("valid selection");
        let second = select_transitions(
            &resource(),
            &ideal,
            &current,
            &live(),
            &candidates,
            &[],
            &leader_standby(),
        )
        .expect("valid selection");
        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
    }
}
