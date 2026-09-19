use crate::model::{
    BestPossibleState, CurrentState, IdealState, InstanceId, ReplicaStateError, State,
    StateCardinality, StateModelDefinition,
};
use std::collections::{BTreeSet, VecDeque};
use std::fmt;

/// Errors raised while computing a desired state for an explicit placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SemiAutoError {
    ReplicaState(ReplicaStateError),
}

impl fmt::Display for SemiAutoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplicaState(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for SemiAutoError {}

impl From<ReplicaStateError> for SemiAutoError {
    fn from(error: ReplicaStateError) -> Self {
        Self::ReplicaState(error)
    }
}

/// Compute the Helix SEMI_AUTO best-possible state for an explicit placement.
pub fn compute_semi_auto_best_possible_state(
    ideal_state: &IdealState,
    current_state: &CurrentState,
    live_instances: &BTreeSet<InstanceId>,
    state_model: &StateModelDefinition,
) -> Result<BestPossibleState, SemiAutoError> {
    let mut output = BestPossibleState::builder();

    for (partition, preference_list) in ideal_state.preference_lists() {
        let current = current_state.entries().get(partition);

        if let Some(current) = current {
            for instance in current.keys() {
                if !preference_list.contains(instance) {
                    output.set_state(
                        partition.clone(),
                        instance.clone(),
                        State::new("DROPPED").expect("Helix-defined state is non-empty"),
                    )?;
                }
            }
        }

        let active_instances = preference_list
            .iter()
            .filter(|instance| live_instances.contains(*instance))
            .cloned()
            .collect::<Vec<_>>();
        let mut active_queue = VecDeque::from(active_instances.clone());
        let mut current_priority = active_instances;
        current_priority
            .sort_by_key(|instance| current_state_priority(current, instance, state_model));
        let mut current_priority = current_priority.into_iter();
        let mut assigned = BTreeSet::new();
        let total_candidate_count = active_queue.len();

        for requested_state in state_model.states_in_priority_order() {
            let mut state_count = state_count(
                state_model.cardinality_for(requested_state),
                total_candidate_count,
                preference_list.len(),
            );
            while state_count > 0 {
                let Some(peek_instance) = active_queue.front().cloned() else {
                    break;
                };
                if assigned.contains(&peek_instance) {
                    active_queue.pop_front();
                    continue;
                }

                let current_instance_state = current
                    .and_then(|states| states.get(&peek_instance))
                    .unwrap_or(state_model.initial_state());
                let proposed = adjust_instance_if_necessary(
                    &peek_instance,
                    &assigned,
                    total_candidate_count.saturating_sub(assigned.len()),
                    state_count,
                    requested_state == state_model.highest_priority_state(),
                    current_instance_state != requested_state
                        && !is_second_top_state(current_instance_state, state_model),
                    &mut current_priority,
                );
                if proposed == peek_instance {
                    active_queue.pop_front();
                }
                output.set_state(partition.clone(), proposed.clone(), requested_state.clone())?;
                assigned.insert(proposed);
                state_count -= 1;
            }
        }
    }

    // A partition removed from the ideal state still exists in CurrentState
    // until its replicas have completed the state-model path to DROPPED.
    // Keep those replicas in the target map so transition generation and the
    // later controller stages can drain them instead of treating the state
    // as malformed input.
    let dropped = State::new("DROPPED").expect("Helix-defined state is non-empty");
    for (partition, current) in current_state.entries() {
        if ideal_state.preference_list(partition).is_none() {
            for instance in current.keys() {
                output.set_state(partition.clone(), instance.clone(), dropped.clone())?;
            }
        }
    }

    Ok(output.build())
}

fn state_count(
    cardinality: Option<StateCardinality>,
    live_candidate_count: usize,
    preference_list_count: usize,
) -> usize {
    match cardinality {
        Some(StateCardinality::Exact(count)) => count as usize,
        Some(StateCardinality::ReplicaCount) => preference_list_count,
        Some(StateCardinality::NodeCount) => live_candidate_count,
        Some(StateCardinality::Unbounded) | None => 0,
    }
}

fn current_state_priority(
    current: Option<&std::collections::BTreeMap<InstanceId, State>>,
    instance: &InstanceId,
    state_model: &StateModelDefinition,
) -> usize {
    current
        .and_then(|states| states.get(instance))
        .and_then(|state| {
            state_model
                .states_in_priority_order()
                .iter()
                .position(|candidate| candidate == state)
        })
        .unwrap_or(usize::MAX)
}

fn adjust_instance_if_necessary(
    proposed_instance: &InstanceId,
    assigned: &BTreeSet<InstanceId>,
    remaining_candidate_count: usize,
    remaining_request_count: usize,
    requested_state_is_top: bool,
    current_needs_adjustment: bool,
    current_priority: &mut impl Iterator<Item = InstanceId>,
) -> InstanceId {
    if remaining_request_count < remaining_candidate_count
        && requested_state_is_top
        && current_needs_adjustment
    {
        for candidate in current_priority {
            if !assigned.contains(&candidate) {
                return candidate;
            }
        }
    }
    proposed_instance.clone()
}

fn is_second_top_state(state: &State, state_model: &StateModelDefinition) -> bool {
    state_model
        .states_in_priority_order()
        .iter()
        .any(|candidate| {
            state_model.next_state_toward(candidate, state_model.highest_priority_state())
                == Some(state_model.highest_priority_state())
                && candidate == state
        })
}

#[cfg(test)]
mod tests {
    use super::compute_semi_auto_best_possible_state;
    use crate::model::{
        IdealState, InstanceId, PartitionId, State, StateCardinality, StateModelDefinition,
        Transition,
    };
    use std::collections::BTreeSet;

    fn state(name: &str) -> State {
        State::new(name).unwrap()
    }

    fn instance(name: &str) -> InstanceId {
        InstanceId::new(name).unwrap()
    }

    fn model() -> StateModelDefinition {
        let mut builder = StateModelDefinition::builder("custom");
        builder.initial_state(state("COLD"));
        builder.add_state(state("HOT"), StateCardinality::Exact(1));
        builder.add_state(state("WARM"), StateCardinality::ReplicaCount);
        builder.add_state(state("COLD"), StateCardinality::Unbounded);
        builder.add_state(state("DROPPED"), StateCardinality::Unbounded);
        builder.add_transition(Transition::new(state("HOT"), state("WARM")));
        builder.add_transition(Transition::new(state("WARM"), state("HOT")));
        builder.add_transition(Transition::new(state("WARM"), state("COLD")));
        builder.add_transition(Transition::new(state("COLD"), state("WARM")));
        builder.add_transition(Transition::new(state("COLD"), state("DROPPED")));
        builder.build().unwrap()
    }

    fn ideal() -> IdealState {
        let mut builder =
            IdealState::builder(crate::model::ResourceId::new("documents").unwrap(), 3);
        builder
            .set_preference_list(
                PartitionId::new("p0").unwrap(),
                vec![instance("node-a"), instance("node-b"), instance("node-c")],
            )
            .unwrap();
        builder.build().unwrap()
    }

    #[test]
    fn assigns_all_live_custom_replicas_in_state_priority_order() {
        let mut current = crate::model::CurrentState::builder();
        current
            .set_state(
                PartitionId::new("p0").unwrap(),
                instance("node-a"),
                state("COLD"),
            )
            .unwrap();
        current
            .set_state(
                PartitionId::new("p0").unwrap(),
                instance("node-b"),
                state("WARM"),
            )
            .unwrap();
        let live = [instance("node-a"), instance("node-b"), instance("node-c")]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let result =
            compute_semi_auto_best_possible_state(&ideal(), &current.build(), &live, &model())
                .unwrap();
        assert_eq!(
            result
                .state(&PartitionId::new("p0").unwrap(), &instance("node-a"))
                .unwrap()
                .as_str(),
            "WARM"
        );
        assert_eq!(
            result
                .state(&PartitionId::new("p0").unwrap(), &instance("node-b"))
                .unwrap()
                .as_str(),
            "HOT"
        );
        assert_eq!(
            result
                .state(&PartitionId::new("p0").unwrap(), &instance("node-c"))
                .unwrap()
                .as_str(),
            "WARM"
        );
    }

    #[test]
    fn drops_current_replicas_removed_from_preference_list() {
        let mut ideal_builder =
            IdealState::builder(crate::model::ResourceId::new("documents").unwrap(), 2);
        ideal_builder
            .set_preference_list(
                PartitionId::new("p0").unwrap(),
                vec![instance("node-a"), instance("node-b")],
            )
            .unwrap();
        let ideal = ideal_builder.build().unwrap();
        let mut current = crate::model::CurrentState::builder();
        current
            .set_state(
                PartitionId::new("p0").unwrap(),
                instance("node-c"),
                state("WARM"),
            )
            .unwrap();
        let live = [instance("node-a"), instance("node-b"), instance("node-c")]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let result =
            compute_semi_auto_best_possible_state(&ideal, &current.build(), &live, &model())
                .unwrap();
        assert_eq!(
            result
                .state(&PartitionId::new("p0").unwrap(), &instance("node-c"))
                .unwrap()
                .as_str(),
            "DROPPED"
        );
    }

    #[test]
    fn drops_partitions_removed_from_the_ideal_state() {
        let ideal = ideal();
        let removed_partition = PartitionId::new("p1").unwrap();
        let mut current = crate::model::CurrentState::builder();
        current
            .set_state(
                removed_partition.clone(),
                instance("node-a"),
                state("OFFLINE"),
            )
            .unwrap();
        let live = [instance("node-a")].into_iter().collect::<BTreeSet<_>>();

        let result =
            compute_semi_auto_best_possible_state(&ideal, &current.build(), &live, &model())
                .unwrap();

        assert_eq!(
            result
                .state(&removed_partition, &instance("node-a"))
                .unwrap()
                .as_str(),
            "DROPPED"
        );
    }
}
