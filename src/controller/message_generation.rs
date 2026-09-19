use crate::model::StateModelDefinition;
use crate::model::{BestPossibleState, CurrentState, InstanceId, PartitionId, ResourceId};
use crate::transition::TransitionRequest;
use std::fmt;

/// Errors for unsupported or incomplete explicit M2 state input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionGenerationError {
    MissingCurrentState {
        partition: PartitionId,
        instance: InstanceId,
    },
    MissingTargetState {
        partition: PartitionId,
        instance: InstanceId,
    },
}

impl fmt::Display for TransitionGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingCurrentState {
                partition,
                instance,
            } => write!(
                formatter,
                "missing current state for partition {partition} and instance {instance}"
            ),
            Self::MissingTargetState {
                partition,
                instance,
            } => write!(
                formatter,
                "missing target state for partition {partition} and instance {instance}"
            ),
        }
    }
}

impl std::error::Error for TransitionGenerationError {}

/// Generate the semantic next transition for every explicitly supplied replica.
pub fn generate_transitions(
    resource: &ResourceId,
    current: &CurrentState,
    target: &BestPossibleState,
    state_model: &StateModelDefinition,
) -> Result<Vec<TransitionRequest>, TransitionGenerationError> {
    validate_explicit_replica_set(current, target)?;

    let mut requests = Vec::new();
    for (partition, instances) in target.entries() {
        for (instance, desired_state) in instances {
            let current_state = current
                .state(partition, instance)
                .expect("validated current state entry");

            if current_state == desired_state {
                continue;
            }

            let Some(next_state) = state_model.next_state_toward(current_state, desired_state)
            else {
                continue;
            };

            requests.push(TransitionRequest::new(
                resource.clone(),
                partition.clone(),
                instance.clone(),
                current_state.clone(),
                next_state.clone(),
            ));
        }
    }
    Ok(requests)
}

fn validate_explicit_replica_set(
    current: &CurrentState,
    target: &BestPossibleState,
) -> Result<(), TransitionGenerationError> {
    for (partition, instances) in target.entries() {
        for instance in instances.keys() {
            if current.state(partition, instance).is_none() {
                return Err(TransitionGenerationError::MissingCurrentState {
                    partition: partition.clone(),
                    instance: instance.clone(),
                });
            }
        }
    }
    for (partition, instances) in current.entries() {
        for instance in instances.keys() {
            if target.state(partition, instance).is_none() {
                return Err(TransitionGenerationError::MissingTargetState {
                    partition: partition.clone(),
                    instance: instance.clone(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{generate_transitions, TransitionGenerationError};
    use crate::model::{
        BestPossibleState, CurrentState, InstanceId, PartitionId, ResourceId, State,
        StateCardinality, StateModelDefinition, Transition,
    };

    fn state(value: &str) -> State {
        State::new(value).expect("valid state")
    }

    fn instance(value: &str) -> InstanceId {
        InstanceId::new(value).expect("valid instance")
    }

    fn partition(value: &str) -> PartitionId {
        PartitionId::new(value).expect("valid partition")
    }

    fn resource() -> ResourceId {
        ResourceId::new("documents").expect("valid resource")
    }

    fn model() -> StateModelDefinition {
        let mut builder = StateModelDefinition::builder("custom");
        builder.initial_state(state("COLD"));
        builder.add_state(state("HOT"), StateCardinality::Exact(1));
        builder.add_state(state("WARM"), StateCardinality::ReplicaCount);
        builder.add_state(state("COLD"), StateCardinality::Unbounded);
        builder.add_state(state("REMOVED"), StateCardinality::Unbounded);
        builder.add_state(state("DROPPED"), StateCardinality::Unbounded);
        builder.add_transition(Transition::new(state("HOT"), state("WARM")));
        builder.add_transition(Transition::new(state("WARM"), state("HOT")));
        builder.add_transition(Transition::new(state("WARM"), state("COLD")));
        builder.add_transition(Transition::new(state("COLD"), state("WARM")));
        builder.add_transition(Transition::new(state("COLD"), state("REMOVED")));
        builder.add_transition(Transition::new(state("REMOVED"), state("DROPPED")));
        builder.build().expect("valid model")
    }

    fn case_sensitive_model() -> StateModelDefinition {
        let mut builder = StateModelDefinition::builder("case-sensitive");
        builder.initial_state(state("FOO"));
        builder.add_state(state("FOO"), StateCardinality::Exact(1));
        builder.add_state(state("foo"), StateCardinality::Unbounded);
        builder.add_state(state("DROPPED"), StateCardinality::Unbounded);
        builder.add_transition(Transition::new(state("FOO"), state("foo")));
        builder.add_transition(Transition::new(state("foo"), state("DROPPED")));
        builder.build().expect("valid case-sensitive model")
    }

    fn inputs(current_state: &str, target_state: &str) -> (CurrentState, BestPossibleState) {
        let mut current = CurrentState::builder();
        current
            .set_state(partition("p0"), instance("node-a"), state(current_state))
            .expect("unique current state");
        let mut target = BestPossibleState::builder();
        target
            .set_state(partition("p0"), instance("node-a"), state(target_state))
            .expect("unique target state");
        (current.build(), target.build())
    }

    #[test]
    fn stable_replica_generates_no_request() {
        let (current, target) = inputs("WARM", "WARM");
        assert!(
            generate_transitions(&resource(), &current, &target, &model())
                .expect("valid input")
                .is_empty()
        );
    }

    #[test]
    fn direct_and_multihop_transitions_use_model_next_hops() {
        let (current, target) = inputs("WARM", "HOT");
        let direct =
            generate_transitions(&resource(), &current, &target, &model()).expect("valid input");
        assert_eq!(direct.len(), 1);
        assert_eq!(direct[0].source_state().as_str(), "WARM");
        assert_eq!(direct[0].target_state().as_str(), "HOT");

        let (current, target) = inputs("COLD", "HOT");
        let multihop =
            generate_transitions(&resource(), &current, &target, &model()).expect("valid input");
        assert_eq!(multihop.len(), 1);
        assert_eq!(multihop[0].source_state().as_str(), "COLD");
        assert_eq!(multihop[0].target_state().as_str(), "WARM");
    }

    #[test]
    fn state_names_are_case_sensitive() {
        let (current, target) = inputs("FOO", "foo");
        let transitions =
            generate_transitions(&resource(), &current, &target, &case_sensitive_model())
                .expect("valid input");

        assert_eq!(transitions.len(), 1);
        assert_eq!(transitions[0].source_state().as_str(), "FOO");
        assert_eq!(transitions[0].target_state().as_str(), "foo");
    }

    #[test]
    fn all_partitions_and_instances_are_processed_deterministically() {
        let mut current = CurrentState::builder();
        let mut target = BestPossibleState::builder();
        for (partition_name, instance_name, from, to) in [
            ("p1", "node-b", "COLD", "HOT"),
            ("p0", "node-a", "WARM", "HOT"),
            ("p1", "node-a", "WARM", "WARM"),
        ] {
            current
                .set_state(
                    partition(partition_name),
                    instance(instance_name),
                    state(from),
                )
                .expect("unique current state");
            target
                .set_state(
                    partition(partition_name),
                    instance(instance_name),
                    state(to),
                )
                .expect("unique target state");
        }
        let current = current.build();
        let target = target.build();
        let first =
            generate_transitions(&resource(), &current, &target, &model()).expect("valid input");
        let second =
            generate_transitions(&resource(), &current, &target, &model()).expect("valid input");
        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].partition().as_str(), "p0");
        assert_eq!(first[1].partition().as_str(), "p1");
        for request in &first {
            assert_eq!(
                model().next_state_toward(request.source_state(), &state("HOT")),
                Some(request.target_state())
            );
        }
    }

    #[test]
    fn unreachable_targets_are_ignored_without_panicking() {
        let (current, target) = inputs("REMOVED", "HOT");
        assert!(
            generate_transitions(&resource(), &current, &target, &model())
                .expect("valid input")
                .is_empty()
        );
    }

    #[test]
    fn missing_replica_entries_are_rejected() {
        let mut current = CurrentState::builder();
        current
            .set_state(partition("p0"), instance("node-a"), state("COLD"))
            .expect("unique current state");
        let mut target = BestPossibleState::builder();
        target
            .set_state(partition("p0"), instance("node-b"), state("HOT"))
            .expect("unique target state");
        let error = generate_transitions(&resource(), &current.build(), &target.build(), &model())
            .expect_err("missing current state must be rejected");
        assert!(matches!(
            error,
            TransitionGenerationError::MissingCurrentState { .. }
        ));
    }
}
