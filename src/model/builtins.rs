use super::{State, StateCardinality, StateModelDefinition, Transition};

/// Construct Apache Helix 2.0.1's built-in LeaderStandby definition.
pub fn leader_standby() -> StateModelDefinition {
    let mut builder = StateModelDefinition::builder("LeaderStandby");
    builder.initial_state(state("OFFLINE"));
    builder.add_state(state("LEADER"), StateCardinality::Exact(1));
    builder.add_state(state("STANDBY"), StateCardinality::ReplicaCount);
    builder.add_state(state("OFFLINE"), StateCardinality::Unbounded);
    builder.add_state(state("DROPPED"), StateCardinality::Unbounded);
    builder.add_transition(transition("LEADER", "STANDBY"));
    builder.add_transition(transition("STANDBY", "LEADER"));
    builder.add_transition(transition("OFFLINE", "STANDBY"));
    builder.add_transition(transition("STANDBY", "OFFLINE"));
    builder.add_transition(transition("OFFLINE", "DROPPED"));
    builder
        .build()
        .expect("LeaderStandby built-in definition is valid")
}

fn state(name: &str) -> State {
    State::new(name).expect("built-in state name is non-empty")
}

fn transition(from: &str, to: &str) -> Transition {
    Transition::new(state(from), state(to))
}

#[cfg(test)]
mod tests {
    use super::leader_standby;
    use crate::model::{State, StateCardinality};

    #[test]
    fn leader_standby_matches_m0_metadata() {
        let model = leader_standby();
        assert_eq!(model.name(), "LeaderStandby");
        assert_eq!(model.initial_state().as_str(), "OFFLINE");
        assert_eq!(model.highest_priority_state().as_str(), "LEADER");
        assert!(model.has_single_highest_priority_state());
        assert_eq!(
            model.cardinality_for(&State::new("LEADER").expect("valid state")),
            Some(StateCardinality::Exact(1))
        );
        assert_eq!(
            model.cardinality_for(&State::new("STANDBY").expect("valid state")),
            Some(StateCardinality::ReplicaCount)
        );
    }
}
