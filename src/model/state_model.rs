use super::{State, StateCardinality, Transition};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

/// An immutable, valid-by-construction state-model definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateModelDefinition {
    name: String,
    initial_state: State,
    states_by_priority: Vec<State>,
    transitions_by_priority: Vec<Transition>,
    cardinalities: BTreeMap<State, StateCardinality>,
    next_states: BTreeMap<State, BTreeMap<State, State>>,
}

impl StateModelDefinition {
    /// Start constructing a state-model definition.
    pub fn builder(name: impl Into<String>) -> StateModelBuilder {
        StateModelBuilder::new(name)
    }

    /// Return the model name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the initial state.
    pub fn initial_state(&self) -> &State {
        &self.initial_state
    }

    /// Return states from highest to lowest priority.
    pub fn states_in_priority_order(&self) -> &[State] {
        &self.states_by_priority
    }

    /// Return states from highest to lowest priority.
    pub fn states_priority(&self) -> &[State] {
        self.states_in_priority_order()
    }

    /// Return direct transitions from highest to lowest priority.
    pub fn transitions_in_priority_order(&self) -> &[Transition] {
        &self.transitions_by_priority
    }

    /// Return direct transitions from highest to lowest priority.
    pub fn transition_priority(&self) -> &[Transition] {
        self.transitions_in_priority_order()
    }

    /// Return the cardinality metadata for a state.
    pub fn cardinality_for(&self, state: &State) -> Option<StateCardinality> {
        self.cardinalities.get(state).copied()
    }

    /// Return the cardinality metadata for a state.
    pub fn state_cardinality(&self, state: &State) -> Option<StateCardinality> {
        self.cardinality_for(state)
    }

    /// Return the immutable cardinality map.
    pub fn cardinalities(&self) -> &BTreeMap<State, StateCardinality> {
        &self.cardinalities
    }

    /// Return the immutable cardinality map.
    pub fn state_cardinalities(&self) -> &BTreeMap<State, StateCardinality> {
        self.cardinalities()
    }

    /// Return the highest-priority state.
    pub fn highest_priority_state(&self) -> &State {
        &self.states_by_priority[0]
    }

    /// Return the highest-priority state.
    pub fn top_state(&self) -> &State {
        self.highest_priority_state()
    }

    /// Return whether exactly one instance is allowed in the top state.
    pub fn has_single_highest_priority_state(&self) -> bool {
        self.cardinality_for(self.highest_priority_state()) == Some(StateCardinality::Exact(1))
    }

    /// Return whether exactly one instance is allowed in the top state.
    pub fn is_single_top_state(&self) -> bool {
        self.has_single_highest_priority_state()
    }

    /// Return the first state Helix uses when moving from `from` toward `to`.
    pub fn next_state_toward(&self, from: &State, to: &State) -> Option<&State> {
        self.next_states
            .get(from)
            .and_then(|targets| targets.get(to))
    }

    /// Return the first state Helix uses when moving from `from` toward `to`.
    pub fn next_state(&self, from: &State, to: &State) -> Option<&State> {
        self.next_state_toward(from, to)
    }
}

/// Builder for an immutable [`StateModelDefinition`].
pub struct StateModelBuilder {
    name: String,
    initial_state: Option<State>,
    states: Vec<(State, StateCardinality)>,
    transitions: Vec<Transition>,
}

impl StateModelBuilder {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            initial_state: None,
            states: Vec::new(),
            transitions: Vec::new(),
        }
    }

    /// Set the initial state.
    pub fn initial_state(&mut self, state: State) -> &mut Self {
        self.initial_state = Some(state);
        self
    }

    /// Add a state at the next priority position.
    pub fn add_state(&mut self, state: State, cardinality: StateCardinality) -> &mut Self {
        self.states.push((state, cardinality));
        self
    }

    /// Add a direct transition at the next transition-priority position.
    pub fn add_transition(&mut self, transition: Transition) -> &mut Self {
        self.transitions.push(transition);
        self
    }

    /// Validate and publish the immutable definition.
    pub fn build(self) -> Result<StateModelDefinition, StateModelError> {
        if self.name.is_empty() {
            return Err(StateModelError::EmptyModelName);
        }
        if self.states.is_empty() {
            return Err(StateModelError::NoStates);
        }

        let mut state_set = HashSet::with_capacity(self.states.len());
        let mut cardinalities = BTreeMap::new();
        let mut states_by_priority = Vec::with_capacity(self.states.len());
        for (state, cardinality) in self.states {
            if !state_set.insert(state.clone()) {
                return Err(StateModelError::DuplicateState(state));
            }
            states_by_priority.push(state.clone());
            cardinalities.insert(state, cardinality);
        }

        let initial_state = self
            .initial_state
            .ok_or(StateModelError::MissingInitialState)?;
        if !state_set.contains(&initial_state) {
            return Err(StateModelError::UnknownInitialState(initial_state));
        }

        let mut transition_set = HashSet::with_capacity(self.transitions.len());
        for transition in &self.transitions {
            if !state_set.contains(transition.source()) {
                return Err(StateModelError::UnknownTransitionState {
                    transition: transition.clone(),
                    endpoint: transition.source().clone(),
                });
            }
            if !state_set.contains(transition.target()) {
                return Err(StateModelError::UnknownTransitionState {
                    transition: transition.clone(),
                    endpoint: transition.target().clone(),
                });
            }
            if !transition_set.insert(transition.clone()) {
                return Err(StateModelError::DuplicateTransition(transition.clone()));
            }
        }

        let next_states = build_transition_table(&states_by_priority, &self.transitions);
        let dropped = states_by_priority
            .iter()
            .find(|state| state.as_str() == "DROPPED")
            .ok_or(StateModelError::MissingDroppedState)?;
        for state in &states_by_priority {
            if state != dropped
                && !next_states
                    .get(state)
                    .is_some_and(|targets| targets.contains_key(dropped))
            {
                return Err(StateModelError::NoPathToDropped(state.clone()));
            }
        }

        Ok(StateModelDefinition {
            name: self.name,
            initial_state,
            states_by_priority,
            transitions_by_priority: self.transitions,
            cardinalities,
            next_states,
        })
    }
}

/// Construction failures for a state-model definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateModelError {
    EmptyModelName,
    NoStates,
    MissingInitialState,
    UnknownInitialState(State),
    MissingDroppedState,
    NoPathToDropped(State),
    DuplicateState(State),
    UnknownTransitionState {
        transition: Transition,
        endpoint: State,
    },
    DuplicateTransition(Transition),
}

impl fmt::Display for StateModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyModelName => formatter.write_str("state-model name must not be empty"),
            Self::NoStates => formatter.write_str("state model must contain at least one state"),
            Self::MissingInitialState => formatter.write_str("state model has no initial state"),
            Self::UnknownInitialState(state) => {
                write!(formatter, "initial state is not defined: {state}")
            }
            Self::MissingDroppedState => {
                formatter.write_str("state model must contain the DROPPED state")
            }
            Self::NoPathToDropped(state) => {
                write!(formatter, "state has no path to DROPPED: {state}")
            }
            Self::DuplicateState(state) => write!(formatter, "duplicate state: {state}"),
            Self::UnknownTransitionState {
                transition,
                endpoint,
            } => write!(
                formatter,
                "transition {} -> {} references unknown state {endpoint}",
                transition.source(),
                transition.target()
            ),
            Self::DuplicateTransition(transition) => write!(
                formatter,
                "duplicate transition: {} -> {}",
                transition.source(),
                transition.target()
            ),
        }
    }
}

impl std::error::Error for StateModelError {}

fn build_transition_table(
    states: &[State],
    transitions: &[Transition],
) -> BTreeMap<State, BTreeMap<State, State>> {
    let indexes: HashMap<State, usize> = states
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, state)| (state, index))
        .collect();
    let mut distances = vec![vec![None; states.len()]; states.len()];
    let mut next = vec![vec![None; states.len()]; states.len()];

    for index in 0..states.len() {
        distances[index][index] = Some(0usize);
        next[index][index] = Some(index);
    }
    for transition in transitions {
        let from = indexes[transition.source()];
        let to = indexes[transition.target()];
        distances[from][to] = Some(1);
        next[from][to] = Some(to);
    }

    // This is the ordered Floyd-Warshall next-hop construction used by
    // Apache Helix's StateTransitionTableBuilder in 2.0.1. The strict
    // comparison and loop order are semantic: equal-length paths retain the
    // earlier path already present in the table.
    for intermediate in 0..states.len() {
        for from in 0..states.len() {
            for to in 0..states.len() {
                let Some(left) = distances[from][intermediate] else {
                    continue;
                };
                let Some(right) = distances[intermediate][to] else {
                    continue;
                };
                let candidate = left + right;
                let current = distances[from][to].unwrap_or(usize::MAX);
                if candidate < current {
                    distances[from][to] = Some(candidate);
                    next[from][to] = next[from][intermediate];
                }
            }
        }
    }

    let mut table = BTreeMap::new();
    for (from, row) in next.into_iter().enumerate() {
        let mut targets = BTreeMap::new();
        for (to, next_index) in row.into_iter().enumerate() {
            if from != to {
                if let Some(next_index) = next_index {
                    targets.insert(states[to].clone(), states[next_index].clone());
                }
            }
        }
        table.insert(states[from].clone(), targets);
    }
    table
}

#[cfg(test)]
mod tests {
    use super::{StateModelDefinition, StateModelError};
    use crate::model::{State, StateCardinality, Transition};

    fn state(name: &str) -> State {
        State::new(name).expect("valid test state")
    }

    fn custom_model() -> StateModelDefinition {
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
        builder.build().expect("valid custom model")
    }

    #[test]
    fn arbitrary_names_and_priorities_are_preserved() {
        let model = custom_model();
        assert_eq!(model.name(), "custom");
        assert_eq!(model.initial_state().as_str(), "COLD");
        assert_eq!(
            model
                .states_in_priority_order()
                .iter()
                .map(State::as_str)
                .collect::<Vec<_>>(),
            ["HOT", "WARM", "COLD", "DROPPED"]
        );
        assert_eq!(model.highest_priority_state().as_str(), "HOT");
        assert_eq!(model.top_state().as_str(), "HOT");
        assert!(model.has_single_highest_priority_state());
        assert!(model.is_single_top_state());
        assert_eq!(model.states_priority(), model.states_in_priority_order());
        assert_eq!(
            model.transition_priority(),
            model.transitions_in_priority_order()
        );
        assert_eq!(model.transitions_in_priority_order().len(), 5);
        assert_eq!(
            model.cardinality_for(&state("HOT")),
            Some(StateCardinality::Exact(1))
        );
        assert_eq!(
            model.state_cardinality(&state("WARM")),
            Some(StateCardinality::ReplicaCount)
        );
        assert_eq!(model.cardinalities(), model.state_cardinalities());
    }

    #[test]
    fn multihop_and_reverse_paths_use_next_hops() {
        let model = custom_model();
        assert_eq!(
            model
                .next_state_toward(&state("COLD"), &state("HOT"))
                .map(State::as_str),
            Some("WARM")
        );
        assert_eq!(
            model
                .next_state_toward(&state("HOT"), &state("COLD"))
                .map(State::as_str),
            Some("WARM")
        );
    }

    #[test]
    fn unreachable_targets_return_none() {
        let model = custom_model();
        assert_eq!(
            model.next_state_toward(&state("DROPPED"), &state("HOT")),
            None
        );
        assert_eq!(
            model.next_state_toward(&state("COLD"), &state("COLD")),
            None
        );
    }

    #[test]
    fn dropped_state_is_required() {
        let mut builder = StateModelDefinition::builder("custom");
        builder.initial_state(state("COLD"));
        builder.add_state(state("COLD"), StateCardinality::Unbounded);

        assert_eq!(builder.build(), Err(StateModelError::MissingDroppedState));
    }

    #[test]
    fn every_state_must_reach_dropped() {
        let mut builder = StateModelDefinition::builder("custom");
        builder.initial_state(state("COLD"));
        builder.add_state(state("COLD"), StateCardinality::Unbounded);
        builder.add_state(state("DROPPED"), StateCardinality::Unbounded);

        assert_eq!(
            builder.build(),
            Err(StateModelError::NoPathToDropped(state("COLD")))
        );
    }

    #[test]
    fn malformed_definitions_are_rejected() {
        let mut builder = StateModelDefinition::builder("custom");
        builder.initial_state(state("COLD"));
        builder.add_state(state("COLD"), StateCardinality::Unbounded);
        builder.add_state(state("COLD"), StateCardinality::Unbounded);
        assert!(matches!(
            builder.build(),
            Err(StateModelError::DuplicateState(_))
        ));
    }

    #[test]
    fn reports_all_builder_errors() {
        assert_eq!(
            StateModelDefinition::builder("").build(),
            Err(StateModelError::EmptyModelName)
        );
        assert_eq!(
            StateModelDefinition::builder("custom").build(),
            Err(StateModelError::NoStates)
        );
        let mut missing_initial = StateModelDefinition::builder("custom");
        missing_initial.add_state(state("DROPPED"), StateCardinality::Unbounded);
        assert_eq!(
            missing_initial.build(),
            Err(StateModelError::MissingInitialState)
        );

        let mut unknown_initial = StateModelDefinition::builder("custom");
        unknown_initial.initial_state(state("COLD"));
        unknown_initial.add_state(state("DROPPED"), StateCardinality::Unbounded);
        assert_eq!(
            unknown_initial.build(),
            Err(StateModelError::UnknownInitialState(state("COLD")))
        );

        let mut unknown_source = StateModelDefinition::builder("custom");
        unknown_source.initial_state(state("DROPPED"));
        unknown_source.add_state(state("DROPPED"), StateCardinality::Unbounded);
        unknown_source.add_transition(Transition::new(state("MISSING"), state("DROPPED")));
        assert!(matches!(
            unknown_source.build(),
            Err(StateModelError::UnknownTransitionState { .. })
        ));

        let mut unknown_target = StateModelDefinition::builder("custom");
        unknown_target.initial_state(state("DROPPED"));
        unknown_target.add_state(state("DROPPED"), StateCardinality::Unbounded);
        unknown_target.add_transition(Transition::new(state("DROPPED"), state("MISSING")));
        assert!(matches!(
            unknown_target.build(),
            Err(StateModelError::UnknownTransitionState { .. })
        ));

        let mut duplicate_transition = StateModelDefinition::builder("custom");
        duplicate_transition.initial_state(state("DROPPED"));
        duplicate_transition.add_state(state("DROPPED"), StateCardinality::Unbounded);
        let transition = Transition::new(state("DROPPED"), state("DROPPED"));
        duplicate_transition.add_transition(transition.clone());
        duplicate_transition.add_transition(transition.clone());
        assert_eq!(
            duplicate_transition.build(),
            Err(StateModelError::DuplicateTransition(transition))
        );

        let errors = [
            StateModelError::EmptyModelName,
            StateModelError::NoStates,
            StateModelError::MissingInitialState,
            StateModelError::UnknownInitialState(state("COLD")),
            StateModelError::MissingDroppedState,
            StateModelError::NoPathToDropped(state("COLD")),
            StateModelError::DuplicateState(state("COLD")),
            StateModelError::UnknownTransitionState {
                transition: Transition::new(state("A"), state("B")),
                endpoint: state("B"),
            },
            StateModelError::DuplicateTransition(Transition::new(state("A"), state("B"))),
        ];
        assert!(errors.iter().all(|error| !error.to_string().is_empty()));
    }
}
