use std::fmt;

/// An application-defined state name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct State(String);

impl State {
    /// Construct a state from its exact name.
    pub fn new(name: impl Into<String>) -> Result<Self, StateError> {
        let name = name.into();
        if name.is_empty() {
            return Err(StateError::EmptyName);
        }
        Ok(Self(name))
    }

    /// Return the exact state name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Return whether this is the built-in LeaderStandby leader state.
    pub fn is_leader(&self) -> bool {
        self.as_str() == "LEADER"
    }

    /// Return whether this is the built-in LeaderStandby standby state.
    pub fn is_standby(&self) -> bool {
        self.as_str() == "STANDBY"
    }

    /// Return whether this is the built-in LeaderStandby offline state.
    pub fn is_offline(&self) -> bool {
        self.as_str() == "OFFLINE"
    }

    /// Return whether this is the built-in LeaderStandby dropped state.
    pub fn is_dropped(&self) -> bool {
        self.as_str() == "DROPPED"
    }

    /// Return whether this is the built-in LeaderStandby error state.
    pub fn is_error(&self) -> bool {
        self.as_str() == "ERROR"
    }
}

impl TryFrom<String> for State {
    type Error = StateError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for State {
    type Error = StateError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for State {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Errors constructing an opaque state name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateError {
    EmptyName,
}

impl fmt::Display for StateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName => formatter.write_str("state name must not be empty"),
        }
    }
}

impl std::error::Error for StateError {}

/// A state-machine transition identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Transition {
    source: State,
    target: State,
}

impl Transition {
    /// Construct a transition from one state to another.
    pub fn new(source: State, target: State) -> Self {
        Self { source, target }
    }

    /// Return the source state.
    pub fn source(&self) -> &State {
        &self.source
    }

    /// Return the source state using the established transition terminology.
    pub fn from(&self) -> &State {
        self.source()
    }

    /// Return the destination state.
    pub fn target(&self) -> &State {
        &self.target
    }

    /// Return the destination state using the established transition terminology.
    pub fn to(&self) -> &State {
        self.target()
    }
}

/// The symbolic cardinality metadata attached to a state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateCardinality {
    /// A fixed non-negative numeric cardinality.
    Exact(u32),
    /// Helix's `R`: all replicas in the resource preference list.
    ReplicaCount,
    /// Helix's `N`: all candidate nodes in the cluster.
    NodeCount,
    /// Helix's `-1`: no required cardinality.
    Unbounded,
}

impl TryFrom<&str> for StateCardinality {
    type Error = StateCardinalityError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "R" => Ok(Self::ReplicaCount),
            "N" => Ok(Self::NodeCount),
            "-1" => Ok(Self::Unbounded),
            value => value
                .parse::<u32>()
                .map(Self::Exact)
                .map_err(|_| StateCardinalityError::InvalidValue(value.to_owned())),
        }
    }
}

/// An invalid Helix cardinality symbol or numeric value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateCardinalityError {
    InvalidValue(String),
}

impl fmt::Display for StateCardinalityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidValue(value) => write!(formatter, "invalid state cardinality: {value}"),
        }
    }
}

impl std::error::Error for StateCardinalityError {}

#[cfg(test)]
mod tests {
    use super::{State, StateCardinality, StateCardinalityError, StateError, Transition};

    #[test]
    fn state_names_are_opaque_and_orderable() {
        let cold = State::new("COLD").expect("valid state");
        let hot = State::new("HOT").expect("valid state");
        assert_eq!(cold.as_str(), "COLD");
        assert!(cold < hot);
        assert_eq!(cold, State::try_from("COLD").expect("valid state"));
    }

    #[test]
    fn leader_standby_predicates_are_false_for_other_state_names() {
        let leader = State::new("LEADER").expect("valid state");
        let standby = State::new("STANDBY").expect("valid state");
        let offline = State::new("OFFLINE").expect("valid state");
        let dropped = State::new("DROPPED").expect("valid state");
        let error = State::new("ERROR").expect("valid state");
        let custom = State::new("CUSTOM").expect("valid state");

        assert!(leader.is_leader());
        assert!(standby.is_standby());
        assert!(offline.is_offline());
        assert!(dropped.is_dropped());
        assert!(error.is_error());
        assert!(!custom.is_leader());
        assert!(!custom.is_standby());
        assert!(!custom.is_offline());
        assert!(!custom.is_dropped());
        assert!(!custom.is_error());
    }

    #[test]
    fn empty_state_names_are_rejected() {
        assert_eq!(State::new(""), Err(StateError::EmptyName));
    }

    #[test]
    fn transitions_are_state_only() {
        let from = State::new("COLD").expect("valid state");
        let to = State::new("WARM").expect("valid state");
        let transition = Transition::new(from.clone(), to.clone());
        assert_eq!(transition.source(), &from);
        assert_eq!(transition.from(), &from);
        assert_eq!(transition.target(), &to);
        assert_eq!(transition.to(), &to);
        assert_eq!(State::try_from(String::from("C")).unwrap().to_string(), "C");
        assert_eq!(
            StateError::EmptyName.to_string(),
            "state name must not be empty"
        );
        assert_eq!(
            StateCardinalityError::InvalidValue(String::from("bad")).to_string(),
            "invalid state cardinality: bad"
        );
    }

    #[test]
    fn cardinalities_preserve_helix_symbols() {
        assert_eq!(
            StateCardinality::try_from("1"),
            Ok(StateCardinality::Exact(1))
        );
        assert_eq!(
            StateCardinality::try_from("R"),
            Ok(StateCardinality::ReplicaCount)
        );
        assert_eq!(
            StateCardinality::try_from("N"),
            Ok(StateCardinality::NodeCount)
        );
        assert_eq!(
            StateCardinality::try_from("-1"),
            Ok(StateCardinality::Unbounded)
        );
        assert_eq!(
            StateCardinality::try_from("-27"),
            Err(StateCardinalityError::InvalidValue(String::from("-27")))
        );
    }
}
