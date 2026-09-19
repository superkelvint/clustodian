//! Immutable state-model semantics used by Helix-derived control-plane code.

mod best_possible_state;
mod builtins;
mod current_state;
mod external_view;
mod ideal_state;
mod identity;
mod replica_state;
mod session;
mod state;
mod state_model;

pub use best_possible_state::{BestPossibleState, BestPossibleStateBuilder};
pub use builtins::leader_standby;
pub use current_state::{CurrentState, CurrentStateBuilder};
pub use external_view::ExternalView;
pub use ideal_state::{IdealState, IdealStateBuilder, IdealStateError};
pub use identity::{
    IdentifierError, IdentifierKind, InstanceId, PartitionId, ResourceId, SessionId,
};
pub use replica_state::ReplicaStateError;
pub use session::{
    ActiveCurrentState, LiveInstance, ParticipantSessionSnapshot, ParticipantSessionState,
    SessionError,
};
pub use state::{State, StateCardinality, StateCardinalityError, StateError, Transition};
pub use state_model::{StateModelBuilder, StateModelDefinition, StateModelError};
