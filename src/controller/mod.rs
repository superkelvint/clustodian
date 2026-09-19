//! Deterministic controller stages and the etcd-backed controller runtime.

mod intermediate_state;
mod message_generation;
mod message_selection;
mod message_throttle;
mod runtime;

pub use intermediate_state::IntermediateState;
pub use message_generation::{generate_transitions, TransitionGenerationError};
pub use message_selection::{select_transitions, MessageSelectionError};
pub use message_throttle::{
    compute_intermediate_and_throttle, IntermediateThrottleError, IntermediateThrottleResult,
    OperationalPendingTransition, RebalanceType, ResourceTransitionInput,
    StateTransitionThrottleConfig, ThrottleScope,
};
pub(crate) use runtime::LeaderRunResult;
pub use runtime::{ControllerReconciler, ControllerRuntimeError, PublishedTransition};
