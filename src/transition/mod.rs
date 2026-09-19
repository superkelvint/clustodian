//! Semantic and wire representations of state transitions.

mod message;
mod pending;
mod request;

pub use message::TransitionMessage;
pub use pending::PendingTransition;
pub use request::TransitionRequest;
