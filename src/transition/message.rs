use serde::{Deserialize, Serialize};

/// The production wire representation of a participant transition message.
///
/// The queue is shared with the controller runtime, so this type deliberately
/// contains only coordination data.  Application work is represented by the
/// separate [`crate::participant::TransitionHandler`] boundary.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TransitionMessage {
    /// Stable identity assigned by the controller for one transition attempt.
    pub message_id: String,
    pub resource: String,
    pub partition: String,
    pub instance: String,
    pub target_session: u64,
    pub from: String,
    pub to: String,
    pub message_type: String,
}
