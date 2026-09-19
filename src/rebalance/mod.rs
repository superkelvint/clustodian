//! Deterministic desired-state calculation for explicit placements.

mod crush;
mod semi_auto;

pub use crush::{compute_crush_assignment, CrushError, CrushInstance, CrushTopology};
pub use semi_auto::{compute_semi_auto_best_possible_state, SemiAutoError};
