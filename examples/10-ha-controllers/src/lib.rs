use clustodian::model::PartitionId;
use clustodian::participant::{TransitionExecution, TransitionHandler, TransitionHandlerError};
use clustodian::{
    ResourceHandler, ResourceState, ResourceTransition, TransitionContext, TransitionError,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

pub const RESOURCE: &str = "control-work";
pub const PARTITIONS: [&str; 4] = [
    "control-work_0",
    "control-work_1",
    "control-work_2",
    "control-work_3",
];

#[derive(Clone, Default)]
pub struct ParticipantState(Arc<Mutex<BTreeMap<PartitionId, String>>>);

impl ParticipantState {
    pub fn snapshot(&self) -> BTreeMap<String, String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(p, s)| (p.to_string(), s.clone()))
            .collect()
    }
}

impl ResourceHandler for ParticipantState {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        if !PARTITIONS.contains(&transition.partition().as_str()) {
            return Err(TransitionError::new("unexpected HA demo transition"));
        }
        let mut states = self
            .0
            .lock()
            .map_err(|_| TransitionError::new("state mutex poisoned"))?;
        if matches!(transition.target(), ResourceState::Dropped) {
            states.remove(transition.partition());
        } else {
            states.insert(
                transition.partition().clone(),
                transition.target().to_string(),
            );
        }
        Ok(())
    }
}

impl TransitionHandler for ParticipantState {
    fn handle(&self, execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        if execution.resource().as_str() != RESOURCE
            || !PARTITIONS.contains(&execution.partition().as_str())
        {
            return Err(TransitionHandlerError::new("unexpected HA demo transition"));
        }
        let mut states = self
            .0
            .lock()
            .map_err(|_| TransitionHandlerError::new("state mutex poisoned"))?;
        if execution.target_state().is_dropped() {
            states.remove(execution.partition());
        } else {
            states.insert(
                execution.partition().clone(),
                execution.target_state().to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ParticipantState;
    #[test]
    fn participant_state_starts_empty() {
        assert!(ParticipantState::default().snapshot().is_empty());
    }
}
