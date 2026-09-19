//! Application-side callback used by the rolling-restart showcase.

use clustodian::participant::{TransitionExecution, TransitionHandler, TransitionHandlerError};
use clustodian::{ResourceHandler, ResourceTransition, TransitionContext, TransitionError};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

pub const RESOURCE: &str = "ledger";
pub const PARTITION_COUNT: usize = 8;
pub const REPLICA_COUNT: usize = 3;
pub const PARTICIPANT_LEASE_TTL: Duration = Duration::from_secs(2);
pub const PARTICIPANTS: [&str; 3] = ["state-a", "state-b", "state-c"];

fn append_hit(path: &Path, line: String) {
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = file.write_all(line.as_bytes());
    }
}

pub fn preference_list(index: usize) -> Vec<String> {
    let offset = index % PARTICIPANTS.len();
    (0..PARTICIPANTS.len())
        .map(|step| PARTICIPANTS[(offset + step) % PARTICIPANTS.len()].to_owned())
        .collect()
}

pub fn preference_lists() -> std::collections::BTreeMap<String, Vec<String>> {
    (0..PARTITION_COUNT)
        .map(|index| (format!("{RESOURCE}_{index}"), preference_list(index)))
        .collect()
}

/// A process-local callback with an externally controlled pause point.
pub struct FencedCallback {
    instance: String,
    control_file: Option<PathBuf>,
    control_instance: Option<String>,
    release_file: Option<PathBuf>,
    hits_file: Option<PathBuf>,
}

impl FencedCallback {
    pub fn from_environment(instance: impl Into<String>) -> Self {
        Self {
            instance: instance.into(),
            control_file: std::env::var_os("ROLLING_CALLBACK_CONTROL").map(PathBuf::from),
            control_instance: std::env::var("ROLLING_CALLBACK_CONTROL_INSTANCE").ok(),
            release_file: std::env::var_os("ROLLING_CALLBACK_RELEASE").map(PathBuf::from),
            hits_file: std::env::var_os("ROLLING_CALLBACK_HITS").map(PathBuf::from),
        }
    }

    fn record(&self, phase: &str, transition: &ResourceTransition) {
        let Some(path) = &self.hits_file else { return };
        append_hit(
            path,
            format!(
                "instance={} phase={} message={} partition={} from={} to={}\n",
                self.instance,
                phase,
                transition.attempt_id(),
                transition.partition(),
                transition.source(),
                transition.target()
            ),
        );
    }

    fn record_execution(&self, phase: &str, execution: &TransitionExecution) {
        let Some(path) = &self.hits_file else { return };
        append_hit(
            path,
            format!(
                "instance={} phase={} message={} partition={} from={} to={}\n",
                self.instance,
                phase,
                execution.transition_id(),
                execution.partition(),
                execution.source_state(),
                execution.target_state()
            ),
        );
    }
}

impl ResourceHandler for FencedCallback {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        self.record("started", &transition);
        if match self.control_instance.as_ref() {
            None => true,
            Some(instance) => instance == &self.instance,
        } && self.control_file.as_ref().is_some_and(|path| path.exists())
        {
            let Some(release) = &self.release_file else {
                return Err(TransitionError::new(
                    "callback control file requires a release file",
                ));
            };
            while !release.exists() {
                thread::sleep(Duration::from_millis(20));
            }
        }
        self.record("finished", &transition);
        Ok(())
    }
}

impl TransitionHandler for FencedCallback {
    fn handle(&self, execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        if execution.resource().as_str() != RESOURCE {
            return Err(TransitionHandlerError::new(
                "unexpected rolling-restart resource",
            ));
        }
        self.record_execution("started", execution);
        if match self.control_instance.as_ref() {
            None => true,
            Some(instance) => instance == &self.instance,
        } && self.control_file.as_ref().is_some_and(|path| path.exists())
        {
            let Some(release) = &self.release_file else {
                return Err(TransitionHandlerError::new(
                    "callback control file requires a release file",
                ));
            };
            while !release.exists() {
                thread::sleep(Duration::from_millis(20));
            }
        }
        self.record_execution("finished", execution);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preference_lists_are_fixed_and_rotating() {
        assert_eq!(preference_list(0), ["state-a", "state-b", "state-c"]);
        assert_eq!(preference_list(1), ["state-b", "state-c", "state-a"]);
        assert_eq!(preference_lists().len(), PARTITION_COUNT);
        assert!(preference_lists()
            .values()
            .all(|list| list.len() == REPLICA_COUNT));
    }
}
