use clustodian::{ResourceHandler, ResourceTransition, TransitionContext, TransitionError};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// The small application state owned by one worker process.
#[derive(Clone, Debug, Default)]
pub struct WorkerState {
    roles: BTreeMap<String, String>,
    processed: BTreeMap<String, u64>,
}

impl WorkerState {
    pub fn role(&self, partition: &str) -> Option<&str> {
        self.roles.get(partition).map(String::as_str)
    }

    pub fn set_role(&mut self, partition: &str, role: &str) {
        if role == "DROPPED" {
            self.roles.remove(partition);
        } else {
            self.roles.insert(partition.to_owned(), role.to_owned());
        }
    }

    pub fn process(&mut self, partition: &str, job: &str) -> ProcessResult {
        if self.role(partition) != Some("LEADER") {
            return ProcessResult::NotOwner {
                role: self.role(partition).unwrap_or("OFFLINE").to_owned(),
            };
        }
        let count = self.processed.entry(partition.to_owned()).or_default();
        *count += 1;
        ProcessResult::Processed {
            job: job.to_owned(),
            count: *count,
        }
    }

    pub fn snapshot(&self, instance: &str) -> WorkerSnapshot {
        WorkerSnapshot {
            instance: instance.to_owned(),
            leaders: self
                .roles
                .iter()
                .filter(|(_, state)| state.as_str() == "LEADER")
                .map(|(partition, _)| partition.clone())
                .collect(),
            standbys: self
                .roles
                .iter()
                .filter(|(_, state)| state.as_str() == "STANDBY")
                .map(|(partition, _)| partition.clone())
                .collect(),
            roles: self.roles.clone(),
            processed: self.processed.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcessResult {
    Processed { job: String, count: u64 },
    NotOwner { role: String },
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkerSnapshot {
    pub instance: String,
    pub leaders: Vec<String>,
    pub standbys: Vec<String>,
    pub roles: BTreeMap<String, String>,
    pub processed: BTreeMap<String, u64>,
}

/// Transition callback for the worker's application ownership state.
#[derive(Clone)]
pub struct WorkerHandler {
    state: Arc<Mutex<WorkerState>>,
}

impl WorkerHandler {
    pub fn new(state: Arc<Mutex<WorkerState>>) -> Self {
        Self { state }
    }
}

impl ResourceHandler for WorkerHandler {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TransitionError::new("worker state lock poisoned"))?;
        state.set_role(
            transition.partition().as_str(),
            transition.target().as_str(),
        );
        Ok(())
    }
}

pub fn partition_names() -> impl Iterator<Item = String> {
    (0..3).map(|index| format!("job-queues_{index}"))
}

pub fn configured_workers() -> Vec<String> {
    std::env::var("JOB_WORKERS_WORKERS")
        .unwrap_or_else(|_| "worker-a,worker-b,worker-c".to_owned())
        .split(',')
        .map(str::trim)
        .filter(|worker| !worker.is_empty())
        .map(str::to_owned)
        .collect()
}

pub fn validate_partition(partition: &str) -> bool {
    partition_names().any(|candidate| candidate == partition)
}

pub fn valid_worker_names(workers: &[String]) -> Result<(), String> {
    let unique = workers.iter().collect::<BTreeSet<_>>();
    if workers.len() != 3 || unique.len() != workers.len() {
        return Err("exactly three distinct workers are required".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ProcessResult, WorkerState};

    #[test]
    fn only_leaders_process_jobs() {
        let mut state = WorkerState::default();
        state.set_role("job-queues_0", "STANDBY");
        assert_eq!(
            state.process("job-queues_0", "job-1"),
            ProcessResult::NotOwner {
                role: "STANDBY".to_owned()
            }
        );
        state.set_role("job-queues_0", "LEADER");
        assert_eq!(
            state.process("job-queues_0", "job-1"),
            ProcessResult::Processed {
                job: "job-1".to_owned(),
                count: 1
            }
        );
    }

    #[test]
    fn dropped_partitions_are_released() {
        let mut state = WorkerState::default();
        state.set_role("job-queues_0", "LEADER");
        state.set_role("job-queues_0", "DROPPED");
        assert_eq!(state.role("job-queues_0"), None);
    }
}
