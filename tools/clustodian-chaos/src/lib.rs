//! Deterministic logical chaos traces and runtime validation primitives.

use clustodian::observe::ClusterSnapshot;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub mod minimizer;

pub const ARTIFACT_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_REPLAY_ATTEMPTS: u32 = 10;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClusterConfig {
    pub controllers: Vec<String>,
    pub participants: Vec<ParticipantConfig>,
    pub resources: Vec<ResourceConfig>,
    pub throttles: Vec<ThrottleConfig>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParticipantConfig {
    pub id: String,
    pub zone: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResourceConfig {
    pub name: String,
    pub partitions: Vec<String>,
    pub replicas: usize,
    pub placement: PlacementConfig,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PlacementConfig {
    Crush,
    SemiAuto {
        preference_lists: BTreeMap<String, Vec<String>>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ThrottleConfig {
    pub scope: String,
    pub rebalance_type: String,
    pub max_in_flight: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Action {
    StartController {
        id: String,
    },
    GracefulStopController {
        id: String,
    },
    CrashController {
        id: String,
    },
    RestartController {
        id: String,
    },
    StartParticipant {
        id: String,
    },
    GracefulStopParticipant {
        id: String,
    },
    CrashParticipant {
        id: String,
    },
    RestartParticipant {
        id: String,
    },
    AddParticipant {
        participant: ParticipantConfig,
    },
    RemoveParticipant {
        id: String,
    },
    CreateResource {
        resource: ResourceConfig,
    },
    ModifyPreferenceList {
        resource: String,
        partition: String,
        instances: Vec<String>,
    },
    ChangeThrottle {
        throttle: ThrottleConfig,
    },
    Wait {
        milliseconds: u64,
    },
    RequestQuiescence,
    WaitForConvergence {
        deadline_milliseconds: u64,
    },
    AssertIdle {
        seconds: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Fault {
    Network {
        target: NetworkTarget,
        toxic: NetworkToxic,
        duration_milliseconds: u64,
    },
    EtcdMemberRestart {
        member: String,
    },
    EtcdCompaction,
    Callback {
        participant: String,
        behavior: CallbackBehavior,
    },
    Clock {
        participant: String,
        perturbation: ClockPerturbation,
    },
    Failpoint {
        point: String,
        behavior: FailpointBehavior,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NetworkTarget {
    Controller { id: String },
    Participant { id: String },
    ControllersAndParticipants,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NetworkToxic {
    Disconnect,
    DirectionalDisconnect {
        direction: Direction,
    },
    Latency {
        milliseconds: u64,
        jitter_milliseconds: u64,
    },
    Timeout {
        milliseconds: u64,
    },
    ConnectionReset,
    Bandwidth {
        kilobytes_per_second: u64,
    },
    SlowClose {
        milliseconds: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Direction {
    Upstream,
    Downstream,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CallbackBehavior {
    SucceedImmediately,
    SucceedSlowly { milliseconds: u64 },
    Block { token: String },
    Error,
    RepeatedError { attempts: u32 },
    Panic,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ClockPerturbation {
    WallForward { milliseconds: i64 },
    WallBackward { milliseconds: i64 },
    MonotonicOffset { milliseconds: i64 },
    Suspend { milliseconds: u64 },
    DelayedKeepalive { milliseconds: u64 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FailpointBehavior {
    Pause { milliseconds: u64 },
    ReturnError,
    Panic,
    HardAbort,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Trace {
    pub schema_version: u32,
    pub seed: u64,
    pub profile: String,
    pub initial_config: ClusterConfig,
    pub actions: Vec<Action>,
    pub faults: Vec<ScheduledFault>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScheduledFault {
    pub action_index: usize,
    pub fault: Fault,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FaultBudget {
    pub etcd_failures_remaining: u32,
    pub controller_failures_remaining: u32,
    pub participant_failures_remaining: u32,
    pub network_partitions_remaining: u32,
    pub clock_faults_remaining: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FaultAdmission {
    Admitted,
    Rejected(String),
}

impl FaultBudget {
    pub fn admit(&mut self, fault: &Fault) -> FaultAdmission {
        let remaining = match fault {
            Fault::EtcdMemberRestart { .. } | Fault::EtcdCompaction => {
                &mut self.etcd_failures_remaining
            }
            Fault::Network { .. } => &mut self.network_partitions_remaining,
            Fault::Callback { .. } | Fault::Failpoint { .. } => return FaultAdmission::Admitted,
            Fault::Clock { .. } => &mut self.clock_faults_remaining,
        };
        if *remaining == 0 {
            return FaultAdmission::Rejected(String::from("fault budget exhausted"));
        }
        *remaining -= 1;
        FaultAdmission::Admitted
    }

    pub fn admit_controller_failure(&mut self) -> FaultAdmission {
        admit_counter(&mut self.controller_failures_remaining)
    }

    pub fn admit_participant_failure(&mut self) -> FaultAdmission {
        admit_counter(&mut self.participant_failures_remaining)
    }
}

fn admit_counter(counter: &mut u32) -> FaultAdmission {
    if *counter == 0 {
        FaultAdmission::Rejected(String::from("fault budget exhausted"))
    } else {
        *counter -= 1;
        FaultAdmission::Admitted
    }
}

#[derive(Clone, Debug)]
pub struct SeededGenerator {
    seed: u64,
    state: u64,
}

impl SeededGenerator {
    pub fn new(seed: u64) -> Self {
        Self { seed, state: seed }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn generate(&mut self, profile: &str, steps: usize) -> Trace {
        let initial_config = config_for_profile(profile);
        let mut actions = initial_config
            .controllers
            .iter()
            .cloned()
            .map(|id| Action::StartController { id })
            .collect::<Vec<_>>();
        actions.extend(initial_config.participants.iter().map(|participant| {
            Action::StartParticipant {
                id: participant.id.clone(),
            }
        }));
        // Let lease-backed participant registration reach etcd before the first
        // CRUSH reconciliation.  This keeps generated startup traces focused
        // on runtime faults instead of an avoidable empty-placement race.
        actions.push(Action::Wait { milliseconds: 500 });
        actions.extend(
            initial_config
                .resources
                .iter()
                .cloned()
                .map(|resource| Action::CreateResource { resource }),
        );
        if is_scale_profile(profile) {
            actions.push(Action::WaitForConvergence {
                deadline_milliseconds: 300_000,
            });
            actions.push(Action::AssertIdle { seconds: 30 });
            return Trace {
                schema_version: ARTIFACT_SCHEMA_VERSION,
                seed: self.seed,
                profile: profile.to_owned(),
                initial_config,
                actions,
                faults: Vec::new(),
            };
        }
        // Exercise the graceful lifecycle paths in every ordinary generated
        // run.  The controller intentionally relies on lease expiry after
        // SIGTERM, while the participant has an explicit session revoke path.
        actions.extend([
            Action::GracefulStopParticipant {
                id: String::from("node-4"),
            },
            Action::RestartParticipant {
                id: String::from("node-4"),
            },
            Action::GracefulStopController {
                id: String::from("controller-c"),
            },
            Action::RestartController {
                id: String::from("controller-c"),
            },
            Action::WaitForConvergence {
                deadline_milliseconds: 30_000,
            },
        ]);
        let mut faults = Vec::new();
        let controller_failure_limit = if profile == "pr" { 3 } else { 8 };
        let participant_failure_limit = if profile == "pr" { 3 } else { 8 };
        let mut controller_failures = 0;
        let mut participant_failures = 0;
        for index in 0..steps {
            if index % 13 == 0 && controller_failures < controller_failure_limit {
                let id = self.controller_id();
                actions.push(Action::CrashController { id: id.clone() });
                actions.push(Action::RestartController { id });
                controller_failures += 1;
            } else if index % 7 == 0 && participant_failures < participant_failure_limit {
                let id = self.participant_id();
                actions.push(Action::CrashParticipant { id: id.clone() });
                actions.push(Action::RestartParticipant { id });
                participant_failures += 1;
            } else {
                let choice = self.next_u64() % 8;
                actions.push(match choice {
                    0 => Action::Wait { milliseconds: 50 },
                    1 => Action::RequestQuiescence,
                    2 => Action::WaitForConvergence {
                        deadline_milliseconds: 30_000,
                    },
                    3 => Action::ChangeThrottle {
                        throttle: ThrottleConfig {
                            scope: String::from("CLUSTER"),
                            rebalance_type: String::from("ANY"),
                            max_in_flight: (self.next_u64() % 3 + 1) as usize,
                        },
                    },
                    4 => Action::AddParticipant {
                        participant: ParticipantConfig {
                            id: String::from("node-5"),
                            zone: String::from("zone-2"),
                        },
                    },
                    5 => Action::RemoveParticipant {
                        id: String::from("node-5"),
                    },
                    6 => Action::ModifyPreferenceList {
                        resource: String::from("cache"),
                        partition: format!("cache_{}", self.next_u64() % 2),
                        instances: {
                            let offset = self.next_u64() % 4;
                            (0..2)
                                .map(|index| format!("node-{}", ((offset + index) % 4) + 1))
                                .collect()
                        },
                    },
                    _ => Action::StartParticipant {
                        id: self.participant_id(),
                    },
                });
            }
            if let Some(fault) = generated_fault(profile, index) {
                if requires_fault_hit(&fault) {
                    // Restore node-1 to the preference list, then remove it
                    // while the surgical fault is active.  This gives the
                    // callback/failpoint boundary a deterministic trigger.
                    actions.push(Action::ModifyPreferenceList {
                        resource: String::from("cache"),
                        partition: String::from("cache_0"),
                        instances: vec![String::from("node-1"), String::from("node-2")],
                    });
                    actions.push(Action::ModifyPreferenceList {
                        resource: String::from("cache"),
                        partition: String::from("cache_0"),
                        instances: vec![String::from("node-2"), String::from("node-3")],
                    });
                }
                let callback_panics = matches!(
                    &fault,
                    Fault::Callback {
                        behavior: CallbackBehavior::Panic,
                        ..
                    }
                );
                faults.push(ScheduledFault {
                    action_index: actions.len() - 1,
                    fault,
                });
                if callback_panics {
                    actions.push(Action::RestartParticipant {
                        id: String::from("node-1"),
                    });
                }
            }
        }
        Trace {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            seed: self.seed,
            profile: profile.to_owned(),
            initial_config,
            actions,
            faults,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }

    fn controller_id(&mut self) -> String {
        ["controller-a", "controller-b", "controller-c"][(self.next_u64() % 3) as usize].to_owned()
    }

    fn participant_id(&mut self) -> String {
        format!("node-{}", (self.next_u64() % 4) + 1)
    }
}

fn requires_fault_hit(fault: &Fault) -> bool {
    matches!(fault, Fault::Callback { .. } | Fault::Failpoint { .. })
}

#[allow(clippy::manual_is_multiple_of)]
fn generated_fault(profile: &str, index: usize) -> Option<Fault> {
    let nightly = profile != "pr";
    let matrix_size = if nightly { 11 } else { 6 };
    let matrix_index = if index < matrix_size {
        Some(index)
    } else if index % 17 == 0 {
        Some(index % matrix_size)
    } else {
        None
    }?;
    let callback_participant = String::from("node-1");
    let fault = match matrix_index {
        0 => {
            if nightly {
                Fault::EtcdCompaction
            } else {
                Fault::Callback {
                    participant: callback_participant,
                    behavior: CallbackBehavior::SucceedSlowly { milliseconds: 20 },
                }
            }
        }
        1 => {
            if nightly {
                Fault::Callback {
                    participant: callback_participant,
                    behavior: CallbackBehavior::Error,
                }
            } else {
                Fault::Callback {
                    participant: callback_participant,
                    behavior: CallbackBehavior::Block {
                        token: format!("{profile}-{index}"),
                    },
                }
            }
        }
        2 => {
            if nightly {
                Fault::Network {
                    target: NetworkTarget::Participant {
                        id: String::from("node-1"),
                    },
                    toxic: NetworkToxic::Latency {
                        milliseconds: 100,
                        jitter_milliseconds: 25,
                    },
                    duration_milliseconds: 300,
                }
            } else {
                Fault::Callback {
                    participant: callback_participant,
                    behavior: CallbackBehavior::RepeatedError { attempts: 3 },
                }
            }
        }
        3 => {
            if nightly {
                Fault::EtcdMemberRestart {
                    member: String::from("etcd1"),
                }
            } else {
                Fault::Callback {
                    participant: callback_participant,
                    behavior: CallbackBehavior::Panic,
                }
            }
        }
        4 => Fault::Clock {
            participant: String::from("node-1"),
            perturbation: ClockPerturbation::Suspend { milliseconds: 300 },
        },
        5 => {
            if nightly {
                Fault::Failpoint {
                    point: String::from("controller_after_reconcile_before_publish"),
                    behavior: FailpointBehavior::Pause { milliseconds: 100 },
                }
            } else {
                Fault::Failpoint {
                    point: String::from("participant_after_callback_success_before_current_state"),
                    behavior: FailpointBehavior::Pause { milliseconds: 100 },
                }
            }
        }
        6 => Fault::Callback {
            participant: callback_participant,
            behavior: CallbackBehavior::Block {
                token: format!("{profile}-{index}"),
            },
        },
        7 => Fault::Network {
            target: NetworkTarget::Controller {
                id: String::from("controller-a"),
            },
            toxic: NetworkToxic::Disconnect,
            duration_milliseconds: 300,
        },
        8 => Fault::Callback {
            participant: callback_participant,
            behavior: CallbackBehavior::Panic,
        },
        9 => Fault::Callback {
            participant: callback_participant,
            behavior: CallbackBehavior::RepeatedError { attempts: 3 },
        },
        10 => Fault::Network {
            target: NetworkTarget::Participant {
                id: String::from("node-1"),
            },
            toxic: NetworkToxic::Timeout { milliseconds: 250 },
            duration_milliseconds: 300,
        },
        _ => return None,
    };
    Some(fault)
}

fn is_scale_profile(profile: &str) -> bool {
    matches!(profile, "scale-100-10k" | "scale-250-50k")
}

fn config_for_profile(profile: &str) -> ClusterConfig {
    match profile {
        "scale-100-10k" => scale_config(100, 100, 10_000),
        "scale-250-50k" => scale_config(250, 250, 50_000),
        _ => default_config(),
    }
}

fn scale_config(
    participant_count: usize,
    resource_count: usize,
    partition_count: usize,
) -> ClusterConfig {
    let partitions_per_resource = partition_count / resource_count;
    assert_eq!(
        partitions_per_resource * resource_count,
        partition_count,
        "scale profile must divide partitions evenly"
    );
    ClusterConfig {
        controllers: (0..3).map(|index| format!("controller-{index}")).collect(),
        participants: (0..participant_count)
            .map(|index| ParticipantConfig {
                id: format!("node-{index}"),
                zone: format!("zone-{}", index % 10),
            })
            .collect(),
        resources: (0..resource_count)
            .map(|resource| ResourceConfig {
                name: format!("resource-{resource}"),
                partitions: (0..partitions_per_resource)
                    .map(|partition| format!("resource-{resource}_{partition}"))
                    .collect(),
                replicas: 3,
                placement: PlacementConfig::Crush,
            })
            .collect(),
        throttles: Vec::new(),
    }
}

fn default_config() -> ClusterConfig {
    ClusterConfig {
        controllers: ["controller-a", "controller-b", "controller-c"]
            .into_iter()
            .map(String::from)
            .collect(),
        participants: (0..4)
            .map(|index| ParticipantConfig {
                id: format!("node-{}", index + 1),
                zone: format!("zone-{}", (index % 3) + 1),
            })
            .collect(),
        resources: vec![
            ResourceConfig {
                name: String::from("service"),
                partitions: (0..4).map(|index| format!("service_{index}")).collect(),
                replicas: 3,
                placement: PlacementConfig::Crush,
            },
            ResourceConfig {
                name: String::from("cache"),
                partitions: (0..2).map(|index| format!("cache_{index}")).collect(),
                replicas: 2,
                placement: PlacementConfig::SemiAuto {
                    preference_lists: (0..2)
                        .map(|index| {
                            (
                                format!("cache_{index}"),
                                (1..=2).map(|node| format!("node-{node}")).collect(),
                            )
                        })
                        .collect(),
                },
            },
        ],
        throttles: Vec::new(),
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QuiescenceInputs {
    pub no_action_executing: bool,
    pub no_fault_awaiting_completion: bool,
    pub stopped_processes_resumed: bool,
    pub toxics_removed: bool,
    pub network_paths_restored: bool,
    pub callback_barriers_released: bool,
    pub no_hanging_callbacks: bool,
    pub etcd_quorum_available: bool,
    pub etcd_endpoints_reachable: bool,
    pub eligible_controller_running: bool,
    pub minimum_participants_healthy: bool,
    pub clock_perturbations_removed: bool,
    pub observer_connected: bool,
    pub observer_caught_up: bool,
}

impl QuiescenceInputs {
    pub fn is_quiescent(&self) -> bool {
        self.no_action_executing
            && self.no_fault_awaiting_completion
            && self.stopped_processes_resumed
            && self.toxics_removed
            && self.network_paths_restored
            && self.callback_barriers_released
            && self.no_hanging_callbacks
            && self.etcd_quorum_available
            && self.etcd_endpoints_reachable
            && self.eligible_controller_running
            && self.minimum_participants_healthy
            && self.clock_perturbations_removed
            && self.observer_connected
            && self.observer_caught_up
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvariantViolation {
    pub class: InvariantClass,
    pub name: String,
    pub detail: String,
    pub observer_revision: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum InvariantClass {
    Safety,
    DerivedState,
    Convergence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DerivedCheck {
    Valid,
    Deferred,
    Invalid(String),
}

pub fn check_safety(snapshot: &ClusterSnapshot) -> Vec<InvariantViolation> {
    let revision = snapshot.observer_revision.value();
    let mut violations = Vec::new();
    if snapshot.controllers.active.len() > 1 {
        violations.push(violation(
            InvariantClass::Safety,
            "single_authoritative_controller",
            "more than one active controller",
            revision,
        ));
    }
    for (instance, current) in &snapshot.active_current_state {
        match snapshot.live_instances.get(instance) {
            Some(session) if *session == current.session => {}
            Some(session) => violations.push(violation(
                InvariantClass::Safety,
                "current_state_session",
                &format!(
                    "{instance} has session {} but live session is {session}",
                    current.session
                ),
                revision,
            )),
            None => violations.push(violation(
                InvariantClass::Safety,
                "current_state_live_instance",
                &format!("{instance} has CurrentState without a live session"),
                revision,
            )),
        }
    }
    let mut current_state_leaders = BTreeMap::<(String, String), Vec<String>>::new();
    for (instance, current) in &snapshot.active_current_state {
        for (resource, partitions) in &current.resources {
            for (partition, state) in partitions {
                if state == "LEADER" {
                    current_state_leaders
                        .entry((resource.clone(), partition.clone()))
                        .or_default()
                        .push(instance.clone());
                }
            }
        }
    }
    for ((resource, partition), leaders) in current_state_leaders {
        if leaders.len() > 1 {
            violations.push(violation(
                InvariantClass::Safety,
                "current_state_single_leader",
                &format!(
                    "{resource}/{partition} has {} active CurrentState leaders: {leaders:?}",
                    leaders.len()
                ),
                revision,
            ));
        }
    }
    // LiveInstance registration and the controller-owned queue are separate
    // etcd keys. A session replacement can therefore be visible before the
    // controller has reconciled its queue. The participant completion fence
    // makes such a message non-executable, so this is convergence lag rather
    // than a safety violation until the controller's watermark catches up.
    if snapshot
        .processed_revision
        .is_some_and(|processed| processed.value() >= snapshot.authoritative_revision.value())
    {
        for transition in &snapshot.pending_transitions {
            if snapshot.live_instances.get(&transition.instance) != Some(&transition.target_session)
            {
                violations.push(violation(
                    InvariantClass::Safety,
                    "pending_transition_session",
                    &format!("{} targets stale session", transition.instance),
                    revision,
                ));
            }
        }
    }
    if let Some(external) = snapshot.external_view.as_object() {
        for (resource, partitions) in external {
            if let Some(partitions) = partitions.as_object() {
                for (partition, states) in partitions {
                    let leaders = states
                        .as_object()
                        .into_iter()
                        .flat_map(|states| states.iter())
                        .filter(|(instance, state)| {
                            state.as_str() == Some("LEADER")
                                && snapshot.live_instances.contains_key(*instance)
                        })
                        .count();
                    if leaders > 1 {
                        violations.push(violation(
                            InvariantClass::Safety,
                            "single_leader",
                            &format!("{resource}/{partition} has {leaders} leaders"),
                            revision,
                        ));
                    }
                }
            }
        }
    }
    violations
}

pub fn check_derived_state(snapshot: &ClusterSnapshot) -> DerivedCheck {
    if !snapshot
        .processed_revision
        .is_some_and(|processed| processed.value() >= snapshot.authoritative_revision.value())
    {
        return DerivedCheck::Deferred;
    }
    let expected = aggregate_active_current_state(snapshot);
    if snapshot.external_view == expected {
        DerivedCheck::Valid
    } else {
        DerivedCheck::Invalid(format!(
            "ExternalView differs from active CurrentState: expected={expected}, observed={}",
            snapshot.external_view
        ))
    }
}

fn aggregate_active_current_state(snapshot: &ClusterSnapshot) -> serde_json::Value {
    let mut aggregate = BTreeMap::<String, BTreeMap<String, BTreeMap<String, String>>>::new();
    for (instance, current) in &snapshot.active_current_state {
        for (resource, partitions) in &current.resources {
            for (partition, state) in partitions {
                aggregate
                    .entry(resource.clone())
                    .or_default()
                    .entry(partition.clone())
                    .or_default()
                    .insert(instance.clone(), state.clone());
            }
        }
    }
    serde_json::to_value(aggregate).expect("BTreeMap state aggregation is serializable")
}

/// Check the settled-state contract after a quiescence/convergence action.
pub fn check_convergence(
    snapshot: &ClusterSnapshot,
    config: &ClusterConfig,
) -> Vec<InvariantViolation> {
    let revision = snapshot.observer_revision.value();
    let mut violations = Vec::new();
    if snapshot.controllers.active.len() != 1 {
        violations.push(violation(
            InvariantClass::Convergence,
            "one_active_controller",
            &format!(
                "expected one active controller, got {:?}",
                snapshot.controllers.active
            ),
            revision,
        ));
    }
    if !snapshot.pending_transitions.is_empty() {
        violations.push(violation(
            InvariantClass::Convergence,
            "no_pending_transitions",
            &format!(
                "{} pending transitions remain",
                snapshot.pending_transitions.len()
            ),
            revision,
        ));
    }
    if !snapshot
        .processed_revision
        .is_some_and(|processed| processed.value() >= snapshot.authoritative_revision.value())
    {
        violations.push(violation(
            InvariantClass::Convergence,
            "controller_caught_up",
            "controller processed revision is behind authoritative revision",
            revision,
        ));
    }
    if let DerivedCheck::Invalid(detail) = check_derived_state(snapshot) {
        violations.push(violation(
            InvariantClass::DerivedState,
            "external_view_matches_current_state",
            &detail,
            revision,
        ));
    }
    match expected_best_possible_state(config, snapshot) {
        Ok(expected) if snapshot.external_view != expected => violations.push(violation(
            InvariantClass::Convergence,
            "external_view_matches_best_possible_state",
            &format!(
                "expected BestPossibleState={expected}, observed ExternalView={}",
                snapshot.external_view
            ),
            revision,
        )),
        Ok(_) => {}
        Err(detail) => violations.push(violation(
            InvariantClass::Convergence,
            "best_possible_state_oracle",
            &detail,
            revision,
        )),
    }

    let external = match serde_json::from_value::<
        BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>,
    >(snapshot.external_view.clone())
    {
        Ok(external) => external,
        Err(error) => {
            violations.push(violation(
                InvariantClass::DerivedState,
                "external_view_shape",
                &format!("ExternalView is not a replica map: {error}"),
                revision,
            ));
            return violations;
        }
    };
    let observed_routing = snapshot
        .routing_results
        .iter()
        .map(|result| {
            (
                (
                    result.resource.clone(),
                    result.partition.clone(),
                    result.state.clone(),
                ),
                result.instances.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut expected_routing = BTreeMap::new();
    for (resource, partitions) in &external {
        for (partition, instances) in partitions {
            let mut states = BTreeMap::<String, Vec<String>>::new();
            for (instance, state) in instances {
                states
                    .entry(state.clone())
                    .or_default()
                    .push(instance.clone());
            }
            for (state, mut instances) in states {
                instances.sort();
                expected_routing.insert((resource.clone(), partition.clone(), state), instances);
            }
        }
    }
    if observed_routing != expected_routing {
        violations.push(violation(
            InvariantClass::DerivedState,
            "routing_matches_external_view",
            "routing results differ from ExternalView",
            revision,
        ));
    }

    for resource in &config.resources {
        for partition in &resource.partitions {
            let eligible_live = eligible_live_count(config, resource, partition, snapshot);
            let expected_replicas = resource.replicas.min(eligible_live);
            let Some(instances) = external
                .get(&resource.name)
                .and_then(|partitions| partitions.get(partition))
            else {
                if expected_replicas > 0 {
                    violations.push(violation(
                        InvariantClass::Convergence,
                        "settled_partition_present",
                        &format!("missing settled partition {}/{}", resource.name, partition),
                        revision,
                    ));
                }
                continue;
            };
            if instances.len() != expected_replicas {
                violations.push(violation(
                    InvariantClass::Convergence,
                    "settled_replica_cardinality",
                    &format!(
                        "{}/{} expected {} replicas, observed {}",
                        resource.name,
                        partition,
                        expected_replicas,
                        instances.len()
                    ),
                    revision,
                ));
            }
            let leaders = instances
                .values()
                .filter(|state| state.as_str() == "LEADER")
                .count();
            if expected_replicas > 0 && leaders != 1 {
                violations.push(violation(
                    InvariantClass::Convergence,
                    "settled_single_leader",
                    &format!(
                        "{}/{} expected one leader, observed {}",
                        resource.name, partition, leaders
                    ),
                    revision,
                ));
            }
        }
    }
    violations
}

fn expected_best_possible_state(
    config: &ClusterConfig,
    snapshot: &ClusterSnapshot,
) -> Result<serde_json::Value, String> {
    let live = snapshot
        .live_instances
        .keys()
        .map(|instance| {
            clustodian::model::InstanceId::new(instance.clone())
                .map_err(|error| format!("invalid live instance {instance}: {error}"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let model = clustodian::model::leader_standby();
    let mut expected = BTreeMap::<String, BTreeMap<String, BTreeMap<String, String>>>::new();

    for resource in &config.resources {
        let resource_id = clustodian::model::ResourceId::new(resource.name.clone())
            .map_err(|error| format!("invalid resource {}: {error}", resource.name))?;
        let partitions = resource
            .partitions
            .iter()
            .map(|partition| {
                clustodian::model::PartitionId::new(partition.clone()).map_err(|error| {
                    format!("invalid partition {}/{}: {error}", resource.name, partition)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let preference_lists = match &resource.placement {
            PlacementConfig::Crush => {
                let instances = config
                    .participants
                    .iter()
                    .map(|participant| {
                        clustodian::rebalance::CrushInstance::new(
                            clustodian::model::InstanceId::new(participant.id.clone()).map_err(
                                |error| format!("invalid instance {}: {error}", participant.id),
                            )?,
                            participant.zone.clone(),
                        )
                        .map_err(|error| format!("CRUSH instance {}: {error}", participant.id))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let topology =
                    clustodian::rebalance::CrushTopology::new("/instance", "instance", "instance")
                        .map_err(|error| format!("CRUSH topology: {error}"))?;
                clustodian::rebalance::compute_crush_assignment(
                    &resource_id,
                    &partitions,
                    resource.replicas.min(live.len()),
                    &instances,
                    &live,
                    &topology,
                )
                .map_err(|error| format!("CRUSH assignment for {}: {error}", resource.name))?
            }
            PlacementConfig::SemiAuto { preference_lists } => preference_lists
                .iter()
                .map(|(partition, instances)| {
                    Ok((
                        clustodian::model::PartitionId::new(partition.clone()).map_err(
                            |error| {
                                format!(
                                    "invalid partition {}/{}: {error}",
                                    resource.name, partition
                                )
                            },
                        )?,
                        instances
                            .iter()
                            .map(|instance| {
                                clustodian::model::InstanceId::new(instance.clone()).map_err(
                                    |error| format!("invalid instance {instance}: {error}"),
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, String>>()?,
        };
        let mut ideal_builder = clustodian::model::IdealState::builder(
            resource_id.clone(),
            match &resource.placement {
                PlacementConfig::Crush => resource.replicas.min(live.len()),
                PlacementConfig::SemiAuto { .. } => resource.replicas,
            },
        );
        for (partition, instances) in preference_lists {
            ideal_builder
                .set_preference_list(partition, instances)
                .map_err(|error| {
                    format!("BestPossibleState input for {}: {error}", resource.name)
                })?;
        }
        let ideal = ideal_builder
            .build()
            .map_err(|error| format!("BestPossibleState ideal for {}: {error}", resource.name))?;
        let best = clustodian::rebalance::compute_semi_auto_best_possible_state(
            &ideal,
            &clustodian::model::CurrentState::default(),
            &live,
            &model,
        )
        .map_err(|error| format!("BestPossibleState for {}: {error}", resource.name))?;
        for (partition, instances) in best.entries() {
            let output = expected.entry(resource.name.clone()).or_default();
            let partition_output = output.entry(partition.to_string()).or_default();
            for (instance, state) in instances {
                partition_output.insert(instance.to_string(), state.to_string());
            }
        }
    }
    serde_json::to_value(expected).map_err(|error| format!("serialize BestPossibleState: {error}"))
}

fn eligible_live_count(
    config: &ClusterConfig,
    resource: &ResourceConfig,
    partition: &str,
    snapshot: &ClusterSnapshot,
) -> usize {
    match &resource.placement {
        PlacementConfig::Crush => config
            .participants
            .iter()
            .filter(|participant| snapshot.live_instances.contains_key(&participant.id))
            .count(),
        PlacementConfig::SemiAuto { preference_lists } => preference_lists
            .get(partition)
            .into_iter()
            .flat_map(|instances| instances.iter())
            .filter(|instance| snapshot.live_instances.contains_key(*instance))
            .count(),
    }
}

fn violation(class: InvariantClass, name: &str, detail: &str, revision: i64) -> InvariantViolation {
    InvariantViolation {
        class,
        name: name.to_owned(),
        detail: detail.to_owned(),
        observer_revision: revision,
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FailureArtifact {
    pub artifact_schema_version: u32,
    pub seed: u64,
    pub source_revision: String,
    pub build_identity: String,
    pub trace: Trace,
    pub events: Vec<RecordedEvent>,
    pub invariant_failure: Option<InvariantViolation>,
    pub quiescence: Vec<QuiescenceRecord>,
    pub evidence_directory: String,
    pub replay: ReplayStats,
    #[serde(default)]
    pub process_ids: BTreeMap<String, i32>,
    #[serde(default)]
    pub observer_revisions: Vec<i64>,
    #[serde(default)]
    pub invariant_snapshots: Vec<serde_json::Value>,
    #[serde(default)]
    pub callback_state: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub failpoints_active: Vec<String>,
    #[serde(default)]
    pub clock_perturbations: Vec<ClockPerturbation>,
    #[serde(default)]
    pub toxiproxy_url: Option<String>,
    #[serde(default)]
    pub fault_activations: Vec<FaultActivation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecordedEvent {
    pub sequence: u64,
    pub monotonic_millis: u128,
    pub kind: String,
    pub detail: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QuiescenceRecord {
    pub monotonic_millis: u128,
    pub observer_revision: i64,
    pub authoritative_revision: i64,
    pub processed_revision: Option<i64>,
    pub inputs: QuiescenceInputs,
    pub stable: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FaultActivation {
    pub key: String,
    pub configured: bool,
    pub hit_count: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeMetricSample {
    pub action_index: usize,
    pub action: String,
    pub action_latency_millis: u128,
    pub observer_revision: i64,
    pub semantic_revision_delta: i64,
    pub pending_transition_count: usize,
    pub external_view_bytes: usize,
    pub process_count: usize,
    pub task_count: usize,
    pub rss_bytes: u64,
    pub open_fd_count: usize,
    pub watch_event_count: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReplayStats {
    pub attempts: u32,
    pub reproduced: u32,
    pub reproduction_rate: f64,
    pub classification: FailureClassification,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FailureClassification {
    Stable,
    Intermittent,
    NotYetReproduced,
}

impl ReplayStats {
    pub fn new(attempts: u32, reproduced: u32) -> Self {
        let reproduction_rate = if attempts == 0 {
            0.0
        } else {
            f64::from(reproduced) / f64::from(attempts)
        };
        let classification = if reproduced == attempts && attempts > 0 {
            FailureClassification::Stable
        } else if reproduced == 0 {
            FailureClassification::NotYetReproduced
        } else {
            FailureClassification::Intermittent
        };
        Self {
            attempts,
            reproduced,
            reproduction_rate,
            classification,
        }
    }
}

pub async fn write_json<T: Serialize>(path: impl AsRef<Path>, value: &T) -> Result<(), ChaosError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(ChaosError::Serialization)?;
    tokio::fs::write(path, bytes).await.map_err(ChaosError::Io)
}

pub async fn read_json<T: for<'de> Deserialize<'de>>(
    path: impl AsRef<Path>,
) -> Result<T, ChaosError> {
    let bytes = tokio::fs::read(path).await.map_err(ChaosError::Io)?;
    serde_json::from_slice(&bytes).map_err(ChaosError::Serialization)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessRole {
    Controller,
    Participant,
}

fn endpoint_prefix(endpoint: &str) -> Option<String> {
    let (scheme, authority) = endpoint.split_once("://")?;
    let host = authority.rsplit_once(':')?.0;
    Some(format!("{scheme}://{host}"))
}

fn proxy_ports(role: ProcessRole, id: &str) -> Option<Vec<u16>> {
    let slot = match role {
        ProcessRole::Controller => {
            let suffix = id.strip_prefix("controller-")?;
            if let Some(letter) = suffix.strip_prefix('a') {
                if letter.is_empty() {
                    Some((0, 12_379_u16))
                } else {
                    None
                }
            } else if suffix.len() == 1 && suffix.as_bytes()[0].is_ascii_lowercase() {
                Some((usize::from(suffix.as_bytes()[0] - b'a'), 12_379))
            } else {
                suffix.parse::<usize>().ok().map(|index| (index, 12_388))
            }
        }
        ProcessRole::Participant => {
            let index = id.strip_prefix("node-")?.parse::<usize>().ok()?;
            Some((index, 13_379))
        }
    }?;
    let start = slot
        .1
        .checked_add(u16::try_from(slot.0.checked_mul(3)?).ok()?)?;
    Some(vec![start, start.checked_add(1)?, start.checked_add(2)?])
}

/// Owns the real controller, participant, and observer child processes.
pub struct ProcessSupervisor {
    node_binary: PathBuf,
    work_directory: PathBuf,
    cluster: String,
    prefix: String,
    controller_endpoints: String,
    participant_endpoints: String,
    observer_endpoints: String,
    use_process_specific_endpoints: bool,
    controllers: BTreeMap<String, Child>,
    participants: BTreeMap<String, Child>,
    expected_controllers: BTreeSet<String>,
    expected_participants: BTreeSet<String>,
    observer: Option<Child>,
    suspended: BTreeSet<String>,
    active_toxics: BTreeSet<String>,
}

impl ProcessSupervisor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_binary: impl Into<PathBuf>,
        work_directory: impl Into<PathBuf>,
        cluster: String,
        prefix: String,
        controller_endpoints: String,
        participant_endpoints: String,
        observer_endpoints: String,
        use_process_specific_endpoints: bool,
    ) -> Result<Self, ChaosError> {
        let work_directory = work_directory.into();
        std::fs::create_dir_all(&work_directory).map_err(ChaosError::Io)?;
        std::fs::write(work_directory.join("fault-hits.jsonl"), []).map_err(ChaosError::Io)?;
        std::fs::write(work_directory.join("fault-activations.jsonl"), [])
            .map_err(ChaosError::Io)?;
        Ok(Self {
            node_binary: node_binary.into(),
            work_directory,
            cluster,
            prefix,
            controller_endpoints,
            participant_endpoints,
            observer_endpoints,
            use_process_specific_endpoints,
            controllers: BTreeMap::new(),
            participants: BTreeMap::new(),
            expected_controllers: BTreeSet::new(),
            expected_participants: BTreeSet::new(),
            observer: None,
            suspended: BTreeSet::new(),
            active_toxics: BTreeSet::new(),
        })
    }

    pub fn start_observer(&mut self) -> Result<(), ChaosError> {
        if self.observer.as_mut().is_some_and(child_running) {
            return Ok(());
        }
        let events = self.work_directory.join("observer-events.jsonl");
        let log = append_log(&self.work_directory.join("observer.log"))?;
        let child = Command::new(&self.node_binary)
            .envs(self.common_environment(&self.observer_endpoints))
            .env("CLUSTODIAN_CHAOS_NODE_MODE", "observer")
            .env("CLUSTODIAN_CHAOS_OBSERVER_EVENTS", events)
            .env(
                "CLUSTODIAN_CHAOS_FAILPOINTS_FILE",
                self.work_directory.join("failpoints.txt"),
            )
            .stdout(Stdio::from(log.try_clone().map_err(ChaosError::Io)?))
            .stderr(Stdio::from(log))
            .spawn()
            .map_err(ChaosError::Io)?;
        self.observer = Some(child);
        self.persist_process_ids()?;
        Ok(())
    }

    pub fn start_controller(&mut self, id: &str) -> Result<(), ChaosError> {
        if self.controllers.get_mut(id).is_some_and(child_running) {
            return Ok(());
        }
        let log = append_log(&self.work_directory.join(format!("controller-{id}.log")))?;
        let child = Command::new(&self.node_binary)
            .envs(self.common_environment(&self.process_endpoints(
                &self.controller_endpoints,
                ProcessRole::Controller,
                id,
            )))
            .env("CLUSTODIAN_CHAOS_NODE_MODE", "controller")
            .env("CLUSTODIAN_CHAOS_CONTROLLER_ID", id)
            .env(
                "CLUSTODIAN_CHAOS_FAILPOINTS_FILE",
                self.work_directory.join("failpoints.txt"),
            )
            .stdout(Stdio::from(log.try_clone().map_err(ChaosError::Io)?))
            .stderr(Stdio::from(log))
            .spawn()
            .map_err(ChaosError::Io)?;
        self.controllers.insert(id.to_owned(), child);
        self.expected_controllers.insert(id.to_owned());
        self.persist_process_ids()?;
        Ok(())
    }

    pub fn start_participant(&mut self, participant: &ParticipantConfig) -> Result<(), ChaosError> {
        if self
            .participants
            .get_mut(&participant.id)
            .is_some_and(child_running)
        {
            return Ok(());
        }
        let callback_file = self
            .work_directory
            .join(format!("callback-{}.json", participant.id));
        if !callback_file.exists() {
            std::fs::write(&callback_file, br#"{"kind":"succeed_immediately"}"#)
                .map_err(ChaosError::Io)?;
        }
        let log = append_log(
            self.work_directory
                .join(format!("participant-{}.log", participant.id))
                .as_path(),
        )?;
        let child = Command::new(&self.node_binary)
            .envs(self.common_environment(&self.process_endpoints(
                &self.participant_endpoints,
                ProcessRole::Participant,
                &participant.id,
            )))
            .env("CLUSTODIAN_CHAOS_NODE_MODE", "participant")
            .env(
                "CLUSTODIAN_CHAOS_PARTICIPANT_LEASE_TTL_MS",
                std::env::var("CLUSTODIAN_CHAOS_PARTICIPANT_LEASE_TTL_MS")
                    .unwrap_or_else(|_| String::from("2000")),
            )
            .env("CLUSTODIAN_CHAOS_INSTANCE_ID", &participant.id)
            .env("CLUSTODIAN_CHAOS_ZONE", &participant.zone)
            .env("CLUSTODIAN_CHAOS_CALLBACK_FILE", callback_file)
            .env(
                "CLUSTODIAN_CHAOS_FAILPOINTS_FILE",
                self.work_directory.join("failpoints.txt"),
            )
            .stdout(Stdio::from(log.try_clone().map_err(ChaosError::Io)?))
            .stderr(Stdio::from(log))
            .spawn()
            .map_err(ChaosError::Io)?;
        self.participants.insert(participant.id.clone(), child);
        self.expected_participants.insert(participant.id.clone());
        self.persist_process_ids()?;
        Ok(())
    }

    pub fn callback_file(&self, participant: &str) -> PathBuf {
        self.work_directory
            .join(format!("callback-{participant}.json"))
    }

    pub fn stop_controller(&mut self, id: &str) -> Result<(), ChaosError> {
        let result = stop_child(self.controllers.get_mut(id), false);
        self.expected_controllers.remove(id);
        self.persist_process_ids()?;
        result
    }

    pub fn stop_participant(&mut self, id: &str) -> Result<(), ChaosError> {
        let result = stop_child(self.participants.get_mut(id), false);
        self.expected_participants.remove(id);
        self.persist_process_ids()?;
        result
    }

    pub fn crash_controller(&mut self, id: &str) -> Result<(), ChaosError> {
        let result = stop_child(self.controllers.get_mut(id), true);
        self.expected_controllers.remove(id);
        self.persist_process_ids()?;
        result
    }

    pub fn crash_participant(&mut self, id: &str) -> Result<(), ChaosError> {
        let result = stop_child(self.participants.get_mut(id), true);
        self.expected_participants.remove(id);
        self.persist_process_ids()?;
        result
    }

    pub fn controller_running(&mut self, id: &str) -> bool {
        self.controllers.get_mut(id).is_some_and(child_running)
    }

    pub fn participant_running(&mut self, id: &str) -> bool {
        self.participants.get_mut(id).is_some_and(child_running)
    }

    /// Return the three etcd proxy names belonging to one SUT process.
    pub fn process_proxy_names(role: ProcessRole, id: &str) -> Vec<String> {
        let _ = role;
        (1..=3)
            .map(|member| format!("{id}-etcd-{member}"))
            .collect()
    }

    /// Return all currently expected SUT process proxy names.
    pub fn all_process_proxy_names(&self) -> Vec<String> {
        let mut proxies = Vec::new();
        for id in &self.expected_controllers {
            proxies.extend(Self::process_proxy_names(ProcessRole::Controller, id));
        }
        for id in &self.expected_participants {
            proxies.extend(Self::process_proxy_names(ProcessRole::Participant, id));
        }
        proxies
    }

    /// Whether all processes the driver expects to be running are live in etcd
    /// and have a running child process.
    pub fn expected_processes_healthy(&mut self, snapshot: &ClusterSnapshot) -> bool {
        let expected_participants = self
            .expected_participants
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let participants_healthy = expected_participants
            .iter()
            .all(|id| self.participant_running(id) && snapshot.live_instances.contains_key(id));
        let expected_controllers = self
            .expected_controllers
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let controllers_healthy = expected_controllers.iter().all(|id| {
            self.controller_running(id)
                && (snapshot
                    .controllers
                    .active
                    .iter()
                    .any(|active| active == id)
                    || snapshot
                        .controllers
                        .standby
                        .iter()
                        .any(|standby| standby == id))
        });
        participants_healthy && controllers_healthy
    }

    pub fn expected_controller_membership_healthy(&self, snapshot: &ClusterSnapshot) -> bool {
        self.expected_controllers.iter().all(|id| {
            snapshot
                .controllers
                .active
                .iter()
                .any(|active| active == id)
                || snapshot
                    .controllers
                    .standby
                    .iter()
                    .any(|standby| standby == id)
        })
    }

    pub fn suspend_controller(&mut self, id: &str) -> Result<(), ChaosError> {
        suspend_child(
            self.controllers.get_mut(id),
            &mut self.suspended,
            format!("controller/{id}"),
        )
    }

    pub fn suspend_participant(&mut self, id: &str) -> Result<(), ChaosError> {
        suspend_child(
            self.participants.get_mut(id),
            &mut self.suspended,
            format!("participant/{id}"),
        )
    }

    pub fn resume_all(&mut self) -> Result<(), ChaosError> {
        let suspended = std::mem::take(&mut self.suspended);
        for identity in suspended {
            let child = if let Some(id) = identity.strip_prefix("controller/") {
                self.controllers.get_mut(id)
            } else if let Some(id) = identity.strip_prefix("participant/") {
                self.participants.get_mut(id)
            } else {
                None
            };
            if let Some(child) = child {
                resume_child(child)?;
            }
        }
        Ok(())
    }

    pub fn record_observation(&self, snapshot: &ClusterSnapshot) -> Result<(), ChaosError> {
        append_json_line(&self.work_directory.join("observations.jsonl"), snapshot)
    }

    pub fn record_quiescence(&self, record: &QuiescenceRecord) -> Result<(), ChaosError> {
        append_json_line(&self.work_directory.join("quiescence.jsonl"), record)
    }

    pub fn record_idle_window(
        &self,
        seconds: u64,
        revision_delta: i64,
        before: ProcessMetrics,
        after: ProcessMetrics,
    ) -> Result<(), ChaosError> {
        append_json_line(
            &self.work_directory.join("idle-windows.jsonl"),
            &serde_json::json!({
                "seconds": seconds,
                "semantic_revision_delta": revision_delta,
                "cpu_jiffies_delta": after.cpu_jiffies.saturating_sub(before.cpu_jiffies),
                "rss_bytes_delta": after.rss_bytes.saturating_sub(before.rss_bytes),
                "open_fd_delta": after.open_fd_count.saturating_sub(before.open_fd_count),
                "task_delta": after.task_count.saturating_sub(before.task_count),
                "process_ids": self.process_ids(),
            }),
        )
    }

    pub fn record_fault_activation(&self, activation: &FaultActivation) -> Result<(), ChaosError> {
        append_json_line(
            &self.work_directory.join("fault-activations.jsonl"),
            activation,
        )
    }

    pub fn fault_hit_count(&self, key: &str) -> u64 {
        std::fs::read_to_string(self.work_directory.join("fault-hits.jsonl"))
            .map(|contents| contents.lines().filter(|line| *line == key).count() as u64)
            .unwrap_or(0)
    }

    pub fn record_metrics(&self, sample: &RuntimeMetricSample) -> Result<(), ChaosError> {
        append_json_line(&self.work_directory.join("metrics.jsonl"), sample)
    }

    pub fn process_metrics(&self) -> ProcessMetrics {
        let process_ids = self.process_ids();
        let mut metrics = ProcessMetrics {
            process_count: process_ids.len(),
            ..ProcessMetrics::default()
        };
        for pid in process_ids.values() {
            let path = PathBuf::from(format!("/proc/{pid}"));
            if let Ok(entries) = std::fs::read_dir(path.join("task")) {
                metrics.task_count += entries.count();
            }
            if let Ok(value) = std::fs::read_to_string(path.join("statm")) {
                if let Some(pages) = value.split_whitespace().nth(1) {
                    metrics.rss_bytes += pages.parse::<u64>().unwrap_or(0) * 4096;
                }
            }
            if let Ok(value) = std::fs::read_to_string(path.join("stat")) {
                if let Some((_, rest)) = value.rsplit_once(')') {
                    let fields = rest.split_whitespace().collect::<Vec<_>>();
                    if let (Some(user), Some(system)) = (fields.get(11), fields.get(12)) {
                        metrics.cpu_jiffies += user.parse::<u64>().unwrap_or(0);
                        metrics.cpu_jiffies += system.parse::<u64>().unwrap_or(0);
                    }
                }
            }
            if let Ok(entries) = std::fs::read_dir(path.join("fd")) {
                metrics.open_fd_count += entries.count();
            }
        }
        metrics
    }

    pub fn watch_event_count(&self) -> usize {
        std::fs::read_to_string(self.work_directory.join("observer-events.jsonl"))
            .map(|contents| contents.lines().count())
            .unwrap_or(0)
    }

    /// Return invariant failures reported by the independent observer process.
    pub fn observer_violations(&self) -> Vec<InvariantViolation> {
        let Ok(contents) =
            std::fs::read_to_string(self.work_directory.join("observer-events.jsonl"))
        else {
            return Vec::new();
        };
        contents
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .flat_map(|event| {
                event
                    .get("violations")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .unwrap_or_default()
            })
            .filter_map(|violation| serde_json::from_value(violation).ok())
            .collect()
    }

    pub fn all_processes_resumed(&self) -> bool {
        self.suspended.is_empty()
    }

    pub fn has_active_toxics(&self) -> bool {
        !self.active_toxics.is_empty()
    }

    pub fn mark_toxic(&mut self, proxy: &str, name: &str, active: bool) {
        let key = format!("{proxy}/{name}");
        if active {
            self.active_toxics.insert(key);
        } else {
            self.active_toxics.remove(&key);
        }
    }

    pub fn set_failpoint(&self, point: &str, behavior: &str) -> Result<(), ChaosError> {
        let value = if behavior.is_empty() {
            String::new()
        } else {
            format!("{point}={behavior}")
        };
        std::fs::write(self.work_directory.join("failpoints.txt"), value).map_err(ChaosError::Io)
    }

    pub fn callback_is_hanging(&self, participant: &str) -> Result<bool, ChaosError> {
        let path = self.callback_file(participant);
        if !path.exists() {
            return Ok(false);
        }
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).map_err(ChaosError::Io)?)
                .map_err(ChaosError::Serialization)?;
        Ok(value.get("kind").and_then(serde_json::Value::as_str) == Some("block"))
    }

    pub fn process_ids(&self) -> BTreeMap<String, i32> {
        let mut ids = BTreeMap::new();
        for (id, child) in &self.controllers {
            ids.insert(format!("controller/{id}"), child.id() as i32);
        }
        for (id, child) in &self.participants {
            ids.insert(format!("participant/{id}"), child.id() as i32);
        }
        if let Some(observer) = &self.observer {
            ids.insert(String::from("observer"), observer.id() as i32);
        }
        ids
    }

    fn common_environment(&self, endpoints: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                String::from("CLUSTODIAN_CHAOS_CLUSTER"),
                self.cluster.clone(),
            ),
            (String::from("CLUSTODIAN_CHAOS_PREFIX"), self.prefix.clone()),
            (
                String::from("CLUSTODIAN_CHAOS_ETCD_ENDPOINTS"),
                endpoints.to_owned(),
            ),
            (
                String::from("CLUSTODIAN_CHAOS_HITS_FILE"),
                self.work_directory
                    .join("fault-hits.jsonl")
                    .to_string_lossy()
                    .into_owned(),
            ),
        ])
    }

    fn process_endpoints(&self, endpoints: &str, role: ProcessRole, id: &str) -> String {
        if !self.use_process_specific_endpoints {
            return endpoints.to_owned();
        }
        let Some(endpoint_prefix) = endpoints
            .split(',')
            .map(str::trim)
            .find_map(endpoint_prefix)
        else {
            return endpoints.to_owned();
        };
        let Some(ports) = proxy_ports(role, id) else {
            return endpoints.to_owned();
        };
        ports
            .into_iter()
            .map(|port| format!("{endpoint_prefix}:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn persist_process_ids(&self) -> Result<(), ChaosError> {
        let bytes =
            serde_json::to_vec_pretty(&self.process_ids()).map_err(ChaosError::Serialization)?;
        std::fs::write(self.work_directory.join("processes.json"), bytes).map_err(ChaosError::Io)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessMetrics {
    pub process_count: usize,
    pub task_count: usize,
    pub cpu_jiffies: u64,
    pub rss_bytes: u64,
    pub open_fd_count: usize,
}

fn append_log(path: &Path) -> Result<File, ChaosError> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(ChaosError::Io)
}

fn append_json_line<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), ChaosError> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(ChaosError::Io)?;
    serde_json::to_writer(&mut file, value).map_err(ChaosError::Serialization)?;
    std::io::Write::write_all(&mut file, b"\n").map_err(ChaosError::Io)
}

impl Drop for ProcessSupervisor {
    fn drop(&mut self) {
        for child in self.controllers.values_mut() {
            let _ = child.kill();
        }
        for child in self.participants.values_mut() {
            let _ = child.kill();
        }
        if let Some(child) = &mut self.observer {
            let _ = child.kill();
        }
    }
}

fn child_running(child: &mut Child) -> bool {
    child.try_wait().ok().flatten().is_none()
}

fn stop_child(child: Option<&mut Child>, crash: bool) -> Result<(), ChaosError> {
    let Some(child) = child else {
        return Ok(());
    };
    if !child_running(child) {
        return Ok(());
    }
    if crash {
        child.kill().map_err(ChaosError::Io)?;
    } else {
        let status = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .map_err(ChaosError::Io)?;
        if !status.success() {
            return Err(ChaosError::InvalidArguments(String::from(
                "failed to send SIGTERM",
            )));
        }
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !child_running(child) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    child.kill().map_err(ChaosError::Io)?;
    let _ = child.wait();
    Ok(())
}

fn suspend_child(
    child: Option<&mut Child>,
    suspended: &mut BTreeSet<String>,
    identity: String,
) -> Result<(), ChaosError> {
    let Some(child) = child else {
        return Ok(());
    };
    if child_running(child) {
        send_signal(child, "-STOP")?;
        suspended.insert(identity);
    }
    Ok(())
}

fn resume_child(child: &mut Child) -> Result<(), ChaosError> {
    if child_running(child) {
        send_signal(child, "-CONT")?;
    }
    Ok(())
}

fn send_signal(child: &Child, signal: &str) -> Result<(), ChaosError> {
    let status = Command::new("kill")
        .args([signal, &child.id().to_string()])
        .status()
        .map_err(ChaosError::Io)?;
    if status.success() {
        Ok(())
    } else {
        Err(ChaosError::InvalidArguments(format!(
            "failed to send {signal}"
        )))
    }
}

pub fn monotonic_millis() -> u128 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis()
}

pub fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

#[derive(Debug)]
pub enum ChaosError {
    Io(std::io::Error),
    Serialization(serde_json::Error),
    InvalidArguments(String),
    Invariant(InvariantViolation),
}

impl fmt::Display for ChaosError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Serialization(error) => error.fmt(formatter),
            Self::InvalidArguments(message) => formatter.write_str(message),
            Self::Invariant(violation) => {
                write!(formatter, "{}: {}", violation.name, violation.detail)
            }
        }
    }
}

impl std::error::Error for ChaosError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_produces_same_trace() {
        let left = SeededGenerator::new(187_231).generate("pr", 100);
        let right = SeededGenerator::new(187_231).generate("pr", 100);
        assert_eq!(left, right);
    }

    #[test]
    fn different_seed_changes_trace() {
        let left = SeededGenerator::new(1).generate("pr", 20);
        let right = SeededGenerator::new(2).generate("pr", 20);
        assert_ne!(left.actions, right.actions);
    }

    #[test]
    fn generated_profiles_cover_their_fault_matrix() {
        let pr = SeededGenerator::new(1).generate("pr", 20);
        assert!(pr.faults.iter().any(|fault| matches!(
            &fault.fault,
            Fault::Callback {
                behavior: CallbackBehavior::Block { .. },
                ..
            }
        )));
        assert!(pr
            .faults
            .iter()
            .any(|fault| matches!(&fault.fault, Fault::Clock { .. })));
        assert!(pr
            .faults
            .iter()
            .any(|fault| matches!(&fault.fault, Fault::Failpoint { .. })));
        assert!(!pr
            .faults
            .iter()
            .any(|fault| matches!(&fault.fault, Fault::Network { .. })));

        let nightly = SeededGenerator::new(1).generate("nightly", 20);
        assert!(nightly
            .faults
            .iter()
            .any(|fault| matches!(&fault.fault, Fault::Network { .. })));
        assert!(nightly
            .faults
            .iter()
            .any(|fault| matches!(&fault.fault, Fault::EtcdMemberRestart { .. })));
        assert!(nightly
            .faults
            .iter()
            .any(|fault| matches!(&fault.fault, Fault::EtcdCompaction)));
        assert!(nightly
            .faults
            .iter()
            .any(|fault| matches!(&fault.fault, Fault::Failpoint { .. })));
    }

    #[test]
    fn generated_surgical_faults_have_a_triggering_action() {
        let trace = SeededGenerator::new(1).generate("nightly", 20);
        for scheduled in &trace.faults {
            if requires_fault_hit(&scheduled.fault) {
                assert!(matches!(
                    trace.actions.get(scheduled.action_index),
                    Some(Action::ModifyPreferenceList { .. })
                ));
            }
        }
        assert!(trace
            .actions
            .iter()
            .any(|action| matches!(action, Action::GracefulStopParticipant { .. })));
        assert!(trace
            .actions
            .iter()
            .any(|action| matches!(action, Action::GracefulStopController { .. })));
    }

    #[test]
    fn process_proxy_names_are_owned_by_the_sut_process() {
        assert_eq!(
            ProcessSupervisor::process_proxy_names(ProcessRole::Controller, "controller-a"),
            [
                "controller-a-etcd-1",
                "controller-a-etcd-2",
                "controller-a-etcd-3"
            ]
        );
        assert_eq!(
            ProcessSupervisor::process_proxy_names(ProcessRole::Participant, "node-7"),
            ["node-7-etcd-1", "node-7-etcd-2", "node-7-etcd-3"]
        );
    }

    #[test]
    fn convergence_requires_derived_state_and_replica_shape() {
        let config = ClusterConfig {
            controllers: vec![String::from("controller-a")],
            participants: (1..=3)
                .map(|index| ParticipantConfig {
                    id: format!("node-{index}"),
                    zone: String::from("zone-1"),
                })
                .collect(),
            resources: vec![ResourceConfig {
                name: String::from("service"),
                partitions: vec![String::from("service_0")],
                replicas: 3,
                placement: PlacementConfig::SemiAuto {
                    preference_lists: BTreeMap::from([(
                        String::from("service_0"),
                        vec![
                            String::from("node-1"),
                            String::from("node-2"),
                            String::from("node-3"),
                        ],
                    )]),
                },
            }],
            throttles: Vec::new(),
        };
        let active_current_state = BTreeMap::from([
            (
                String::from("node-1"),
                clustodian::observe::ActiveCurrentStateObservation {
                    session: 1,
                    resources: BTreeMap::from([(
                        String::from("service"),
                        BTreeMap::from([(String::from("service_0"), String::from("LEADER"))]),
                    )]),
                },
            ),
            (
                String::from("node-2"),
                clustodian::observe::ActiveCurrentStateObservation {
                    session: 1,
                    resources: BTreeMap::from([(
                        String::from("service"),
                        BTreeMap::from([(String::from("service_0"), String::from("STANDBY"))]),
                    )]),
                },
            ),
            (
                String::from("node-3"),
                clustodian::observe::ActiveCurrentStateObservation {
                    session: 1,
                    resources: BTreeMap::from([(
                        String::from("service"),
                        BTreeMap::from([(String::from("service_0"), String::from("STANDBY"))]),
                    )]),
                },
            ),
        ]);
        let snapshot = clustodian::observe::ClusterSnapshot {
            observer_revision: clustodian::observe::ObserverRevision::from_value(4),
            authoritative_revision: clustodian::observe::ObserverRevision::from_value(4),
            processed_revision: Some(clustodian::observe::ObserverRevision::from_value(4)),
            controllers: clustodian::observe::ControllerMembership {
                active: vec![String::from("controller-a")],
                standby: Vec::new(),
            },
            live_instances: BTreeMap::from([
                (String::from("node-1"), 1),
                (String::from("node-2"), 1),
                (String::from("node-3"), 1),
            ]),
            active_current_state,
            external_view: serde_json::json!({
                "service": {
                    "service_0": {
                        "node-1": "LEADER",
                        "node-2": "STANDBY",
                        "node-3": "STANDBY"
                    }
                }
            }),
            pending_transitions: Vec::new(),
            routing_results: vec![
                clustodian::observe::RoutingResult {
                    resource: String::from("service"),
                    partition: String::from("service_0"),
                    state: String::from("LEADER"),
                    instances: vec![String::from("node-1")],
                },
                clustodian::observe::RoutingResult {
                    resource: String::from("service"),
                    partition: String::from("service_0"),
                    state: String::from("STANDBY"),
                    instances: vec![String::from("node-2"), String::from("node-3")],
                },
            ],
        };
        assert!(check_convergence(&snapshot, &config).is_empty());

        let mut duplicate_current_state = snapshot.clone();
        duplicate_current_state
            .active_current_state
            .get_mut("node-2")
            .expect("fixture has node-2")
            .resources
            .get_mut("service")
            .expect("fixture has service")
            .insert(String::from("service_0"), String::from("LEADER"));
        assert!(check_safety(&duplicate_current_state)
            .iter()
            .any(|violation| violation.name == "current_state_single_leader"));

        let mut wrong_nodes = snapshot.clone();
        wrong_nodes.external_view = serde_json::json!({
            "service": {
                "service_0": {
                    "node-1": "LEADER",
                    "node-2": "STANDBY",
                    "node-4": "STANDBY"
                }
            }
        });
        wrong_nodes.routing_results[1].instances =
            vec![String::from("node-2"), String::from("node-4")];
        assert!(check_convergence(&wrong_nodes, &config)
            .iter()
            .any(|violation| violation.name == "external_view_matches_best_possible_state"));

        let mut session_replacement = snapshot.clone();
        session_replacement.authoritative_revision =
            clustodian::observe::ObserverRevision::from_value(5);
        session_replacement.pending_transitions =
            vec![clustodian::controller::PublishedTransition {
                resource: String::from("service"),
                partition: String::from("service_0"),
                instance: String::from("node-1"),
                target_session: 0,
                from: String::from("LEADER"),
                to: String::from("STANDBY"),
                message_type: String::from("STATE_TRANSITION"),
                message_id: String::from("stale-session"),
            }];
        assert!(check_safety(&session_replacement).is_empty());
        session_replacement.processed_revision =
            Some(clustodian::observe::ObserverRevision::from_value(5));
        assert!(check_safety(&session_replacement)
            .iter()
            .any(|violation| violation.name == "pending_transition_session"));

        let mut broken = snapshot;
        broken.external_view = serde_json::json!({});
        let violations = check_convergence(&broken, &config);
        assert!(violations
            .iter()
            .any(|violation| violation.name == "external_view_matches_current_state"));
        assert!(violations
            .iter()
            .any(|violation| violation.name == "settled_partition_present"));
    }

    #[test]
    fn quiescence_requires_every_condition() {
        let mut inputs = QuiescenceInputs {
            no_action_executing: true,
            no_fault_awaiting_completion: true,
            stopped_processes_resumed: true,
            toxics_removed: true,
            network_paths_restored: true,
            callback_barriers_released: true,
            no_hanging_callbacks: true,
            etcd_quorum_available: true,
            etcd_endpoints_reachable: true,
            eligible_controller_running: true,
            minimum_participants_healthy: true,
            clock_perturbations_removed: true,
            observer_connected: true,
            observer_caught_up: true,
        };
        assert!(inputs.is_quiescent());
        inputs.observer_caught_up = false;
        assert!(!inputs.is_quiescent());
    }

    #[test]
    fn replay_classification_preserves_non_reproduction() {
        assert_eq!(
            ReplayStats::new(10, 0).classification,
            FailureClassification::NotYetReproduced
        );
        assert_eq!(ReplayStats::new(10, 7).reproduction_rate, 0.7);
        assert_eq!(
            ReplayStats::new(10, 10).classification,
            FailureClassification::Stable
        );
    }

    #[test]
    fn budget_rejects_exhausted_faults() {
        let mut budget = FaultBudget {
            etcd_failures_remaining: 0,
            controller_failures_remaining: 1,
            participant_failures_remaining: 1,
            network_partitions_remaining: 1,
            clock_faults_remaining: 0,
        };
        assert!(matches!(
            budget.admit(&Fault::EtcdCompaction),
            FaultAdmission::Rejected(_)
        ));
        assert_eq!(budget.admit_controller_failure(), FaultAdmission::Admitted);
        assert!(matches!(
            budget.admit_controller_failure(),
            FaultAdmission::Rejected(_)
        ));
    }

    #[test]
    fn scale_profiles_have_fixed_workload_shapes() {
        let scale = SeededGenerator::new(1).generate("scale-100-10k", 0);
        assert_eq!(scale.initial_config.controllers.len(), 3);
        assert_eq!(scale.initial_config.participants.len(), 100);
        assert_eq!(scale.initial_config.resources.len(), 100);
        assert_eq!(
            scale
                .initial_config
                .resources
                .iter()
                .map(|resource| resource.partitions.len())
                .sum::<usize>(),
            10_000
        );
        assert!(matches!(
            scale.actions.last(),
            Some(Action::AssertIdle { seconds: 30 })
        ));
        let large = SeededGenerator::new(1).generate("scale-250-50k", 0);
        assert_eq!(large.initial_config.participants.len(), 250);
        assert_eq!(
            large
                .initial_config
                .resources
                .iter()
                .map(|resource| resource.partitions.len())
                .sum::<usize>(),
            50_000
        );
    }
}
