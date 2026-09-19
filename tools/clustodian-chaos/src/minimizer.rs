//! Deterministic trace transformations used by the real-stack minimizer.

use crate::{
    Action, CallbackBehavior, ClockPerturbation, FailpointBehavior, Fault, NetworkTarget,
    NetworkToxic, PlacementConfig, ResourceConfig, ScheduledFault, Trace,
};

/// Remove an action range and remap faults that occur after that range.
pub fn remove_action_range(trace: &Trace, start: usize, end: usize) -> Option<Trace> {
    if start >= end || end > trace.actions.len() {
        return None;
    }
    let removed = end - start;
    let mut candidate = trace.clone();
    candidate.actions.drain(start..end);
    candidate.faults = trace
        .faults
        .iter()
        .filter_map(|scheduled| {
            if (start..end).contains(&scheduled.action_index) {
                None
            } else {
                let action_index = scheduled
                    .action_index
                    .checked_sub(removed * usize::from(scheduled.action_index >= end))?;
                Some(ScheduledFault {
                    action_index,
                    fault: scheduled.fault.clone(),
                })
            }
        })
        .collect();
    Some(candidate)
}

/// Remove a range of scheduled faults without changing action indices.
pub fn remove_fault_range(trace: &Trace, start: usize, end: usize) -> Option<Trace> {
    if start >= end || end > trace.faults.len() {
        return None;
    }
    let mut candidate = trace.clone();
    candidate.faults.drain(start..end);
    Some(candidate)
}

/// Generate deterministic scalar reductions for one scheduled fault.
pub fn reduce_fault(trace: &Trace, fault_index: usize) -> Vec<Trace> {
    let Some(scheduled) = trace.faults.get(fault_index) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    match &scheduled.fault {
        Fault::Network {
            target,
            toxic,
            duration_milliseconds,
        } => {
            for value in lower_u64(*duration_milliseconds) {
                let fault = Fault::Network {
                    target: target.clone(),
                    toxic: toxic.clone(),
                    duration_milliseconds: value,
                };
                candidates.push(replace_fault(trace, fault_index, fault));
            }
            match toxic {
                NetworkToxic::Latency {
                    milliseconds,
                    jitter_milliseconds,
                } => {
                    for value in lower_u64(*milliseconds) {
                        candidates.push(replace_fault(
                            trace,
                            fault_index,
                            Fault::Network {
                                target: target.clone(),
                                toxic: NetworkToxic::Latency {
                                    milliseconds: value,
                                    jitter_milliseconds: *jitter_milliseconds,
                                },
                                duration_milliseconds: *duration_milliseconds,
                            },
                        ));
                    }
                    for value in lower_u64(*jitter_milliseconds) {
                        candidates.push(replace_fault(
                            trace,
                            fault_index,
                            Fault::Network {
                                target: target.clone(),
                                toxic: NetworkToxic::Latency {
                                    milliseconds: *milliseconds,
                                    jitter_milliseconds: value,
                                },
                                duration_milliseconds: *duration_milliseconds,
                            },
                        ));
                    }
                }
                NetworkToxic::Timeout { milliseconds }
                | NetworkToxic::SlowClose { milliseconds } => {
                    for value in lower_u64(*milliseconds) {
                        let toxic = match toxic {
                            NetworkToxic::Timeout { .. } => NetworkToxic::Timeout {
                                milliseconds: value,
                            },
                            NetworkToxic::SlowClose { .. } => NetworkToxic::SlowClose {
                                milliseconds: value,
                            },
                            _ => unreachable!("matched timeout or slow-close"),
                        };
                        candidates.push(replace_fault(
                            trace,
                            fault_index,
                            Fault::Network {
                                target: target.clone(),
                                toxic,
                                duration_milliseconds: *duration_milliseconds,
                            },
                        ));
                    }
                }
                NetworkToxic::Disconnect
                | NetworkToxic::DirectionalDisconnect { .. }
                | NetworkToxic::ConnectionReset => {}
                NetworkToxic::Bandwidth {
                    kilobytes_per_second,
                } => {
                    for value in lower_u64(*kilobytes_per_second) {
                        candidates.push(replace_fault(
                            trace,
                            fault_index,
                            Fault::Network {
                                target: target.clone(),
                                toxic: NetworkToxic::Bandwidth {
                                    kilobytes_per_second: value,
                                },
                                duration_milliseconds: *duration_milliseconds,
                            },
                        ));
                    }
                }
            }
        }
        Fault::Callback {
            participant,
            behavior,
        } => match behavior {
            CallbackBehavior::SucceedSlowly { milliseconds } => {
                for value in lower_u64(*milliseconds) {
                    candidates.push(replace_fault(
                        trace,
                        fault_index,
                        Fault::Callback {
                            participant: participant.clone(),
                            behavior: CallbackBehavior::SucceedSlowly {
                                milliseconds: value,
                            },
                        },
                    ));
                }
            }
            CallbackBehavior::RepeatedError { attempts } => {
                for value in lower_u32(*attempts) {
                    candidates.push(replace_fault(
                        trace,
                        fault_index,
                        Fault::Callback {
                            participant: participant.clone(),
                            behavior: CallbackBehavior::RepeatedError { attempts: value },
                        },
                    ));
                }
            }
            CallbackBehavior::SucceedImmediately
            | CallbackBehavior::Block { .. }
            | CallbackBehavior::Error
            | CallbackBehavior::Panic => {}
        },
        Fault::Clock {
            participant,
            perturbation,
        } => match perturbation {
            ClockPerturbation::Suspend { milliseconds }
            | ClockPerturbation::DelayedKeepalive { milliseconds } => {
                for value in lower_u64(*milliseconds) {
                    let perturbation = match perturbation {
                        ClockPerturbation::Suspend { .. } => ClockPerturbation::Suspend {
                            milliseconds: value,
                        },
                        ClockPerturbation::DelayedKeepalive { .. } => {
                            ClockPerturbation::DelayedKeepalive {
                                milliseconds: value,
                            }
                        }
                        _ => unreachable!("matched suspend or delayed keepalive"),
                    };
                    candidates.push(replace_fault(
                        trace,
                        fault_index,
                        Fault::Clock {
                            participant: participant.clone(),
                            perturbation,
                        },
                    ));
                }
            }
            ClockPerturbation::WallForward { milliseconds }
            | ClockPerturbation::WallBackward { milliseconds }
            | ClockPerturbation::MonotonicOffset { milliseconds } => {
                for value in lower_i64(*milliseconds) {
                    let perturbation = match perturbation {
                        ClockPerturbation::WallForward { .. } => ClockPerturbation::WallForward {
                            milliseconds: value,
                        },
                        ClockPerturbation::WallBackward { .. } => ClockPerturbation::WallBackward {
                            milliseconds: value,
                        },
                        ClockPerturbation::MonotonicOffset { .. } => {
                            ClockPerturbation::MonotonicOffset {
                                milliseconds: value,
                            }
                        }
                        _ => unreachable!("matched signed clock perturbation"),
                    };
                    candidates.push(replace_fault(
                        trace,
                        fault_index,
                        Fault::Clock {
                            participant: participant.clone(),
                            perturbation,
                        },
                    ));
                }
            }
        },
        Fault::Failpoint { point, behavior } => {
            if let FailpointBehavior::Pause { milliseconds } = behavior {
                for value in lower_u64(*milliseconds) {
                    candidates.push(replace_fault(
                        trace,
                        fault_index,
                        Fault::Failpoint {
                            point: point.clone(),
                            behavior: FailpointBehavior::Pause {
                                milliseconds: value,
                            },
                        },
                    ));
                }
            }
        }
        Fault::EtcdMemberRestart { .. } | Fault::EtcdCompaction => {}
    }
    candidates
}

/// Generate candidates with a smaller initial cluster or resource topology.
pub fn reduce_cluster_and_resources(trace: &Trace) -> Vec<Trace> {
    let mut candidates = Vec::new();

    if trace.initial_config.controllers.len() > 1 {
        for controller in &trace.initial_config.controllers {
            let mut candidate = transform_actions(trace, |action| {
                (!action_controller_id(&action).is_some_and(|id| id == controller))
                    .then_some(action)
            });
            candidate
                .initial_config
                .controllers
                .retain(|configured| configured != controller);
            candidate.faults.retain(|scheduled| {
                !fault_controller_id(&scheduled.fault).is_some_and(|id| id == controller)
            });
            candidates.push(candidate);
        }
    }

    if trace.initial_config.participants.len() > 1 {
        for participant in &trace.initial_config.participants {
            candidates.push(remove_participant(trace, &participant.id));
        }
    }

    if trace.initial_config.resources.len() > 1 {
        for resource in &trace.initial_config.resources {
            let mut candidate = transform_actions(trace, |action| {
                (!action_resource_id(&action).is_some_and(|id| id == resource.name))
                    .then_some(action)
            });
            candidate
                .initial_config
                .resources
                .retain(|configured| configured.name != resource.name);
            candidates.push(candidate);
        }
    }

    for resource in &trace.initial_config.resources {
        if resource.partitions.len() > 1 {
            for partition in &resource.partitions {
                candidates.push(remove_partition(trace, &resource.name, partition));
            }
        }
        if resource.replicas > 1 {
            let mut candidate = trace.clone();
            set_resource_replicas(&mut candidate, &resource.name, resource.replicas / 2);
            candidates.push(candidate);
        }
    }

    candidates
}

fn remove_participant(trace: &Trace, participant: &str) -> Trace {
    let mut candidate = transform_actions(trace, |action| match action {
        Action::StartController { .. }
        | Action::GracefulStopController { .. }
        | Action::CrashController { .. }
        | Action::RestartController { .. }
        | Action::ChangeThrottle { .. }
        | Action::Wait { .. }
        | Action::RequestQuiescence
        | Action::WaitForConvergence { .. }
        | Action::AssertIdle { .. } => Some(action),
        Action::StartParticipant { id } => {
            (id != participant).then_some(Action::StartParticipant { id })
        }
        Action::GracefulStopParticipant { id } => {
            (id != participant).then_some(Action::GracefulStopParticipant { id })
        }
        Action::CrashParticipant { id } => {
            (id != participant).then_some(Action::CrashParticipant { id })
        }
        Action::RestartParticipant { id } => {
            (id != participant).then_some(Action::RestartParticipant { id })
        }
        Action::RemoveParticipant { id } => {
            (id != participant).then_some(Action::RemoveParticipant { id })
        }
        Action::AddParticipant { participant: added } => {
            (added.id != participant).then_some(Action::AddParticipant { participant: added })
        }
        Action::CreateResource { mut resource } => {
            prune_resource_participant(&mut resource, participant);
            Some(Action::CreateResource { resource })
        }
        Action::ModifyPreferenceList {
            resource,
            partition,
            mut instances,
        } => {
            instances.retain(|instance| instance != participant);
            Some(Action::ModifyPreferenceList {
                resource,
                partition,
                instances,
            })
        }
    });
    candidate
        .initial_config
        .participants
        .retain(|configured| configured.id != participant);
    for resource in &mut candidate.initial_config.resources {
        if let PlacementConfig::SemiAuto { preference_lists } = &mut resource.placement {
            for instances in preference_lists.values_mut() {
                instances.retain(|instance| instance != participant);
            }
        }
    }
    candidate.faults.retain(|scheduled| {
        !fault_participant_id(&scheduled.fault).is_some_and(|id| id == participant)
    });
    candidate
}

fn remove_partition(trace: &Trace, resource_name: &str, partition: &str) -> Trace {
    let mut candidate = transform_actions(trace, |action| match action {
        Action::CreateResource { mut resource } if resource.name == resource_name => {
            resource.partitions.retain(|name| name != partition);
            if let PlacementConfig::SemiAuto { preference_lists } = &mut resource.placement {
                preference_lists.remove(partition);
            }
            Some(Action::CreateResource { resource })
        }
        Action::ModifyPreferenceList {
            resource,
            partition: action_partition,
            instances: _,
        } if resource == resource_name && action_partition == partition => None,
        other => Some(other),
    });
    for resource in &mut candidate.initial_config.resources {
        if resource.name == resource_name {
            resource.partitions.retain(|name| name != partition);
            if let PlacementConfig::SemiAuto { preference_lists } = &mut resource.placement {
                preference_lists.remove(partition);
            }
        }
    }
    candidate
}

fn set_resource_replicas(trace: &mut Trace, resource_name: &str, replicas: usize) {
    for resource in &mut trace.initial_config.resources {
        if resource.name == resource_name {
            resource.replicas = replicas.max(1);
        }
    }
    for action in &mut trace.actions {
        if let Action::CreateResource { resource } = action {
            if resource.name == resource_name {
                resource.replicas = replicas.max(1);
            }
        }
    }
}

fn prune_resource_participant(resource: &mut ResourceConfig, participant: &str) {
    if let PlacementConfig::SemiAuto { preference_lists } = &mut resource.placement {
        for instances in preference_lists.values_mut() {
            instances.retain(|instance| instance != participant);
        }
    }
}

fn action_controller_id(action: &Action) -> Option<&str> {
    match action {
        Action::StartController { id }
        | Action::GracefulStopController { id }
        | Action::CrashController { id }
        | Action::RestartController { id } => Some(id),
        Action::StartParticipant { .. }
        | Action::GracefulStopParticipant { .. }
        | Action::CrashParticipant { .. }
        | Action::RestartParticipant { .. }
        | Action::AddParticipant { .. }
        | Action::RemoveParticipant { .. }
        | Action::CreateResource { .. }
        | Action::ModifyPreferenceList { .. }
        | Action::ChangeThrottle { .. }
        | Action::Wait { .. }
        | Action::RequestQuiescence
        | Action::WaitForConvergence { .. }
        | Action::AssertIdle { .. } => None,
    }
}

fn action_resource_id(action: &Action) -> Option<&str> {
    match action {
        Action::CreateResource { resource } => Some(&resource.name),
        Action::ModifyPreferenceList { resource, .. } => Some(resource),
        Action::StartController { .. }
        | Action::GracefulStopController { .. }
        | Action::CrashController { .. }
        | Action::RestartController { .. }
        | Action::StartParticipant { .. }
        | Action::GracefulStopParticipant { .. }
        | Action::CrashParticipant { .. }
        | Action::RestartParticipant { .. }
        | Action::AddParticipant { .. }
        | Action::RemoveParticipant { .. }
        | Action::ChangeThrottle { .. }
        | Action::Wait { .. }
        | Action::RequestQuiescence
        | Action::WaitForConvergence { .. }
        | Action::AssertIdle { .. } => None,
    }
}

fn fault_controller_id(fault: &Fault) -> Option<&str> {
    match fault {
        Fault::Network {
            target: NetworkTarget::Controller { id },
            ..
        } => Some(id),
        Fault::Network { .. }
        | Fault::EtcdMemberRestart { .. }
        | Fault::EtcdCompaction
        | Fault::Callback { .. }
        | Fault::Clock { .. }
        | Fault::Failpoint { .. } => None,
    }
}

fn fault_participant_id(fault: &Fault) -> Option<&str> {
    match fault {
        Fault::Network {
            target: NetworkTarget::Participant { id },
            ..
        }
        | Fault::Callback {
            participant: id, ..
        }
        | Fault::Clock {
            participant: id, ..
        } => Some(id),
        Fault::Network { .. }
        | Fault::EtcdMemberRestart { .. }
        | Fault::EtcdCompaction
        | Fault::Failpoint { .. } => None,
    }
}

fn replace_fault(trace: &Trace, fault_index: usize, fault: Fault) -> Trace {
    let mut candidate = trace.clone();
    candidate.faults[fault_index].fault = fault;
    candidate
}

fn transform_actions(trace: &Trace, mut transform: impl FnMut(Action) -> Option<Action>) -> Trace {
    let mut candidate = trace.clone();
    let mut remap = vec![None; trace.actions.len()];
    let mut actions = Vec::with_capacity(trace.actions.len());
    for (old_index, action) in trace.actions.iter().cloned().enumerate() {
        if let Some(action) = transform(action) {
            remap[old_index] = Some(actions.len());
            actions.push(action);
        }
    }
    candidate.actions = actions;
    candidate.faults = trace
        .faults
        .iter()
        .filter_map(|scheduled| {
            Some(ScheduledFault {
                action_index: remap.get(scheduled.action_index).copied().flatten()?,
                fault: scheduled.fault.clone(),
            })
        })
        .collect();
    candidate
}

fn lower_u64(value: u64) -> Vec<u64> {
    lower_values(value, |value| value / 2, 1, 0)
}

fn lower_u32(value: u32) -> Vec<u32> {
    lower_values(value, |value| value / 2, 1, 0)
}

fn lower_i64(value: i64) -> Vec<i64> {
    lower_values(value, |value| value / 2, value.signum(), 0)
}

fn lower_values<T: Copy + Eq>(value: T, half: impl Fn(T) -> T, one: T, zero: T) -> Vec<T> {
    let mut values = Vec::new();
    for reduced in [half(value), one, zero] {
        if reduced != value && !values.contains(&reduced) {
            values.push(reduced);
        }
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CallbackBehavior, ClusterConfig, NetworkTarget, NetworkToxic, ParticipantConfig,
        PlacementConfig, ResourceConfig, ThrottleConfig,
    };
    use std::collections::BTreeMap;

    fn trace() -> Trace {
        Trace {
            schema_version: 1,
            seed: 7,
            profile: String::from("nightly"),
            initial_config: ClusterConfig {
                controllers: vec![String::from("controller-a"), String::from("controller-b")],
                participants: vec![
                    ParticipantConfig {
                        id: String::from("node-1"),
                        zone: String::from("zone-1"),
                    },
                    ParticipantConfig {
                        id: String::from("node-2"),
                        zone: String::from("zone-2"),
                    },
                ],
                resources: vec![ResourceConfig {
                    name: String::from("service"),
                    partitions: vec![String::from("service_0"), String::from("service_1")],
                    replicas: 3,
                    placement: PlacementConfig::SemiAuto {
                        preference_lists: BTreeMap::from([
                            (
                                String::from("service_0"),
                                vec![String::from("node-1"), String::from("node-2")],
                            ),
                            (String::from("service_1"), vec![String::from("node-2")]),
                        ]),
                    },
                }],
                throttles: vec![ThrottleConfig {
                    scope: String::from("CLUSTER"),
                    rebalance_type: String::from("ANY"),
                    max_in_flight: 1,
                }],
            },
            actions: vec![
                Action::StartController {
                    id: String::from("controller-a"),
                },
                Action::StartParticipant {
                    id: String::from("node-1"),
                },
                Action::Wait { milliseconds: 50 },
                Action::CreateResource {
                    resource: ResourceConfig {
                        name: String::from("service"),
                        partitions: vec![String::from("service_0"), String::from("service_1")],
                        replicas: 3,
                        placement: PlacementConfig::SemiAuto {
                            preference_lists: BTreeMap::new(),
                        },
                    },
                },
            ],
            faults: vec![
                ScheduledFault {
                    action_index: 1,
                    fault: Fault::Network {
                        target: NetworkTarget::Participant {
                            id: String::from("node-1"),
                        },
                        toxic: NetworkToxic::Latency {
                            milliseconds: 100,
                            jitter_milliseconds: 25,
                        },
                        duration_milliseconds: 300,
                    },
                },
                ScheduledFault {
                    action_index: 3,
                    fault: Fault::Callback {
                        participant: String::from("node-1"),
                        behavior: CallbackBehavior::RepeatedError { attempts: 4 },
                    },
                },
            ],
        }
    }

    #[test]
    fn removing_actions_drops_and_remaps_faults() {
        let candidate = remove_action_range(&trace(), 1, 3).expect("valid range");
        assert_eq!(candidate.actions.len(), 2);
        assert_eq!(candidate.faults.len(), 1);
        assert_eq!(candidate.faults[0].action_index, 1);
    }

    #[test]
    fn fault_reductions_cover_duration_and_repeated_attempts() {
        let trace = trace();
        let duration_candidates = reduce_fault(&trace, 0);
        assert!(duration_candidates.iter().any(|candidate| {
            matches!(
                candidate.faults[0].fault,
                Fault::Network {
                    duration_milliseconds: 0,
                    ..
                }
            )
        }));
        let repeated_candidates = reduce_fault(&trace, 1);
        assert!(repeated_candidates.iter().any(|candidate| {
            matches!(
                candidate.faults[1].fault,
                Fault::Callback {
                    behavior: CallbackBehavior::RepeatedError { attempts: 1 },
                    ..
                }
            )
        }));
    }

    #[test]
    fn topology_reductions_keep_configuration_actions_aligned() {
        let candidates = reduce_cluster_and_resources(&trace());
        assert!(candidates.iter().any(|candidate| {
            candidate.initial_config.participants.len() == 1
                && candidate.initial_config.resources.iter().all(|resource| {
                    !resource
                        .placement
                        .as_semi_auto()
                        .is_some_and(|lists| lists.values().flatten().any(|id| id == "node-1"))
                })
        }));
        assert!(candidates.iter().any(|candidate| {
            candidate
                .actions
                .iter()
                .filter_map(|action| match action {
                    Action::CreateResource { resource } => Some(resource.replicas),
                    _ => None,
                })
                .any(|replicas| replicas == 1)
        }));
        let controller_reduction = candidates
            .iter()
            .find(|candidate| {
                candidate.initial_config.controllers.len() == 1
                    && candidate.initial_config.controllers[0] == "controller-b"
            })
            .expect("controller-a reduction");
        assert_eq!(controller_reduction.faults[0].action_index, 0);
    }

    trait SemiAutoExt {
        fn as_semi_auto(&self) -> Option<&BTreeMap<String, Vec<String>>>;
    }

    impl SemiAutoExt for PlacementConfig {
        fn as_semi_auto(&self) -> Option<&BTreeMap<String, Vec<String>>> {
            match self {
                PlacementConfig::SemiAuto { preference_lists } => Some(preference_lists),
                PlacementConfig::Crush => None,
            }
        }
    }

    #[test]
    fn lower_values_are_strict_and_unique() {
        assert_eq!(lower_u64(1), vec![0]);
        assert_eq!(lower_u32(4), vec![2, 1, 0]);
    }
}
