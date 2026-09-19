use super::model::{ClusterModel, ControllerLifecycle, ParticipantLifecycle};
use rand_chacha::{rand_core::RngCore, ChaCha8Rng};
use serde::{Deserialize, Serialize};

const ACTION_WEIGHT_STABILIZE: u64 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ClusterAction {
    StopParticipant {
        instance: String,
    },
    StartParticipant {
        instance: String,
    },
    RestartParticipant {
        instance: String,
    },
    StopController {
        controller: String,
    },
    StartController {
        controller: String,
    },
    AddParticipant {
        instance: String,
    },
    RemoveParticipant {
        instance: String,
    },
    AddResource {
        resource_spec: ResourceSpec,
    },
    RemoveResource {
        resource: String,
    },
    ChangeReplicaCount {
        resource: String,
        replicas: usize,
    },
    ChangeInstanceZone {
        instance: String,
        zone: String,
    },
    ChangeResourcePlacement {
        resource: String,
        placement: Placement,
    },
    ChangeTransitionLimits {
        limits: Vec<ThrottleSpec>,
    },
    PauseTransitions {
        instance: String,
    },
    ResumeTransitions {
        instance: String,
    },
    Stabilize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResourceSpec {
    pub name: String,
    pub partition_count: usize,
    pub replicas: usize,
    pub state_model: String,
    pub placement: Placement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Placement {
    Crush,
    CrushWithTopology {
        path: String,
        fault_zone_type: String,
        end_node_type: String,
    },
    SemiAuto {
        preference_lists: std::collections::BTreeMap<String, Vec<String>>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ThrottleSpec {
    pub scope: String,
    pub rebalance_type: String,
    pub max_in_flight: usize,
}

#[derive(Clone)]
struct WeightedAction {
    action: ClusterAction,
    weight: u64,
}

pub fn valid_actions(model: &ClusterModel) -> Vec<ClusterAction> {
    weighted_actions(model)
        .into_iter()
        .map(|candidate| candidate.action)
        .collect()
}

pub fn next_action(model: &ClusterModel, rng: &mut ChaCha8Rng) -> ClusterAction {
    let candidates = weighted_actions(model);
    let total_weight = candidates
        .iter()
        .map(|candidate| candidate.weight)
        .sum::<u64>();
    let mut selected = rng.next_u64() % total_weight;
    for candidate in candidates {
        if selected < candidate.weight {
            return candidate.action;
        }
        selected -= candidate.weight;
    }
    unreachable!("weighted action selection has a non-zero total weight")
}

fn weighted_actions(model: &ClusterModel) -> Vec<WeightedAction> {
    let mut actions = Vec::new();
    for (instance, participant) in &model.participants {
        match participant.lifecycle {
            ParticipantLifecycle::Running { .. } => {
                if !model.paused_handlers.contains(instance) {
                    actions.push(weight(
                        ClusterAction::StopParticipant {
                            instance: instance.clone(),
                        },
                        6,
                    ));
                    actions.push(weight(
                        ClusterAction::RestartParticipant {
                            instance: instance.clone(),
                        },
                        6,
                    ));
                }
            }
            ParticipantLifecycle::Stopped if participant.configured => actions.push(weight(
                ClusterAction::StartParticipant {
                    instance: instance.clone(),
                },
                4,
            )),
            ParticipantLifecycle::Stopped => actions.push(weight(
                ClusterAction::AddParticipant {
                    instance: instance.clone(),
                },
                5,
            )),
        }
        if participant.configured {
            actions.push(weight(
                ClusterAction::RemoveParticipant {
                    instance: instance.clone(),
                },
                3,
            ));
        }
    }

    let running_controller_count = model
        .controllers
        .values()
        .filter(|lifecycle| matches!(lifecycle, ControllerLifecycle::Running))
        .count();
    for (controller, lifecycle) in &model.controllers {
        match lifecycle {
            ControllerLifecycle::Running if running_controller_count > 1 => {
                actions.push(weight(
                    ClusterAction::StopController {
                        controller: controller.clone(),
                    },
                    6,
                ));
            }
            ControllerLifecycle::Running => {}
            ControllerLifecycle::Stopped => actions.push(weight(
                ClusterAction::StartController {
                    controller: controller.clone(),
                },
                4,
            )),
        }
    }

    if model.resources.len() < 4 {
        for index in 1..=3 {
            let name = format!("resource-{index}");
            if !model.resources.contains_key(&name) {
                actions.push(weight(
                    ClusterAction::AddResource {
                        resource_spec: ResourceSpec {
                            name,
                            partition_count: 8,
                            replicas: 2,
                            state_model: String::from("LeaderStandby"),
                            placement: Placement::Crush,
                        },
                    },
                    4,
                ));
            }
        }
    }

    for resource in model.resources.values() {
        for placement in placement_variants(model, resource) {
            if placement != resource.placement {
                actions.push(weight(
                    ClusterAction::ChangeResourcePlacement {
                        resource: resource.name.clone(),
                        placement,
                    },
                    3,
                ));
            }
        }
    }

    let maximum_replicas = 3.min(model.configured_instance_count());
    for resource in model.resources.values() {
        for replicas in 1..=maximum_replicas {
            if replicas != resource.replica_count
                && !matches!(resource.placement, Placement::SemiAuto { .. })
            {
                actions.push(weight(
                    ClusterAction::ChangeReplicaCount {
                        resource: resource.name.clone(),
                        replicas,
                    },
                    4,
                ));
            }
        }
    }

    for (instance, participant) in &model.participants {
        if participant.configured && matches!(participant.lifecycle, ParticipantLifecycle::Stopped)
        {
            for zone in ["zone-a", "zone-b", "zone-c", "zone-d", "zone-e"] {
                if zone != participant.zone {
                    actions.push(weight(
                        ClusterAction::ChangeInstanceZone {
                            instance: instance.clone(),
                            zone: zone.to_owned(),
                        },
                        2,
                    ));
                }
            }
        }
    }

    for max_in_flight in [1, 2] {
        let limits = if max_in_flight == 1 {
            vec![ThrottleSpec {
                scope: String::from("CLUSTER"),
                rebalance_type: String::from("ANY"),
                max_in_flight,
            }]
        } else {
            Vec::new()
        };
        if limits != model.transition_limits {
            actions.push(weight(ClusterAction::ChangeTransitionLimits { limits }, 2));
        }
    }

    if model.paused_handlers.is_empty() {
        for (instance, participant) in &model.participants {
            if matches!(participant.lifecycle, ParticipantLifecycle::Running { .. }) {
                actions.push(weight(
                    ClusterAction::PauseTransitions {
                        instance: instance.clone(),
                    },
                    5,
                ));
            }
        }
    } else {
        for instance in &model.paused_handlers {
            actions.push(weight(
                ClusterAction::ResumeTransitions {
                    instance: instance.clone(),
                },
                5,
            ));
        }
    }

    actions.push(weight(ClusterAction::Stabilize, ACTION_WEIGHT_STABILIZE));
    actions
}

fn weight(action: ClusterAction, weight: u64) -> WeightedAction {
    WeightedAction { action, weight }
}

fn placement_variants(
    model: &ClusterModel,
    resource: &super::model::ResourceModel,
) -> Vec<Placement> {
    let mut variants = vec![
        Placement::Crush,
        Placement::CrushWithTopology {
            path: String::from("/zone/instance"),
            fault_zone_type: String::from("zone"),
            end_node_type: String::from("instance"),
        },
    ];
    let instances = model
        .participants
        .iter()
        .filter(|(_, participant)| participant.configured)
        .map(|(instance, _)| instance.clone())
        .collect::<Vec<_>>();
    if instances.len() >= resource.replica_count {
        let preference_lists = (0..resource.partition_count)
            .map(|partition| {
                (
                    format!("{}_{}", resource.name, partition),
                    instances[..resource.replica_count].to_vec(),
                )
            })
            .collect();
        variants.push(Placement::SemiAuto { preference_lists });
    }
    variants
}

#[cfg(test)]
mod tests {
    use super::{next_action, ClusterAction};
    use crate::random_cluster::model::ClusterModel;
    use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
    use std::collections::BTreeSet;

    #[test]
    fn default_seed_corpus_exercises_every_action_kind() {
        let seeds = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0x1234_5678, 0xdead_beef];
        let mut kinds = BTreeSet::new();
        for seed in seeds {
            let mut model = ClusterModel::initial();
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            for _ in 0..40 {
                let action = next_action(&model, &mut rng);
                kinds.insert(action_kind(&action));
                model.apply(&action);
            }
        }
        assert_eq!(
            kinds,
            BTreeSet::from([
                "AddParticipant",
                "AddResource",
                "ChangeInstanceZone",
                "ChangeResourcePlacement",
                "ChangeReplicaCount",
                "ChangeTransitionLimits",
                "PauseTransitions",
                "RemoveParticipant",
                "RestartParticipant",
                "ResumeTransitions",
                "Stabilize",
                "StartController",
                "StartParticipant",
                "StopController",
                "StopParticipant",
            ])
        );
    }

    fn action_kind(action: &ClusterAction) -> &'static str {
        match action {
            ClusterAction::StopParticipant { .. } => "StopParticipant",
            ClusterAction::StartParticipant { .. } => "StartParticipant",
            ClusterAction::RestartParticipant { .. } => "RestartParticipant",
            ClusterAction::StopController { .. } => "StopController",
            ClusterAction::StartController { .. } => "StartController",
            ClusterAction::AddParticipant { .. } => "AddParticipant",
            ClusterAction::RemoveParticipant { .. } => "RemoveParticipant",
            ClusterAction::AddResource { .. } => "AddResource",
            ClusterAction::RemoveResource { .. } => "RemoveResource",
            ClusterAction::ChangeReplicaCount { .. } => "ChangeReplicaCount",
            ClusterAction::ChangeInstanceZone { .. } => "ChangeInstanceZone",
            ClusterAction::ChangeResourcePlacement { .. } => "ChangeResourcePlacement",
            ClusterAction::ChangeTransitionLimits { .. } => "ChangeTransitionLimits",
            ClusterAction::PauseTransitions { .. } => "PauseTransitions",
            ClusterAction::ResumeTransitions { .. } => "ResumeTransitions",
            ClusterAction::Stabilize => "Stabilize",
        }
    }
}
