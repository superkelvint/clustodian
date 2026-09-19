use super::action::{ClusterAction, Placement, ThrottleSpec};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ControllerLifecycle {
    Running,
    Stopped,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ParticipantLifecycle {
    Running { incarnation: u64 },
    Stopped,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParticipantModel {
    pub zone: String,
    pub configured: bool,
    pub started: bool,
    pub last_incarnation: u64,
    pub lifecycle: ParticipantLifecycle,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResourceModel {
    pub name: String,
    pub partition_count: usize,
    pub replica_count: usize,
    pub state_model: String,
    pub eligible_instances: BTreeSet<String>,
    pub placement: Placement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClusterModel {
    pub controllers: BTreeMap<String, ControllerLifecycle>,
    pub participants: BTreeMap<String, ParticipantModel>,
    pub resources: BTreeMap<String, ResourceModel>,
    pub paused_handlers: BTreeSet<String>,
    pub transition_limits: Vec<ThrottleSpec>,
}

impl ClusterModel {
    pub fn initial() -> Self {
        let controllers = ["controller-a", "controller-b", "controller-c"]
            .into_iter()
            .map(|name| (name.to_owned(), ControllerLifecycle::Running))
            .collect();
        let participants = [
            ("node-a", "zone-a", true),
            ("node-b", "zone-b", true),
            ("node-c", "zone-c", true),
            ("node-d", "zone-d", false),
            ("node-e", "zone-e", false),
        ]
        .into_iter()
        .map(|(name, zone, started)| {
            (
                name.to_owned(),
                ParticipantModel {
                    zone: zone.to_owned(),
                    configured: true,
                    started,
                    last_incarnation: u64::from(started),
                    lifecycle: if started {
                        ParticipantLifecycle::Running { incarnation: 1 }
                    } else {
                        ParticipantLifecycle::Stopped
                    },
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
        let eligible_instances = participants.keys().cloned().collect();
        let resource = ResourceModel {
            name: String::from("resource-0"),
            partition_count: 8,
            replica_count: 2,
            state_model: String::from("LeaderStandby"),
            eligible_instances,
            placement: Placement::Crush,
        };
        Self {
            controllers,
            participants,
            resources: BTreeMap::from([(resource.name.clone(), resource)]),
            paused_handlers: BTreeSet::new(),
            transition_limits: Vec::new(),
        }
    }

    pub fn configured_instance_count(&self) -> usize {
        self.participants
            .values()
            .filter(|participant| participant.configured)
            .count()
    }

    pub fn apply(&mut self, action: &ClusterAction) {
        match action {
            ClusterAction::StopParticipant { instance } => {
                self.participants
                    .get_mut(instance)
                    .expect("action precondition keeps participant known")
                    .lifecycle = ParticipantLifecycle::Stopped;
            }
            ClusterAction::StartParticipant { instance } => {
                let participant = self
                    .participants
                    .get_mut(instance)
                    .expect("action precondition keeps participant known");
                let incarnation = match participant.lifecycle {
                    ParticipantLifecycle::Stopped => participant.last_incarnation + 1,
                    ParticipantLifecycle::Running { .. } => {
                        unreachable!("start action requires a stopped participant")
                    }
                };
                participant.started = true;
                participant.last_incarnation = incarnation;
                participant.lifecycle = ParticipantLifecycle::Running { incarnation };
            }
            ClusterAction::RestartParticipant { instance } => {
                let participant = self
                    .participants
                    .get_mut(instance)
                    .expect("action precondition keeps participant known");
                let incarnation = match participant.lifecycle {
                    ParticipantLifecycle::Running { .. } => participant.last_incarnation + 1,
                    ParticipantLifecycle::Stopped => {
                        unreachable!("restart action requires a running participant")
                    }
                };
                participant.last_incarnation = incarnation;
                participant.lifecycle = ParticipantLifecycle::Running { incarnation };
            }
            ClusterAction::RemoveParticipant { instance } => {
                let participant = self
                    .participants
                    .get_mut(instance)
                    .expect("action precondition keeps participant known");
                participant.configured = false;
                participant.started = false;
                participant.lifecycle = ParticipantLifecycle::Stopped;
                self.paused_handlers.remove(instance);
            }
            ClusterAction::StopController { controller } => {
                *self
                    .controllers
                    .get_mut(controller)
                    .expect("action precondition keeps controller known") =
                    ControllerLifecycle::Stopped;
            }
            ClusterAction::StartController { controller } => {
                *self
                    .controllers
                    .get_mut(controller)
                    .expect("action precondition keeps controller known") =
                    ControllerLifecycle::Running;
            }
            ClusterAction::AddParticipant { instance } => {
                let participant = self
                    .participants
                    .get_mut(instance)
                    .expect("action precondition keeps participant known");
                participant.started = true;
                participant.configured = true;
                participant.last_incarnation = 1;
                participant.lifecycle = ParticipantLifecycle::Running { incarnation: 1 };
            }
            ClusterAction::AddResource { resource_spec } => {
                self.resources.insert(
                    resource_spec.name.clone(),
                    ResourceModel {
                        name: resource_spec.name.clone(),
                        partition_count: resource_spec.partition_count,
                        replica_count: resource_spec.replicas,
                        state_model: resource_spec.state_model.clone(),
                        eligible_instances: self.participants.keys().cloned().collect(),
                        placement: resource_spec.placement.clone(),
                    },
                );
            }
            ClusterAction::ChangeReplicaCount { resource, replicas } => {
                self.resources
                    .get_mut(resource)
                    .expect("action precondition keeps resource known")
                    .replica_count = *replicas;
            }
            ClusterAction::RemoveResource { resource } => {
                self.resources.remove(resource);
            }
            ClusterAction::ChangeInstanceZone { instance, zone } => {
                self.participants
                    .get_mut(instance)
                    .expect("action precondition keeps participant known")
                    .zone = zone.clone();
            }
            ClusterAction::ChangeResourcePlacement {
                resource,
                placement,
            } => {
                self.resources
                    .get_mut(resource)
                    .expect("action precondition keeps resource known")
                    .placement = placement.clone();
            }
            ClusterAction::ChangeTransitionLimits { limits } => {
                self.transition_limits = limits.clone();
            }
            ClusterAction::PauseTransitions { instance } => {
                self.paused_handlers.insert(instance.clone());
            }
            ClusterAction::ResumeTransitions { instance } => {
                self.paused_handlers.remove(instance);
            }
            ClusterAction::Stabilize => {}
        }
        let configured: BTreeSet<String> = self
            .participants
            .iter()
            .filter(|(_, participant)| participant.configured)
            .map(|(instance, _)| instance.clone())
            .collect();
        for resource in self.resources.values_mut() {
            resource.eligible_instances = configured.clone();
        }
    }
}
