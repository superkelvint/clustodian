use super::action::{ClusterAction, Placement, ResourceSpec};
use super::invariants::{check_convergence, check_safety, MessageHistory};
use super::model::ClusterModel;
use crate::support::EtcdFixture;
use clustodian::admin::{
    ClusterAdmin, InstanceSpec, PlacementSpec, ResourceSpec as AdminResourceSpec,
    ThrottleSpec as AdminThrottleSpec,
};
use clustodian::coordination::etcd::CoordinationError;
use clustodian::model::{leader_standby, InstanceId};
use clustodian::observe::{ClusterObserver, ClusterSnapshot};
use clustodian::participant::{
    ParticipantRuntime, ParticipantRuntimeError, TransitionExecution, TransitionHandler,
    TransitionHandlerError,
};
use clustodian::runtime::{ControllerRuntime, ControllerRuntimeConfig};
use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const CONTROLLER_LEASE_TTL_MS: u64 = 1_000;
const PARTICIPANT_LEASE_TTL: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const WAIT_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Clone)]
struct TransitionGate {
    state: Arc<(Mutex<GateState>, Condvar)>,
}

struct GateState {
    paused: bool,
    active_handlers: usize,
}

impl TransitionGate {
    fn new() -> Self {
        Self {
            state: Arc::new((
                Mutex::new(GateState {
                    paused: false,
                    active_handlers: 0,
                }),
                Condvar::new(),
            )),
        }
    }

    fn pause(&self) {
        let (state, notify) = &*self.state;
        let mut state = state.lock().expect("transition gate mutex is not poisoned");
        state.paused = true;
        while state.active_handlers != 0 {
            state = notify
                .wait(state)
                .expect("transition gate mutex is not poisoned");
        }
    }

    fn resume(&self) {
        let (state, notify) = &*self.state;
        state
            .lock()
            .expect("transition gate mutex is not poisoned")
            .paused = false;
        notify.notify_all();
    }

    fn enter(&self) -> Result<TransitionPermit, TransitionHandlerError> {
        let (state, notify) = &*self.state;
        let mut state = state
            .lock()
            .map_err(|_| TransitionHandlerError::new("transition gate mutex is poisoned"))?;
        while state.paused {
            state = notify
                .wait(state)
                .map_err(|_| TransitionHandlerError::new("transition gate mutex is poisoned"))?;
        }
        state.active_handlers += 1;
        drop(state);
        Ok(TransitionPermit {
            state: Arc::clone(&self.state),
        })
    }
}

struct TransitionPermit {
    state: Arc<(Mutex<GateState>, Condvar)>,
}

impl Drop for TransitionPermit {
    fn drop(&mut self) {
        let (state, notify) = &*self.state;
        let mut state = state.lock().expect("transition gate mutex is not poisoned");
        state.active_handlers -= 1;
        notify.notify_all();
    }
}

#[derive(Clone)]
struct ControlledHandler {
    gate: Arc<TransitionGate>,
}

impl TransitionHandler for ControlledHandler {
    fn handle(&self, _execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        let _permit = self.gate.enter()?;
        Ok(())
    }
}

pub struct RandomCluster<'a> {
    fixture: &'a EtcdFixture,
    pub prefix: String,
    backend: clustodian::coordination::etcd::EtcdCoordination,
    admin: ClusterAdmin,
    observer: ClusterObserver,
    controllers: BTreeMap<String, JoinHandle<clustodian::Result<()>>>,
    participants: BTreeMap<String, JoinHandle<Result<(), ParticipantRuntimeError>>>,
    gates: BTreeMap<String, Arc<TransitionGate>>,
    zones: BTreeMap<String, String>,
    resource_specs: BTreeMap<String, ResourceSpec>,
    last_sessions: BTreeMap<String, u64>,
    message_history: MessageHistory,
    action_rejected: bool,
}

impl<'a> RandomCluster<'a> {
    pub async fn new(
        fixture: &'a EtcdFixture,
        seed: u64,
        case_index: usize,
    ) -> Result<Self, String> {
        let prefix = format!(
            "clustodian-random-{}/case-{case_index}-seed-{seed:016x}",
            std::process::id()
        );
        let cluster_name = format!("random-{case_index}-{seed:016x}");
        let backend = fixture
            .connect_namespace(&prefix, &cluster_name)
            .await
            .ok_or_else(|| String::from("real etcd was unavailable"))?;
        let admin = ClusterAdmin::new(backend.clone());
        admin
            .ensure_cluster(&cluster_name)
            .await
            .map_err(|error| format!("ensure cluster metadata: {error}"))?;
        for (name, zone) in [
            ("node-a", "zone-a"),
            ("node-b", "zone-b"),
            ("node-c", "zone-c"),
            ("node-d", "zone-d"),
            ("node-e", "zone-e"),
        ] {
            admin
                .put_instance(InstanceSpec {
                    instance_id: name.to_owned(),
                    zone: zone.to_owned(),
                })
                .await
                .map_err(|error| format!("configure {name}: {error}"))?;
        }
        put_resource(
            &admin,
            &ResourceSpec {
                name: String::from("resource-0"),
                partition_count: 8,
                replicas: 2,
                state_model: String::from("LeaderStandby"),
                placement: Placement::Crush,
            },
        )
        .await?;

        let gates = ["node-a", "node-b", "node-c", "node-d", "node-e"]
            .into_iter()
            .map(|name| (name.to_owned(), Arc::new(TransitionGate::new())))
            .collect();
        let zones = [
            ("node-a", "zone-a"),
            ("node-b", "zone-b"),
            ("node-c", "zone-c"),
            ("node-d", "zone-d"),
            ("node-e", "zone-e"),
        ]
        .into_iter()
        .map(|(instance, zone)| (instance.to_owned(), zone.to_owned()))
        .collect();
        let initial_resource = ResourceSpec {
            name: String::from("resource-0"),
            partition_count: 8,
            replicas: 2,
            state_model: String::from("LeaderStandby"),
            placement: Placement::Crush,
        };
        Ok(Self {
            fixture,
            prefix,
            backend: backend.clone(),
            admin,
            observer: ClusterObserver::new(backend),
            controllers: BTreeMap::new(),
            participants: BTreeMap::new(),
            gates,
            zones,
            resource_specs: BTreeMap::from([(initial_resource.name.clone(), initial_resource)]),
            last_sessions: BTreeMap::new(),
            message_history: MessageHistory::default(),
            action_rejected: false,
        })
    }

    pub async fn start_initial(&mut self) -> Result<(), String> {
        for instance in ["node-a", "node-b", "node-c"] {
            self.start_participant(instance).await?;
        }
        for controller in ["controller-a", "controller-b", "controller-c"] {
            self.start_controller(controller).await?;
        }
        Ok(())
    }

    pub async fn execute(&mut self, action: &ClusterAction) -> Result<(), String> {
        self.action_rejected = false;
        match action {
            ClusterAction::StopParticipant { instance } => self.stop_participant(instance).await,
            ClusterAction::StartParticipant { instance } => self.start_participant(instance).await,
            ClusterAction::RestartParticipant { instance } => {
                self.stop_participant(instance).await?;
                self.start_participant(instance).await
            }
            ClusterAction::StopController { controller } => self.stop_controller(controller).await,
            ClusterAction::StartController { controller } => {
                self.start_controller(controller).await
            }
            ClusterAction::AddParticipant { instance } => self.start_participant(instance).await,
            ClusterAction::RemoveParticipant { instance } => self
                .admin
                .remove_instance(instance)
                .await
                .or_else(|error| match error {
                    CoordinationError::InstanceStillLive(_) => {
                        self.action_rejected = true;
                        Ok(())
                    }
                    error => Err(format!("remove participant {instance}: {error}")),
                }),
            ClusterAction::AddResource { resource_spec } => {
                put_resource(&self.admin, resource_spec).await?;
                self.resource_specs
                    .insert(resource_spec.name.clone(), resource_spec.clone());
                Ok(())
            }
            ClusterAction::RemoveResource { resource } => {
                match self.admin.remove_resource(resource).await {
                    Ok(()) => Ok(()),
                    Err(CoordinationError::ResourceDeletionDisabled) => {
                        self.action_rejected = true;
                        Ok(())
                    }
                    Err(error) => Err(format!("remove resource {resource}: {error}")),
                }
            }
            ClusterAction::ChangeReplicaCount { resource, replicas } => {
                let mut current = self
                    .resource_specs
                    .get(resource)
                    .cloned()
                    .ok_or_else(|| format!("unknown resource {resource}"))?;
                current.replicas = *replicas;
                put_resource(&self.admin, &current).await?;
                self.resource_specs.insert(resource.clone(), current);
                Ok(())
            }
            ClusterAction::ChangeInstanceZone { instance, zone } => {
                self.admin
                    .put_instance(InstanceSpec {
                        instance_id: instance.clone(),
                        zone: zone.clone(),
                    })
                    .await
                    .map_err(|error| format!("change zone for {instance}: {error}"))?;
                self.zones.insert(instance.clone(), zone.clone());
                Ok(())
            }
            ClusterAction::ChangeResourcePlacement {
                resource,
                placement,
            } => {
                let mut current = self
                    .resource_specs
                    .get(resource)
                    .cloned()
                    .ok_or_else(|| format!("unknown resource {resource}"))?;
                current.placement = placement.clone();
                put_resource(&self.admin, &current).await?;
                self.resource_specs.insert(resource.clone(), current);
                Ok(())
            }
            ClusterAction::ChangeTransitionLimits { limits } => {
                let limits = limits
                    .iter()
                    .map(|limit| AdminThrottleSpec {
                        scope: limit.scope.clone(),
                        rebalance_type: limit.rebalance_type.clone(),
                        max_in_flight: limit.max_in_flight,
                    })
                    .collect();
                self.admin
                    .put_throttles(limits)
                    .await
                    .map_err(|error| format!("change transition limits: {error}"))
            }
            ClusterAction::PauseTransitions { instance } => {
                self.gates
                    .get(instance)
                    .ok_or_else(|| format!("unknown participant {instance}"))?
                    .pause();
                Ok(())
            }
            ClusterAction::ResumeTransitions { instance } => {
                self.gates
                    .get(instance)
                    .ok_or_else(|| format!("unknown participant {instance}"))?
                    .resume();
                Ok(())
            }
            ClusterAction::Stabilize => Ok(()),
        }?;
        self.wait_until(
            "pending transitions to agree with active CurrentState",
            |snapshot| {
                snapshot.pending_transitions.iter().all(|message| {
                    let state = snapshot
                        .active_current_state
                        .get(&message.instance)
                        .and_then(|current| current.resources.get(&message.resource))
                        .and_then(|partitions| partitions.get(&message.partition));
                    state.is_none() || state != Some(&message.to)
                })
            },
        )
        .await
        .map(|_| ())
    }

    pub fn action_rejected(&self) -> bool {
        self.action_rejected
    }

    pub async fn snapshot(&mut self) -> Result<ClusterSnapshot, String> {
        self.reap_finished_tasks().await?;
        self.observer
            .snapshot()
            .await
            .map_err(|error| format!("observe cluster: {error}"))
    }

    pub fn check_safety(
        &mut self,
        model: &ClusterModel,
        snapshot: &ClusterSnapshot,
    ) -> Result<(), String> {
        check_safety(model, snapshot, &mut self.message_history)
    }

    pub async fn wait_until_converged(
        &mut self,
        model: &ClusterModel,
    ) -> Result<ClusterSnapshot, String> {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            let snapshot = self.snapshot().await?;
            self.check_safety(model, &snapshot)?;
            match check_convergence(model, &snapshot) {
                Ok(()) => return Ok(snapshot),
                Err(error) if Instant::now() >= deadline => {
                    return Err(format!(
                        "timed out waiting for convergence: {error}; last snapshot: {snapshot:?}"
                    ));
                }
                Err(_) => {}
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    pub async fn wait_until_initial_converged(
        &mut self,
        model: &ClusterModel,
    ) -> Result<ClusterSnapshot, String> {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            let snapshot = self.snapshot().await?;
            if check_convergence(model, &snapshot).is_ok() {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for initial convergence; last snapshot: {snapshot:?}"
                ));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    pub async fn wait_until_controller_caught_up(&mut self) -> Result<ClusterSnapshot, String> {
        self.wait_until("controller to process authoritative input", |snapshot| {
            snapshot
                .processed_revision
                .is_some_and(|processed| processed >= snapshot.authoritative_revision)
        })
        .await
    }

    pub fn reset_message_history(&mut self) {
        self.message_history = MessageHistory::default();
    }

    pub fn note_controller_failover(&mut self) {
        self.message_history.note_controller_failover();
    }

    pub async fn shutdown(&mut self) -> Result<(), String> {
        for gate in self.gates.values() {
            gate.resume();
        }
        let controller_ids = self.controllers.keys().cloned().collect::<Vec<_>>();
        for controller in controller_ids {
            if let Some(task) = self.controllers.remove(&controller) {
                task.abort();
                let _ = task.await;
            }
        }
        let participant_ids = self.participants.keys().cloned().collect::<Vec<_>>();
        for instance in participant_ids {
            if let Some(task) = self.participants.remove(&instance) {
                task.abort();
                let _ = task.await;
            }
        }
        for instance in self.gates.keys() {
            if let Some(session) = self
                .backend
                .live_session(&InstanceId::new(instance).map_err(|error| error.to_string())?)
                .await
                .map_err(|error| format!("read {instance} during cleanup: {error}"))?
            {
                self.backend
                    .revoke_live(
                        &InstanceId::new(instance).map_err(|error| error.to_string())?,
                        session,
                    )
                    .await
                    .map_err(|error| format!("revoke {instance} during cleanup: {error}"))?;
            }
        }
        self.fixture.cleanup_namespace(&self.prefix).await
    }

    async fn start_participant(&mut self, instance: &str) -> Result<(), String> {
        if self.participants.contains_key(instance) {
            return Err(format!("participant {instance} is already running"));
        }
        let instance_id =
            InstanceId::new(instance).map_err(|error| format!("participant identity: {error}"))?;
        let gate = self
            .gates
            .get(instance)
            .ok_or_else(|| format!("unknown participant {instance}"))?
            .clone();
        let zone = self
            .zones
            .get(instance)
            .cloned()
            .ok_or_else(|| format!("unknown participant {instance}"))?;
        self.admin
            .put_instance(InstanceSpec {
                instance_id: instance.to_owned(),
                zone,
            })
            .await
            .map_err(|error| format!("restore participant {instance} configuration: {error}"))?;
        let (ready_tx, ready_rx) = oneshot::channel();
        let runtime = ParticipantRuntime::new(
            self.backend.clone(),
            instance_id.clone(),
            leader_standby(),
            ControlledHandler { gate },
        )
        .with_lease_ttl(PARTICIPANT_LEASE_TTL)
        .map_err(|error| format!("configure participant {instance}: {error}"))?;
        let task = tokio::spawn(runtime.run(move || {
            ready_tx
                .send(())
                .map_err(|_| ParticipantRuntimeError::Io(String::from("ready receiver dropped")))
        }));
        self.participants.insert(instance.to_owned(), task);
        tokio::time::timeout(WAIT_TIMEOUT, ready_rx)
            .await
            .map_err(|_| format!("timed out waiting for participant {instance} readiness"))?
            .map_err(|_| format!("participant {instance} stopped before readiness"))?;
        let snapshot = self
            .wait_until(
                &format!("participant {instance} to become live"),
                |snapshot| snapshot.live_instances.contains_key(instance),
            )
            .await?;
        let session = snapshot
            .live_instances
            .get(instance)
            .copied()
            .ok_or_else(|| format!("participant {instance} was not live after readiness"))?;
        if let Some(previous) = self.last_sessions.insert(instance.to_owned(), session) {
            if previous == session {
                return Err(format!(
                    "participant {instance} reused SessionId {session} after restart"
                ));
            }
        }
        self.wait_until(
            &format!("participant {instance} pending work to be fenced"),
            |snapshot| {
                snapshot
                    .pending_transitions
                    .iter()
                    .filter(|message| message.instance == instance)
                    .all(|message| message.target_session == session)
            },
        )
        .await?;
        Ok(())
    }

    async fn stop_participant(&mut self, instance: &str) -> Result<(), String> {
        let task = self
            .participants
            .remove(instance)
            .ok_or_else(|| format!("participant {instance} is not running"))?;
        task.abort();
        let _ = task.await;
        self.wait_until(
            &format!("participant {instance} to leave after crash"),
            |snapshot| !snapshot.live_instances.contains_key(instance),
        )
        .await?;
        self.wait_until(
            &format!("participant {instance} pending work to be fenced"),
            |snapshot| {
                snapshot
                    .pending_transitions
                    .iter()
                    .all(|message| message.instance != instance)
            },
        )
        .await
        .map(|_| ())
    }

    async fn start_controller(&mut self, controller: &str) -> Result<(), String> {
        if self.controllers.contains_key(controller) {
            return Err(format!("controller {controller} is already running"));
        }
        let runtime = ControllerRuntime::new(
            self.backend.clone(),
            ControllerRuntimeConfig {
                cluster: self.backend.cluster().to_owned(),
                controller_id: controller.to_owned(),
                lease_ttl_ms: CONTROLLER_LEASE_TTL_MS,
            },
        )
        .await
        .map_err(|error| format!("create controller {controller}: {error}"))?;
        let task = tokio::spawn(runtime.run());
        self.controllers.insert(controller.to_owned(), task);
        self.wait_until(&format!("controller {controller} candidacy"), |snapshot| {
            snapshot
                .controllers
                .active
                .iter()
                .any(|active| active == controller)
                || snapshot
                    .controllers
                    .standby
                    .iter()
                    .any(|standby| standby == controller)
        })
        .await
        .map(|_| ())
    }

    async fn stop_controller(&mut self, controller: &str) -> Result<(), String> {
        let task = self
            .controllers
            .remove(controller)
            .ok_or_else(|| format!("controller {controller} is not running"))?;
        task.abort();
        let _ = task.await;
        self.wait_until(
            &format!("controller {controller} lease to expire"),
            |snapshot| {
                !snapshot
                    .controllers
                    .active
                    .iter()
                    .any(|active| active == controller)
                    && !snapshot
                        .controllers
                        .standby
                        .iter()
                        .any(|standby| standby == controller)
            },
        )
        .await
        .map(|_| ())
    }

    async fn wait_until(
        &mut self,
        description: &str,
        predicate: impl Fn(&ClusterSnapshot) -> bool,
    ) -> Result<ClusterSnapshot, String> {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            self.reap_finished_tasks().await?;
            let snapshot = self
                .observer
                .snapshot()
                .await
                .map_err(|error| format!("observe while waiting for {description}: {error}"))?;
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for {description}; last snapshot: {snapshot:?}"
                ));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    async fn reap_finished_tasks(&mut self) -> Result<(), String> {
        let controllers = self
            .controllers
            .iter()
            .filter(|(_, task)| task.is_finished())
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if let Some(controller) = controllers.into_iter().next() {
            let task = self
                .controllers
                .remove(&controller)
                .expect("finished controller task remains registered");
            match task.await {
                Ok(Ok(())) => return Err(format!("controller {controller} exited unexpectedly")),
                Ok(Err(error)) => return Err(format!("controller {controller} failed: {error}")),
                Err(error) => return Err(format!("controller {controller} task failed: {error}")),
            }
        }
        let participants = self
            .participants
            .iter()
            .filter(|(_, task)| task.is_finished())
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if let Some(instance) = participants.into_iter().next() {
            let task = self
                .participants
                .remove(&instance)
                .expect("finished participant task remains registered");
            match task.await {
                Ok(Ok(())) => return Err(format!("participant {instance} exited unexpectedly")),
                Ok(Err(error)) => return Err(format!("participant {instance} failed: {error}")),
                Err(error) => return Err(format!("participant {instance} task failed: {error}")),
            }
        }
        Ok(())
    }
}

async fn put_resource(admin: &ClusterAdmin, spec: &ResourceSpec) -> Result<(), String> {
    let placement = match &spec.placement {
        Placement::Crush => PlacementSpec::Crush,
        Placement::CrushWithTopology {
            path,
            fault_zone_type,
            end_node_type,
        } => PlacementSpec::CrushWithTopology {
            topology: clustodian::admin::CrushTopologySpec::new(
                path.clone(),
                fault_zone_type.clone(),
                end_node_type.clone(),
            ),
        },
        Placement::SemiAuto { preference_lists } => PlacementSpec::SemiAuto {
            preference_lists: preference_lists.clone(),
        },
    };
    admin
        .put_resource(AdminResourceSpec {
            name: spec.name.clone(),
            partitions: spec.partition_count,
            replicas: spec.replicas,
            state_model: spec.state_model.clone(),
            placement,
        })
        .await
        .map_err(|error| format!("write resource {}: {error}", spec.name))
}
