//! The M10 watch-driven controller runtime.

use crate::controller::{
    compute_intermediate_and_throttle, generate_transitions, select_transitions,
    OperationalPendingTransition, RebalanceType, ResourceTransitionInput,
    StateTransitionThrottleConfig, ThrottleScope,
};
use crate::coordination::etcd::{
    is_lease_backed_delete, CoordinationError, CoordinationSnapshot, EtcdCoordination, WatchError,
};
use crate::election::{ControllerAuthority, Leadership};
use crate::model::{
    leader_standby, CurrentState, ExternalView, IdealState, InstanceId, PartitionId, ResourceId,
    SessionId, State,
};
use crate::observability::{emit, RuntimeEvent, RuntimeEventHook};
use crate::rebalance::{compute_crush_assignment, compute_semi_auto_best_possible_state};
use crate::transition::PendingTransition;
use crate::transition::TransitionRequest;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

const INSTANCE_CONFIGS_KEY: &str = "controller/instance-configs";
const THROTTLES_KEY: &str = "controller/throttles";
const RESOURCE_PREFIX: &str = "controller/resources/";
const EXTERNAL_OUTPUT_KEY: &str = "controller/output/external-view";
const PENDING_OUTPUT_KEY: &str = "controller/output/pending-transitions";
const PROCESSED_OUTPUT_KEY: &str = "controller/output/processed-revision";

#[derive(Clone, Copy)]
enum ReconcileMode {
    Startup,
    Normal,
    SessionReplacement,
}

struct ControllerState {
    pending: Vec<PublishedTransition>,
    known_sessions: BTreeMap<InstanceId, SessionId>,
    authoritative_revision: crate::coordination::etcd::Revision,
}

impl ControllerState {
    fn from_snapshot(snapshot: &CoordinationSnapshot) -> Result<Self, ControllerRuntimeError> {
        Ok(Self {
            pending: read_pending(snapshot)?,
            known_sessions: live_sessions(snapshot),
            // The authoritative-input marker is the durable watermark for
            // supported mutations. Live-session compares in the publication
            // transaction additionally cover lease-expiry deletions.
            authoritative_revision: snapshot.authoritative_revision(),
        })
    }

    fn observe_input_revision(&mut self, revision: crate::coordination::etcd::Revision) {
        self.authoritative_revision = self.authoritative_revision.max(revision);
    }

    fn mode_for(&self, snapshot: &CoordinationSnapshot) -> ReconcileMode {
        if session_replaced(&self.known_sessions, snapshot) {
            ReconcileMode::SessionReplacement
        } else {
            ReconcileMode::Normal
        }
    }

    fn remember_sessions(&mut self, snapshot: &CoordinationSnapshot) {
        for (instance, live) in snapshot.participants().live_instances() {
            self.known_sessions
                .insert(instance.clone(), live.session_id());
        }
    }
}

/// A pending state-transition message published by the controller.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PublishedTransition {
    pub resource: String,
    pub partition: String,
    pub instance: String,
    pub target_session: u64,
    pub from: String,
    pub to: String,
    pub message_type: String,
    /// Stable identity for this controller-created transition attempt.
    pub message_id: String,
}

/// Errors raised while decoding or running the controller runtime.
#[derive(Debug)]
pub struct ControllerRuntimeError {
    message: String,
    transient: bool,
}

impl ControllerRuntimeError {
    fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            transient: false,
        }
    }

    fn coordination(error: CoordinationError) -> Self {
        Self {
            transient: error.is_transient(),
            message: error.to_string(),
        }
    }

    fn is_transient(&self) -> bool {
        self.transient
    }
}

impl fmt::Display for ControllerRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ControllerRuntimeError {}

impl From<CoordinationError> for ControllerRuntimeError {
    fn from(error: CoordinationError) -> Self {
        Self::coordination(error)
    }
}

impl From<WatchError> for ControllerRuntimeError {
    fn from(error: WatchError) -> Self {
        match error {
            WatchError::Coordination(error) => Self::coordination(error),
            WatchError::Disconnected => Self {
                message: WatchError::Disconnected.to_string(),
                transient: true,
            },
            WatchError::Compacted { revision } => {
                Self::message(WatchError::Compacted { revision }.to_string())
            }
        }
    }
}

pub(crate) enum LeaderRunResult {
    LeadershipLost,
    Shutdown,
}

/// The watch-driven controller reconciler run under controller authority.
pub struct ControllerReconciler {
    backend: EtcdCoordination,
    ready_callback: Option<ReadyCallback>,
    event_hook: Option<RuntimeEventHook>,
}

type ReadyCallback = Box<dyn FnOnce() -> Result<(), Box<dyn Error + Send + Sync>> + Send + Sync>;

impl ControllerReconciler {
    /// Construct a reconciler that publishes controller outputs through etcd.
    pub fn new(backend: EtcdCoordination) -> Self {
        Self {
            backend,
            ready_callback: None,
            event_hook: None,
        }
    }

    /// Set a one-shot callback invoked after the initial output commit.
    pub fn on_ready<F, E>(mut self, callback: F) -> Self
    where
        F: FnOnce() -> Result<(), E> + Send + Sync + 'static,
        E: Error + Send + Sync + 'static,
    {
        self.ready_callback = Some(Box::new(move || {
            callback().map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>)
        }));
        self
    }

    pub(crate) fn on_ready_callback(mut self, callback: ReadyCallback) -> Self {
        self.ready_callback = Some(callback);
        self
    }

    pub(crate) fn on_event_callback(mut self, hook: RuntimeEventHook) -> Self {
        self.event_hook = Some(hook);
        self
    }

    pub(crate) async fn run_as_leader_until<F>(
        &mut self,
        leadership: &Leadership,
        mut shutdown: Pin<&mut F>,
    ) -> Result<LeaderRunResult, ControllerRuntimeError>
    where
        F: Future<Output = ()> + Send,
    {
        let authority = leadership.authority();
        let (snapshot, mut watch) = match retry_coordination(leadership, shutdown.as_mut(), || {
            self.backend.snapshot_and_watch_namespace()
        })
        .await
        {
            Ok(value) => value,
            Err(LeaderWaitError::Shutdown) => return Ok(LeaderRunResult::Shutdown),
            Err(LeaderWaitError::LeadershipLost) => return Ok(LeaderRunResult::LeadershipLost),
            Err(LeaderWaitError::Coordination(error)) => return Err(error.into()),
        };
        crate::failpoints::hard_abort("controller_after_snapshot");
        let mut state = ControllerState::from_snapshot(&snapshot)?;
        let snapshot = snapshot.with_authoritative_revision(state.authoritative_revision);
        loop {
            match self
                .reconcile(
                    &snapshot,
                    &mut state.pending,
                    ReconcileMode::Startup,
                    &authority,
                )
                .await
            {
                Ok(()) => break,
                Err(error) if is_leadership_loss(&error) => {
                    return Ok(LeaderRunResult::LeadershipLost);
                }
                Err(error) if error.is_transient() => {
                    match wait_for_leader_retry(leadership, shutdown.as_mut()).await {
                        Ok(()) => {}
                        Err(LeaderWaitError::Shutdown) => return Ok(LeaderRunResult::Shutdown),
                        Err(LeaderWaitError::LeadershipLost) => {
                            return Ok(LeaderRunResult::LeadershipLost)
                        }
                        Err(LeaderWaitError::Coordination(error)) => return Err(error.into()),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(callback) = self.ready_callback.take() {
            callback().map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
        }

        loop {
            tokio::select! {
                () = shutdown.as_mut() => return Ok(LeaderRunResult::Shutdown),
                () = leadership.lost() => return Ok(LeaderRunResult::LeadershipLost),
                () = tokio::time::sleep(Duration::from_millis(250)) => {
                    let snapshot = match retry_coordination(
                        leadership,
                        shutdown.as_mut(),
                        || self.backend.controller_snapshot(),
                    ).await {
                        Ok(snapshot) => snapshot,
                        Err(LeaderWaitError::Shutdown) => return Ok(LeaderRunResult::Shutdown),
                        Err(LeaderWaitError::LeadershipLost) => return Ok(LeaderRunResult::LeadershipLost),
                        Err(LeaderWaitError::Coordination(error)) => return Err(error.into()),
                    };
                    loop {
                        match self.reconcile_current_snapshot(&snapshot, &mut state, &authority).await {
                            Ok(()) => break,
                            Err(error) if is_leadership_loss(&error) => return Ok(LeaderRunResult::LeadershipLost),
                            Err(error) if error.is_transient() => match wait_for_leader_retry(leadership, shutdown.as_mut()).await {
                                Ok(()) => {}
                                Err(LeaderWaitError::Shutdown) => return Ok(LeaderRunResult::Shutdown),
                                Err(LeaderWaitError::LeadershipLost) => return Ok(LeaderRunResult::LeadershipLost),
                                Err(LeaderWaitError::Coordination(error)) => return Err(error.into()),
                            },
                            Err(error) => return Err(error),
                        }
                    }
                }
                result = watch.next() => match result {
                    Ok(event) if is_controller_input(event.key()) => {
                        if is_lease_backed_delete(&event) {
                            match retry_coordination(
                                leadership,
                                shutdown.as_mut(),
                                || self.backend.record_lease_expiry(&authority, event.revision()),
                            )
                            .await
                            {
                                Ok(_) => {}
                                Err(LeaderWaitError::Shutdown) => {
                                    return Ok(LeaderRunResult::Shutdown)
                                }
                                Err(LeaderWaitError::LeadershipLost) => {
                                    return Ok(LeaderRunResult::LeadershipLost)
                                }
                                Err(LeaderWaitError::Coordination(error)) => {
                                    return Err(error.into())
                                }
                            }
                        }
                        state.observe_input_revision(event.revision());
                        let snapshot = match retry_coordination(
                            leadership,
                            shutdown.as_mut(),
                            || self.backend.controller_snapshot(),
                        ).await {
                            Ok(snapshot) => snapshot,
                            Err(LeaderWaitError::Shutdown) => return Ok(LeaderRunResult::Shutdown),
                            Err(LeaderWaitError::LeadershipLost) => return Ok(LeaderRunResult::LeadershipLost),
                            Err(LeaderWaitError::Coordination(error)) => return Err(error.into()),
                        };
                        loop {
                            match self.reconcile_current_snapshot(&snapshot, &mut state, &authority).await {
                                Ok(()) => break,
                                Err(error) if is_leadership_loss(&error) => return Ok(LeaderRunResult::LeadershipLost),
                                Err(error) if error.is_transient() => match wait_for_leader_retry(leadership, shutdown.as_mut()).await {
                                    Ok(()) => {}
                                    Err(LeaderWaitError::Shutdown) => return Ok(LeaderRunResult::Shutdown),
                                    Err(LeaderWaitError::LeadershipLost) => return Ok(LeaderRunResult::LeadershipLost),
                                    Err(LeaderWaitError::Coordination(error)) => return Err(error.into()),
                                },
                                Err(error) => return Err(error),
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(WatchError::Compacted { .. }) => {
                        let (snapshot, replacement_watch) = match retry_coordination(
                            leadership,
                            shutdown.as_mut(),
                            || self.backend.snapshot_and_watch_namespace(),
                        ).await {
                            Ok(value) => value,
                            Err(LeaderWaitError::Shutdown) => return Ok(LeaderRunResult::Shutdown),
                            Err(LeaderWaitError::LeadershipLost) => return Ok(LeaderRunResult::LeadershipLost),
                            Err(LeaderWaitError::Coordination(error)) => return Err(error.into()),
                        };
                        watch = replacement_watch;
                        state.observe_input_revision(snapshot.revision());
                        loop {
                            match self.reconcile_current_snapshot(&snapshot, &mut state, &authority).await {
                                Ok(()) => break,
                                Err(error) if is_leadership_loss(&error) => return Ok(LeaderRunResult::LeadershipLost),
                                Err(error) if error.is_transient() => match wait_for_leader_retry(leadership, shutdown.as_mut()).await {
                                    Ok(()) => {}
                                    Err(LeaderWaitError::Shutdown) => return Ok(LeaderRunResult::Shutdown),
                                    Err(LeaderWaitError::LeadershipLost) => return Ok(LeaderRunResult::LeadershipLost),
                                    Err(LeaderWaitError::Coordination(error)) => return Err(error.into()),
                                },
                                Err(error) => return Err(error),
                            }
                        }
                    }
                    Err(WatchError::Disconnected)
                    | Err(WatchError::Coordination(CoordinationError::Etcd(_))) => {
                        tokio::select! {
                            () = shutdown.as_mut() => return Ok(LeaderRunResult::Shutdown),
                            () = leadership.lost() => return Ok(LeaderRunResult::LeadershipLost),
                            result = watch.resume_until_available() => {
                                result.map_err(ControllerRuntimeError::from)?;
                            }
                        }
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }

    async fn reconcile_current_snapshot(
        &self,
        snapshot: &CoordinationSnapshot,
        state: &mut ControllerState,
        authority: &ControllerAuthority,
    ) -> Result<(), ControllerRuntimeError> {
        state.observe_input_revision(snapshot.authoritative_revision());
        let snapshot = snapshot
            .clone()
            .with_authoritative_revision(state.authoritative_revision);
        // Participants remove completed messages from the shared queue. Read
        // that published value before planning so a deleted DROPPED state is
        // not mistaken for an uncompleted transition on the next cycle.
        state.pending = read_pending(&snapshot)?;
        let mode = state.mode_for(&snapshot);
        self.reconcile(&snapshot, &mut state.pending, mode, authority)
            .await?;
        state.remember_sessions(&snapshot);
        Ok(())
    }

    async fn reconcile(
        &self,
        snapshot: &CoordinationSnapshot,
        pending: &mut Vec<PublishedTransition>,
        mode: ReconcileMode,
        authority: &ControllerAuthority,
    ) -> Result<(), ControllerRuntimeError> {
        let mut snapshot = snapshot.clone();
        loop {
            let (external_view, next_pending) =
                compute_outputs_with_pending_policy(&snapshot, pending, mode)?;
            let external_json = external_view_json(&external_view)?;
            let pending_json = serde_json::to_string(&next_pending)
                .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
            crate::failpoints::controlled_error_or_abort(
                "controller_after_reconcile_before_publish",
            )
            .map_err(ControllerRuntimeError::message)?;
            // Reconciliation is deliberately computed outside the
            // coordination write. Do not publish a result after a participant
            // or metadata mutation has advanced the authoritative input
            // snapshot. Re-read after publication as well: a participant may
            // complete a transition in the small interval between these
            // checks. Recomputing here prevents an old queue from being
            // reintroduced after a completion.
            let current = self.backend.controller_snapshot().await?;
            if current.authoritative_revision() > snapshot.authoritative_revision() {
                snapshot = current;
                *pending = read_pending(&snapshot)?;
                continue;
            }
            let pending_revision = snapshot
                .metadata()
                .get(PENDING_OUTPUT_KEY)
                .map(|entry| entry.revision());
            if outputs_match(&snapshot, &external_json, &pending_json) {
                *pending = next_pending;
                return Ok(());
            }
            crate::failpoints::hard_abort("controller_before_output_txn");
            let live_sessions = snapshot
                .participants()
                .live_instances()
                .iter()
                .map(|(instance, live)| (instance.clone(), live.session_id()))
                .collect();
            let fence = authority.commit_fence(snapshot.authoritative_revision(), live_sessions);
            match self
                .backend
                .publish_controller_outputs_with_fence(
                    &fence,
                    &external_json,
                    &pending_json,
                    pending_revision,
                )
                .await
            {
                Ok(_) => {}
                Err(CoordinationError::StaleRevision) => {
                    emit(
                        self.event_hook.as_ref(),
                        RuntimeEvent::PublicationRejected {
                            reason: CoordinationError::StaleRevision.to_string(),
                        },
                    );
                    snapshot = self.backend.controller_snapshot().await?;
                    *pending = read_pending(&snapshot)?;
                    continue;
                }
                Err(error) => {
                    emit(
                        self.event_hook.as_ref(),
                        RuntimeEvent::PublicationRejected {
                            reason: error.to_string(),
                        },
                    );
                    return Err(error.into());
                }
            }
            crate::failpoints::hard_abort("controller_after_output_txn");
            crate::failpoints::controlled_error_or_abort(
                "controller_after_output_txn_before_post_publish_resnapshot",
            )
            .map_err(ControllerRuntimeError::message)?;
            let current = self.backend.controller_snapshot().await?;
            if current.authoritative_revision() > snapshot.authoritative_revision() {
                snapshot = current;
                *pending = read_pending(&snapshot)?;
                continue;
            }
            *pending = next_pending;
            return Ok(());
        }
    }
}

fn outputs_match(
    snapshot: &CoordinationSnapshot,
    external_view: &str,
    pending_transitions: &str,
) -> bool {
    snapshot
        .metadata()
        .get(EXTERNAL_OUTPUT_KEY)
        .and_then(|entry| entry.value())
        == Some(external_view)
        && snapshot
            .metadata()
            .get(PENDING_OUTPUT_KEY)
            .and_then(|entry| entry.value())
            == Some(pending_transitions)
        && snapshot
            .metadata()
            .get(PROCESSED_OUTPUT_KEY)
            .and_then(|entry| entry.value())
            .and_then(|value| value.parse::<i64>().ok())
            .is_some_and(|processed| processed >= snapshot.authoritative_revision().value())
}

fn is_leadership_loss(error: &ControllerRuntimeError) -> bool {
    error.message == CoordinationError::StaleController.to_string()
        || error.message == CoordinationError::StaleRevision.to_string()
}

enum LeaderWaitError {
    Shutdown,
    LeadershipLost,
    Coordination(CoordinationError),
}

async fn wait_for_leader_retry<F>(
    leadership: &Leadership,
    mut shutdown: Pin<&mut F>,
) -> Result<(), LeaderWaitError>
where
    F: Future<Output = ()> + Send,
{
    tokio::select! {
        () = shutdown.as_mut() => Err(LeaderWaitError::Shutdown),
        () = leadership.lost() => Err(LeaderWaitError::LeadershipLost),
        () = tokio::time::sleep(Duration::from_millis(250)) => Ok(()),
    }
}

async fn retry_coordination<T, S, O, Fut>(
    leadership: &Leadership,
    mut shutdown: Pin<&mut S>,
    mut operation: O,
) -> Result<T, LeaderWaitError>
where
    S: Future<Output = ()> + Send,
    O: FnMut() -> Fut,
    Fut: Future<Output = Result<T, CoordinationError>>,
{
    loop {
        let result = tokio::select! {
            () = shutdown.as_mut() => return Err(LeaderWaitError::Shutdown),
            () = leadership.lost() => return Err(LeaderWaitError::LeadershipLost),
            result = operation() => result,
        };
        match result {
            Ok(value) => return Ok(value),
            Err(CoordinationError::StaleController) => return Err(LeaderWaitError::LeadershipLost),
            Err(error) if error.is_transient() => {
                wait_for_leader_retry(leadership, shutdown.as_mut()).await?;
            }
            Err(error) => return Err(LeaderWaitError::Coordination(error)),
        }
    }
}

#[derive(Debug, Deserialize)]
struct InstanceConfigRecord {
    name: String,
    zone: String,
}

#[derive(Debug, Deserialize)]
struct ResourceRecord {
    name: String,
    state_model: String,
    placement: PlacementRecord,
}

#[derive(Debug, Deserialize)]
struct PlacementRecord {
    kind: String,
    replicas: usize,
    #[serde(default)]
    preference_lists: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    partitions: Vec<String>,
    #[serde(default)]
    topology: Option<CrushTopologyRecord>,
}

#[derive(Debug, Deserialize)]
struct CrushTopologyRecord {
    path: String,
    fault_zone_type: String,
    end_node_type: String,
}

#[derive(Debug, Deserialize)]
struct ThrottleRecord {
    scope: String,
    #[serde(default)]
    resource: Option<String>,
    #[serde(default)]
    instance: Option<String>,
    rebalance_type: String,
    max_in_flight: usize,
}

#[derive(Clone)]
struct ResourcePlan {
    resource: ResourceId,
    ideal: IdealState,
}

fn live_sessions(snapshot: &CoordinationSnapshot) -> BTreeMap<InstanceId, SessionId> {
    snapshot
        .participants()
        .live_instances()
        .iter()
        .map(|(instance, live)| (instance.clone(), live.session_id()))
        .collect()
}

fn session_replaced(
    known_sessions: &BTreeMap<InstanceId, SessionId>,
    current: &CoordinationSnapshot,
) -> bool {
    current
        .participants()
        .live_instances()
        .iter()
        .any(|(instance, live)| {
            known_sessions
                .get(instance)
                .is_some_and(|session| *session != live.session_id())
        })
}

#[cfg(test)]
fn compute_outputs_with_mode(
    snapshot: &CoordinationSnapshot,
    pending: &[PublishedTransition],
    mode: ReconcileMode,
) -> Result<(ExternalView, Vec<PublishedTransition>), ControllerRuntimeError> {
    compute_outputs_with_pending_policy(snapshot, pending, mode)
}

fn compute_outputs_with_pending_policy(
    snapshot: &CoordinationSnapshot,
    pending: &[PublishedTransition],
    mode: ReconcileMode,
) -> Result<(ExternalView, Vec<PublishedTransition>), ControllerRuntimeError> {
    let live: BTreeSet<InstanceId> = snapshot
        .participants()
        .live_instances()
        .keys()
        .cloned()
        .collect();
    let configs = instance_configs(snapshot)?;
    let actual = active_current_state(snapshot, &configs, &live);
    let plans = resource_plans(snapshot, &configs, &live)?;
    let model = leader_standby();
    let original_pending = pending;
    let strict_pending_fencing = matches!(mode, ReconcileMode::SessionReplacement);
    let pending = retain_pending(pending, snapshot, &actual, &plans, strict_pending_fencing);

    let operational_pending = operational_pending_for_plans(&pending, &plans, mode);
    let mut inputs = Vec::with_capacity(plans.len());
    for plan in &plans {
        inputs.push(resource_transition_input(
            plan,
            &actual,
            &live,
            &operational_pending,
            &model,
        )?);
    }

    let throttle_configs = if matches!(mode, ReconcileMode::Startup) {
        Vec::new()
    } else {
        throttles(snapshot)?
    };
    let throttle = compute_intermediate_and_throttle(
        &inputs,
        &live,
        &operational_pending,
        &throttle_configs,
        &model,
    )
    .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;

    let mut next_pending = pending;
    for (ordinal, transition) in throttle.dispatchable_transitions().iter().enumerate() {
        let Some(session) = snapshot
            .participants()
            .live_instances()
            .get(transition.instance())
            .map(|live| live.session_id())
        else {
            continue;
        };
        let message_id = original_pending
            .iter()
            .find(|existing| same_transition(existing, transition, session.wire_value()))
            .map(|existing| existing.message_id.clone())
            .unwrap_or_else(|| format!("m10:{}:{}", snapshot.revision().value(), ordinal));
        let message = PublishedTransition {
            resource: transition.resource().to_string(),
            partition: transition.partition().to_string(),
            instance: transition.instance().to_string(),
            target_session: session.wire_value(),
            from: transition.source_state().to_string(),
            to: transition.target_state().to_string(),
            message_type: String::from("STATE_TRANSITION"),
            message_id,
        };
        if !next_pending
            .iter()
            .any(|existing| same_replica(existing, &message))
        {
            next_pending.push(message);
        }
    }
    next_pending.sort();
    next_pending.dedup();

    Ok((ExternalView::from_current_states(actual), next_pending))
}

// Published messages remain pending until a participant reports the matching
// CurrentState. Only messages for the current session and current placement
// are allowed to reserve capacity in the next planning cycle.
fn operational_pending_for_plans(
    pending: &[PublishedTransition],
    plans: &[ResourcePlan],
    mode: ReconcileMode,
) -> Vec<OperationalPendingTransition> {
    let mut result = Vec::new();
    for message in pending {
        let Some(transition) = to_operational_pending(message) else {
            continue;
        };
        let belongs_to_plan = !matches!(mode, ReconcileMode::SessionReplacement)
            || plans.iter().any(|plan| {
                plan.resource == *transition.resource()
                    && (plan
                        .ideal
                        .preference_list(transition.partition())
                        .is_some_and(|instances| instances.contains(transition.instance()))
                        || pending_partition_is_current_only(plan, transition.partition()))
            });
        if belongs_to_plan {
            result.push(transition);
        }
    }
    result
}

fn resource_transition_input(
    plan: &ResourcePlan,
    actual: &BTreeMap<ResourceId, CurrentState>,
    live: &BTreeSet<InstanceId>,
    operational_pending: &[OperationalPendingTransition],
    model: &crate::model::StateModelDefinition,
) -> Result<ResourceTransitionInput, ControllerRuntimeError> {
    let actual_state = actual.get(&plan.resource).cloned().unwrap_or_default();
    let planning_state = normalize_current(&actual_state, &plan.ideal, live, model);
    let best = if plan.ideal.preference_lists().is_empty() {
        Default::default()
    } else {
        compute_semi_auto_best_possible_state(&plan.ideal, &planning_state, live, model)
            .map_err(|error| ControllerRuntimeError::message(error.to_string()))?
    };
    let candidates = generate_transitions(&plan.resource, &planning_state, &best, model)
        .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
    let pending_for_resource = pending_for_resource(&plan.resource, operational_pending);
    let mut preference_lists = plan.ideal.preference_lists().clone();
    for partition in planning_state.entries().keys() {
        preference_lists.entry(partition.clone()).or_default();
    }
    let selected = select_transitions(
        &plan.resource,
        &plan.ideal,
        &planning_state,
        live,
        &candidates,
        &pending_for_resource,
        model,
    )
    .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
    Ok(ResourceTransitionInput::new(
        plan.resource.clone(),
        plan.ideal.replicas(),
        None,
        preference_lists,
        planning_state,
        best,
        selected,
    ))
}

fn pending_partition_is_current_only(plan: &ResourcePlan, partition: &PartitionId) -> bool {
    plan.ideal.preference_list(partition).is_none()
}

fn pending_for_resource(
    resource: &ResourceId,
    pending: &[OperationalPendingTransition],
) -> Vec<PendingTransition> {
    pending
        .iter()
        .filter(|transition| transition.resource() == resource)
        .map(|transition| {
            PendingTransition::new(
                transition.partition().clone(),
                transition.instance().clone(),
                transition.source_state().clone(),
                transition.target_state().clone(),
            )
        })
        .collect()
}

fn active_current_state(
    snapshot: &CoordinationSnapshot,
    configs: &BTreeMap<InstanceId, String>,
    live: &BTreeSet<InstanceId>,
) -> BTreeMap<ResourceId, CurrentState> {
    let mut states =
        BTreeMap::<ResourceId, BTreeMap<PartitionId, BTreeMap<InstanceId, State>>>::new();
    for active in snapshot.participants().active_current_state().values() {
        for (resource, current) in active.resources() {
            let resource_states = states.entry(resource.clone()).or_default();
            for (partition, replicas) in current.entries() {
                let partition_states = resource_states.entry(partition.clone()).or_default();
                for (replica, state) in replicas {
                    if !configs.contains_key(replica) || !live.contains(replica) {
                        continue;
                    }
                    partition_states.insert(replica.clone(), state.clone());
                }
            }
        }
    }
    states
        .into_iter()
        .map(|(resource, entries)| {
            let mut builder = CurrentState::builder();
            for (partition, replicas) in entries {
                for (instance, state) in replicas {
                    builder
                        .set_state(partition.clone(), instance, state)
                        .expect("active CurrentState has unique replica entries");
                }
            }
            (resource, builder.build())
        })
        .collect()
}

fn instance_configs(
    snapshot: &CoordinationSnapshot,
) -> Result<BTreeMap<InstanceId, String>, ControllerRuntimeError> {
    let Some(value) = metadata_value(snapshot, INSTANCE_CONFIGS_KEY) else {
        return Ok(BTreeMap::new());
    };
    let records: Vec<InstanceConfigRecord> = serde_json::from_str(value)
        .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
    records
        .into_iter()
        .map(|record| {
            Ok((
                InstanceId::new(record.name)
                    .map_err(|error| ControllerRuntimeError::message(error.to_string()))?,
                record.zone,
            ))
        })
        .collect()
}

fn resource_plans(
    snapshot: &CoordinationSnapshot,
    configs: &BTreeMap<InstanceId, String>,
    live: &BTreeSet<InstanceId>,
) -> Result<Vec<ResourcePlan>, ControllerRuntimeError> {
    let mut plans = Vec::new();
    for (key, entry) in snapshot.metadata() {
        let Some(name) = key.strip_prefix(RESOURCE_PREFIX) else {
            continue;
        };
        let record: ResourceRecord = serde_json::from_str(entry.value().unwrap_or_default())
            .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
        if record.name != name {
            return Err(ControllerRuntimeError::message(format!(
                "resource metadata key {name} disagrees with resource name {}",
                record.name
            )));
        }
        if record.state_model != "LeaderStandby" {
            return Err(ControllerRuntimeError::message(format!(
                "unsupported M10 state model {}",
                record.state_model
            )));
        }
        let resource = ResourceId::new(record.name)
            .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
        let preference_lists = match record.placement.kind.as_str() {
            "CRUSH" => {
                let partitions = record
                    .placement
                    .partitions
                    .iter()
                    .map(|value| {
                        PartitionId::new(value.clone())
                            .map_err(|error| ControllerRuntimeError::message(error.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let instances = configs
                    .iter()
                    .map(|(instance, zone)| {
                        crate::rebalance::CrushInstance::new(instance.clone(), zone.clone())
                            .map_err(|error| ControllerRuntimeError::message(error.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let topology = match record.placement.topology {
                    Some(topology) => crate::rebalance::CrushTopology::new(
                        topology.path,
                        topology.fault_zone_type,
                        topology.end_node_type,
                    )
                    .map_err(|error| ControllerRuntimeError::message(error.to_string()))?,
                    None => {
                        crate::rebalance::CrushTopology::new("/instance", "instance", "instance")
                            .map_err(|error| ControllerRuntimeError::message(error.to_string()))?
                    }
                };
                compute_crush_assignment(
                    &resource,
                    &partitions,
                    record.placement.replicas.min(live.len()),
                    &instances,
                    live,
                    &topology,
                )
                .map_err(|error| ControllerRuntimeError::message(error.to_string()))?
            }
            "SEMI_AUTO" => record
                .placement
                .preference_lists
                .into_iter()
                .map(|(partition, instances)| {
                    Ok((
                        PartitionId::new(partition)
                            .map_err(|error| ControllerRuntimeError::message(error.to_string()))?,
                        instances
                            .into_iter()
                            .map(|instance| {
                                InstanceId::new(instance).map_err(|error| {
                                    ControllerRuntimeError::message(error.to_string())
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, ControllerRuntimeError>>()?,
            unknown => {
                return Err(ControllerRuntimeError::message(format!(
                    "unsupported M10 placement kind {unknown}"
                )))
            }
        };
        let effective_replicas = if record.placement.kind == "CRUSH" {
            preference_lists
                .values()
                .map(Vec::len)
                .min()
                .unwrap_or_default()
        } else {
            record.placement.replicas
        };
        let mut builder = IdealState::builder(resource.clone(), effective_replicas);
        for (partition, instances) in preference_lists {
            builder
                .set_preference_list(partition, instances)
                .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
        }
        plans.push(ResourcePlan {
            resource,
            ideal: builder
                .build()
                .map_err(|error| ControllerRuntimeError::message(error.to_string()))?,
        });
    }
    Ok(plans)
}

fn normalize_current(
    actual: &CurrentState,
    ideal: &IdealState,
    live: &BTreeSet<InstanceId>,
    model: &crate::model::StateModelDefinition,
) -> CurrentState {
    let mut builder = CurrentState::builder();
    for (partition, replicas) in actual.entries() {
        for (instance, state) in replicas {
            if !live.contains(instance) {
                continue;
            }
            builder
                .set_state(partition.clone(), instance.clone(), state.clone())
                .expect("actual CurrentState has unique entries");
        }
    }
    for (partition, instances) in ideal.preference_lists() {
        for instance in instances {
            if !live.contains(instance) {
                continue;
            }
            if actual.state(partition, instance).is_none() {
                builder
                    .set_state(
                        partition.clone(),
                        instance.clone(),
                        model.initial_state().clone(),
                    )
                    .expect("planning CurrentState has unique entries");
            }
        }
    }
    builder.build()
}

fn retain_pending(
    pending: &[PublishedTransition],
    snapshot: &CoordinationSnapshot,
    actual: &BTreeMap<ResourceId, CurrentState>,
    plans: &[ResourcePlan],
    strict_pending_fencing: bool,
) -> Vec<PublishedTransition> {
    pending
        .iter()
        .filter(|message| {
            let Ok(instance) = InstanceId::new(message.instance.clone()) else {
                return false;
            };
            let Some(live) = snapshot.participants().live_instances().get(&instance) else {
                return false;
            };
            if live.session_id().wire_value() != message.target_session {
                return false;
            }
            let Ok(resource) = ResourceId::new(message.resource.clone()) else {
                return false;
            };
            let Ok(partition) = PartitionId::new(message.partition.clone()) else {
                return false;
            };
            let Some(plan) = plans
                .iter()
                .find(|plan| plan.resource.as_str() == message.resource)
            else {
                return false;
            };
            let desired_preference = plan.ideal.preference_list(&partition);
            if desired_preference.is_none()
                && !actual
                    .get(&resource)
                    .is_some_and(|state| state.entries().contains_key(&partition))
            {
                return false;
            }
            if strict_pending_fencing {
                let desired_replica =
                    desired_preference.is_some_and(|preference| preference.contains(&instance));
                let removal_state = message.to == "OFFLINE" || message.to == "DROPPED";
                if desired_replica == removal_state {
                    return false;
                }
            }
            match actual
                .get(&resource)
                .and_then(|state| state.state(&partition, &instance))
            {
                Some(state) => state.as_str() == message.from,
                None => true,
            }
        })
        .cloned()
        .collect()
}

fn to_operational_pending(message: &PublishedTransition) -> Option<OperationalPendingTransition> {
    Some(OperationalPendingTransition::new(
        ResourceId::new(message.resource.clone()).ok()?,
        PartitionId::new(message.partition.clone()).ok()?,
        InstanceId::new(message.instance.clone()).ok()?,
        State::try_from(message.from.as_str()).ok()?,
        State::try_from(message.to.as_str()).ok()?,
    ))
}

fn throttles(
    snapshot: &CoordinationSnapshot,
) -> Result<Vec<StateTransitionThrottleConfig>, ControllerRuntimeError> {
    let Some(value) = metadata_value(snapshot, THROTTLES_KEY) else {
        return Ok(Vec::new());
    };
    let records: Vec<ThrottleRecord> = serde_json::from_str(value)
        .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
    records
        .into_iter()
        .map(|record| {
            let scope = match record.scope.as_str() {
                "CLUSTER" => ThrottleScope::Cluster,
                "RESOURCE" => ThrottleScope::Resource,
                "INSTANCE" => ThrottleScope::Instance,
                value => {
                    return Err(ControllerRuntimeError::message(format!(
                        "unknown throttle scope {value}"
                    )))
                }
            };
            let rebalance_type = match record.rebalance_type.as_str() {
                "ANY" => RebalanceType::Any,
                "RECOVERY_BALANCE" => RebalanceType::RecoveryBalance,
                "LOAD_BALANCE" => RebalanceType::LoadBalance,
                value => {
                    return Err(ControllerRuntimeError::message(format!(
                        "unknown rebalance type {value}"
                    )))
                }
            };
            if record.max_in_flight == 0 {
                return Err(ControllerRuntimeError::message(
                    "transition throttle limit must be greater than zero",
                ));
            }
            match scope {
                ThrottleScope::Cluster => {
                    if record.resource.is_some() || record.instance.is_some() {
                        return Err(ControllerRuntimeError::message(
                            "cluster transition throttle cannot target a resource or instance",
                        ));
                    }
                    Ok(StateTransitionThrottleConfig::new(
                        rebalance_type,
                        scope,
                        record.max_in_flight,
                    ))
                }
                ThrottleScope::Resource => {
                    if record.instance.is_some() {
                        return Err(ControllerRuntimeError::message(
                            "resource transition throttle cannot target an instance",
                        ));
                    }
                    match record.resource {
                        Some(resource) => {
                            let resource = ResourceId::new(resource).map_err(|error| {
                                ControllerRuntimeError::message(error.to_string())
                            })?;
                            Ok(StateTransitionThrottleConfig::for_resource(
                                rebalance_type,
                                resource,
                                record.max_in_flight,
                            ))
                        }
                        None => Ok(StateTransitionThrottleConfig::new(
                            rebalance_type,
                            scope,
                            record.max_in_flight,
                        )),
                    }
                }
                ThrottleScope::Instance => {
                    if record.resource.is_some() {
                        return Err(ControllerRuntimeError::message(
                            "instance transition throttle cannot target a resource",
                        ));
                    }
                    match record.instance {
                        Some(instance) => {
                            let instance = InstanceId::new(instance).map_err(|error| {
                                ControllerRuntimeError::message(error.to_string())
                            })?;
                            Ok(StateTransitionThrottleConfig::for_instance(
                                rebalance_type,
                                instance,
                                record.max_in_flight,
                            ))
                        }
                        None => Ok(StateTransitionThrottleConfig::new(
                            rebalance_type,
                            scope,
                            record.max_in_flight,
                        )),
                    }
                }
            }
        })
        .collect()
}

fn read_pending(
    snapshot: &CoordinationSnapshot,
) -> Result<Vec<PublishedTransition>, ControllerRuntimeError> {
    let Some(value) = metadata_value(snapshot, PENDING_OUTPUT_KEY) else {
        return Ok(Vec::new());
    };
    let pending: Vec<PublishedTransition> = serde_json::from_str(value)
        .map_err(|error| ControllerRuntimeError::message(error.to_string()))?;
    if pending
        .iter()
        .any(|transition| transition.message_id.is_empty())
    {
        return Err(ControllerRuntimeError::message(
            "pending transition is missing message identity",
        ));
    }
    Ok(pending)
}

fn metadata_value<'a>(snapshot: &'a CoordinationSnapshot, key: &str) -> Option<&'a str> {
    snapshot.metadata().get(key)?.value()
}

fn same_replica(left: &PublishedTransition, right: &PublishedTransition) -> bool {
    left.resource == right.resource
        && left.partition == right.partition
        && left.instance == right.instance
}

fn same_transition(
    existing: &PublishedTransition,
    transition: &TransitionRequest,
    session: u64,
) -> bool {
    existing.resource == transition.resource().as_str()
        && existing.partition == transition.partition().as_str()
        && existing.instance == transition.instance().as_str()
        && existing.target_session == session
        && existing.from == transition.source_state().as_str()
        && existing.to == transition.target_state().as_str()
        && existing.message_type == "STATE_TRANSITION"
}

fn external_view_json(current: &ExternalView) -> Result<String, ControllerRuntimeError> {
    let mut output = BTreeMap::<String, BTreeMap<String, BTreeMap<String, String>>>::new();
    for (resource, state) in current.entries() {
        let mut partitions = BTreeMap::new();
        for (partition, replicas) in state {
            partitions.insert(
                partition.to_string(),
                replicas
                    .iter()
                    .map(|(instance, state)| (instance.to_string(), state.to_string()))
                    .collect(),
            );
        }
        output.insert(resource.to_string(), partitions);
    }
    serde_json::to_string(&output)
        .map_err(|error| ControllerRuntimeError::message(error.to_string()))
}

fn is_controller_input(key: &str) -> bool {
    if key.starts_with("live/") || key.starts_with("current-state/") {
        return true;
    }
    let Some(encoded) = key.strip_prefix("metadata/") else {
        return false;
    };
    let Ok(decoded) = crate::coordination::etcd::decode_segment(encoded) else {
        return true;
    };
    !decoded.starts_with("controller/output/") && decoded != "internal/session-sequence"
}

#[cfg(test)]
mod tests {
    use super::{
        compute_outputs_with_mode, compute_outputs_with_pending_policy, external_view_json,
        instance_configs, is_controller_input, is_leadership_loss, live_sessions, metadata_value,
        operational_pending_for_plans, read_pending, resource_plans, session_replaced, throttles,
        to_operational_pending, ControllerRuntimeError, ControllerState, PublishedTransition,
        ReconcileMode,
    };
    use crate::coordination::etcd::{CoordinationSnapshot, MetadataEntry, Revision};
    use crate::model::{
        ActiveCurrentState, CurrentState, ExternalView, InstanceId, LiveInstance,
        ParticipantSessionSnapshot, PartitionId, ResourceId, SessionId, State,
    };
    use std::collections::{BTreeMap, BTreeSet};

    fn state(name: &str) -> State {
        State::try_from(name).expect("valid test state")
    }

    fn metadata(entries: &[(&str, &str)]) -> BTreeMap<String, MetadataEntry> {
        entries
            .iter()
            .enumerate()
            .map(|(index, (key, value))| {
                (
                    (*key).to_owned(),
                    MetadataEntry {
                        value: Some((*value).to_owned()),
                        revision: Revision::new(index as i64 + 1).unwrap(),
                    },
                )
            })
            .collect()
    }

    fn snapshot(
        entries: &[(&str, &str)],
        live_names: &[&str],
        resources: BTreeMap<ResourceId, CurrentState>,
    ) -> CoordinationSnapshot {
        snapshot_at(Revision::new(100).unwrap(), entries, live_names, resources)
    }

    fn snapshot_at(
        revision: Revision,
        entries: &[(&str, &str)],
        live_names: &[&str],
        resources: BTreeMap<ResourceId, CurrentState>,
    ) -> CoordinationSnapshot {
        let mut live = BTreeMap::new();
        let mut active = BTreeMap::new();
        for (sequence, name) in live_names.iter().enumerate() {
            let instance = InstanceId::new(*name).unwrap();
            let session = SessionId::from_wire_value(sequence as u64 + 1);
            live.insert(
                instance.clone(),
                LiveInstance::new(instance.clone(), session),
            );
            active.insert(
                instance,
                ActiveCurrentState::new(session, resources.clone()),
            );
        }
        CoordinationSnapshot::from_parts(
            revision,
            metadata(entries),
            ParticipantSessionSnapshot::from_parts(live, active),
        )
    }

    fn current(entries: &[(&str, &str, &str)]) -> CurrentState {
        let mut builder = CurrentState::builder();
        for (partition, instance, state_name) in entries {
            builder
                .set_state(
                    PartitionId::new(*partition).unwrap(),
                    InstanceId::new(*instance).unwrap(),
                    state(state_name),
                )
                .unwrap();
        }
        builder.build()
    }

    fn config() -> (&'static str, &'static str) {
        (
            "controller/instance-configs",
            r#"[{"name":"node-a","zone":"zone-a"},{"name":"node-b","zone":"zone-b"}]"#,
        )
    }

    fn resource() -> (&'static str, &'static str) {
        (
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":2,"preference_lists":{"documents_0":["node-a","node-b"]}}}"#,
        )
    }

    #[test]
    fn output_events_do_not_reconcile() {
        let encoded =
            crate::coordination::etcd::encode_segment("controller/output/processed-revision");
        assert!(!is_controller_input(&format!("metadata/{encoded}")));
        assert!(is_controller_input("live/6e6f64652d61"));
        assert!(is_controller_input("current-state/a/1/r/p"));
        assert!(is_controller_input("metadata/not-a-valid-hex-key"));
        assert!(!is_controller_input("sessions/node-a/1"));
        assert!(is_controller_input("metadata/"));
    }

    #[test]
    fn runtime_helpers_cover_empty_and_malformed_inputs() {
        let empty = snapshot(&[], &[], BTreeMap::new());
        assert!(live_sessions(&empty).is_empty());
        assert!(!session_replaced(&BTreeMap::new(), &empty));
        assert!(metadata_value(&empty, "missing").is_none());
        assert!(instance_configs(&empty).unwrap().is_empty());
        assert!(resource_plans(&empty, &BTreeMap::new(), &BTreeSet::new())
            .unwrap()
            .is_empty());
        assert!(read_pending(&empty).unwrap().is_empty());
        assert!(throttles(&empty).unwrap().is_empty());
        assert!(operational_pending_for_plans(&[], &[], ReconcileMode::Normal).is_empty());
        assert!(external_view_json(&ExternalView::from_current_states(BTreeMap::new())).is_ok());
        assert!(!is_leadership_loss(&ControllerRuntimeError::message(
            "other"
        )));
        assert!(to_operational_pending(&PublishedTransition {
            resource: String::new(),
            partition: String::from("p0"),
            instance: String::from("node-a"),
            target_session: 1,
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
            message_id: String::from("m1"),
        })
        .is_none());
        assert!(read_pending(&snapshot(
            &[("controller/output/pending-transitions", "not-json")],
            &[],
            BTreeMap::new(),
        ))
        .is_err());
        assert!(throttles(&snapshot(
            &[(
                "controller/throttles",
                "[{\"scope\":\"UNKNOWN\",\"rebalance_type\":\"ANY\",\"max_in_flight\":1}]"
            )],
            &[],
            BTreeMap::new(),
        ))
        .is_err());
        let valid_throttles = throttles(&snapshot(
            &[(
                "controller/throttles",
                "[{\"scope\":\"CLUSTER\",\"rebalance_type\":\"ANY\",\"max_in_flight\":1},{\"scope\":\"RESOURCE\",\"rebalance_type\":\"RECOVERY_BALANCE\",\"max_in_flight\":2},{\"scope\":\"INSTANCE\",\"rebalance_type\":\"LOAD_BALANCE\",\"max_in_flight\":3}]",
            )],
            &[],
            BTreeMap::new(),
        ))
        .unwrap();
        assert_eq!(valid_throttles.len(), 3);
    }

    #[test]
    fn runtime_builds_external_view_and_retains_pending_work() {
        let (config_key, config_value) = config();
        let (resource_key, resource_value) = resource();
        let actual = current(&[
            ("documents_0", "node-a", "STANDBY"),
            ("documents_0", "node-b", "STANDBY"),
        ]);
        let pending_snapshot = snapshot(
            &[
                (config_key, config_value),
                (resource_key, resource_value),
                (
                    "controller/throttles",
                    "[{\"scope\":\"CLUSTER\",\"rebalance_type\":\"ANY\",\"max_in_flight\":1}]",
                ),
            ],
            &["node-a", "node-b"],
            BTreeMap::from([(ResourceId::new("documents").unwrap(), actual)]),
        );
        let pending = vec![PublishedTransition {
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 1,
            from: String::from("STANDBY"),
            to: String::from("LEADER"),
            message_type: String::from("STATE_TRANSITION"),
            message_id: String::from("pending-1"),
        }];
        let (external, retained) =
            compute_outputs_with_mode(&pending_snapshot, &pending, ReconcileMode::Normal).unwrap();
        assert_eq!(retained, pending);
        assert_eq!(external.entries().len(), 1);
        assert_eq!(
            external
                .entries()
                .get(&ResourceId::new("documents").unwrap())
                .unwrap()
                .get(&PartitionId::new("documents_0").unwrap())
                .unwrap()
                .get(&InstanceId::new("node-a").unwrap())
                .unwrap()
                .as_str(),
            "STANDBY"
        );
    }

    #[test]
    fn session_replacement_does_not_discard_unrelated_pending_work() {
        let (config_key, config_value) = config();
        let (resource_key, resource_value) = resource();
        let pending_snapshot = snapshot(
            &[
                (config_key, config_value),
                (resource_key, resource_value),
                (
                    "controller/throttles",
                    "[{\"scope\":\"CLUSTER\",\"rebalance_type\":\"ANY\",\"max_in_flight\":1}]",
                ),
            ],
            &["node-a", "node-b"],
            BTreeMap::from([(
                ResourceId::new("documents").unwrap(),
                current(&[
                    ("documents_0", "node-a", "STANDBY"),
                    ("documents_0", "node-b", "STANDBY"),
                ]),
            )]),
        );
        let pending = vec![
            PublishedTransition {
                resource: String::from("documents"),
                partition: String::from("documents_0"),
                instance: String::from("node-a"),
                target_session: 99,
                from: String::from("STANDBY"),
                to: String::from("LEADER"),
                message_type: String::from("STATE_TRANSITION"),
                message_id: String::from("obsolete-session"),
            },
            PublishedTransition {
                resource: String::from("documents"),
                partition: String::from("documents_0"),
                instance: String::from("node-b"),
                target_session: 2,
                from: String::from("STANDBY"),
                to: String::from("LEADER"),
                message_type: String::from("STATE_TRANSITION"),
                message_id: String::from("unrelated-pending"),
            },
        ];

        let (_, retained) =
            compute_outputs_with_mode(&pending_snapshot, &pending, ReconcileMode::Normal).unwrap();
        assert_eq!(
            retained,
            vec![pending
                .into_iter()
                .find(|message| message.message_id == "unrelated-pending")
                .unwrap()]
        );
    }

    #[test]
    fn retained_pending_work_reserves_replica_across_rebalance() {
        let (config_key, config_value) = config();
        let actual = current(&[
            ("documents_0", "node-a", "STANDBY"),
            ("documents_0", "node-b", "STANDBY"),
        ]);
        let resource_value = r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":2,"preference_lists":{"documents_0":["node-b","node-a"]}}}"#;
        let pending_snapshot = snapshot(
            &[
                (config_key, config_value),
                ("controller/resources/documents", resource_value),
                ("controller/throttles", "[]"),
            ],
            &["node-a", "node-b"],
            BTreeMap::from([(ResourceId::new("documents").unwrap(), actual)]),
        );
        let pending = vec![PublishedTransition {
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 1,
            from: String::from("STANDBY"),
            to: String::from("LEADER"),
            message_type: String::from("STATE_TRANSITION"),
            message_id: String::from("pending-1"),
        }];

        let (_, next_pending) =
            compute_outputs_with_mode(&pending_snapshot, &pending, ReconcileMode::Normal).unwrap();
        assert_eq!(next_pending, pending);
    }

    #[test]
    fn removed_crush_replicas_do_not_block_new_preference_members() {
        let pending_snapshot = snapshot(
            &[
                (
                    "controller/instance-configs",
                    r#"[{"name":"node-a","zone":"zone-a"},{"name":"node-b","zone":"zone-b"},{"name":"node-c","zone":"zone-c"}]"#,
                ),
                (
                    "controller/resources/documents",
                    r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"CRUSH","replicas":2,"partitions":["documents_0","documents_1","documents_2","documents_3"]}}"#,
                ),
                ("controller/throttles", "[]"),
            ],
            &["node-a", "node-b", "node-c"],
            BTreeMap::new(),
        );
        let pending = ["documents_0", "documents_1", "documents_2", "documents_3"]
            .into_iter()
            .flat_map(|partition| {
                [("node-a", 1_u64), ("node-b", 2_u64)].into_iter().map(
                    move |(instance, target_session)| PublishedTransition {
                        resource: String::from("documents"),
                        partition: String::from(partition),
                        instance: String::from(instance),
                        target_session,
                        from: String::from("OFFLINE"),
                        to: String::from("STANDBY"),
                        message_type: String::from("STATE_TRANSITION"),
                        message_id: format!("pending-{partition}-{instance}"),
                    },
                )
            })
            .collect::<Vec<_>>();

        let (_, next_pending) =
            compute_outputs_with_mode(&pending_snapshot, &pending, ReconcileMode::Normal).unwrap();
        assert!(next_pending.iter().any(|message| {
            message.instance == "node-c"
                && ["documents_0", "documents_1", "documents_2"]
                    .contains(&message.partition.as_str())
        }));
        assert!(!next_pending
            .iter()
            .any(|message| { message.instance == "node-c" && message.partition == "documents_3" }));
    }

    #[test]
    fn replacement_session_bootstraps_missing_replica() {
        let (config_key, config_value) = config();
        let (resource_key, resource_value) = resource();
        let pending_snapshot = snapshot(
            &[(config_key, config_value), (resource_key, resource_value)],
            &["node-a", "node-b"],
            BTreeMap::from([(
                ResourceId::new("documents").unwrap(),
                current(&[
                    ("documents_0", "node-a", "OFFLINE"),
                    ("documents_0", "node-b", "STANDBY"),
                ]),
            )]),
        );

        let (_, next_pending) =
            compute_outputs_with_mode(&pending_snapshot, &[], ReconcileMode::Normal).unwrap();
        assert!(next_pending.iter().any(|message| {
            message.instance == "node-a" && message.from == "OFFLINE" && message.to == "STANDBY"
        }));
    }

    #[test]
    fn newly_dispatched_transition_ids_are_unique_and_attempt_scoped() {
        let (config_key, config_value) = config();
        let (resource_key, resource_value) = resource();
        let resources = BTreeMap::from([(
            ResourceId::new("documents").unwrap(),
            current(&[
                ("documents_0", "node-a", "OFFLINE"),
                ("documents_0", "node-b", "STANDBY"),
            ]),
        )]);
        let entries = &[(config_key, config_value), (resource_key, resource_value)];
        let first_snapshot = snapshot_at(
            Revision::new(100).unwrap(),
            entries,
            &["node-a", "node-b"],
            resources.clone(),
        );
        let (_, first_pending) =
            compute_outputs_with_mode(&first_snapshot, &[], ReconcileMode::Normal).unwrap();
        assert_eq!(first_pending.len(), 2);
        assert_eq!(
            first_pending
                .iter()
                .map(|message| message.message_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["m10:100:0", "m10:100:1"])
        );

        let (_, retained) =
            compute_outputs_with_mode(&first_snapshot, &first_pending, ReconcileMode::Normal)
                .unwrap();
        assert_eq!(retained, first_pending);

        let second_snapshot = snapshot_at(
            Revision::new(101).unwrap(),
            entries,
            &["node-a", "node-b"],
            resources,
        );
        let (_, second_pending) =
            compute_outputs_with_mode(&second_snapshot, &[], ReconcileMode::Normal).unwrap();
        assert_eq!(second_pending.len(), 2);
        assert_eq!(
            second_pending
                .iter()
                .map(|message| message.message_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["m10:101:0", "m10:101:1"])
        );
        assert!(first_pending.iter().all(|first| second_pending
            .iter()
            .all(|second| first.message_id != second.message_id)));

        let (_, failover_pending) =
            compute_outputs_with_mode(&second_snapshot, &first_pending, ReconcileMode::Startup)
                .unwrap();
        assert_eq!(failover_pending, first_pending);
    }

    #[test]
    fn obsolete_removal_work_is_replaced_when_placement_returns() {
        let pending_snapshot = snapshot(
            &[
                (
                    "controller/instance-configs",
                    r#"[{"name":"node-a","zone":"zone-a"},{"name":"node-c","zone":"zone-c"}]"#,
                ),
                (
                    "controller/resources/documents",
                    r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":2,"preference_lists":{"documents_0":["node-c","node-a"]}}}"#,
                ),
            ],
            &["node-a", "node-c"],
            BTreeMap::from([(
                ResourceId::new("documents").unwrap(),
                current(&[
                    ("documents_0", "node-a", "OFFLINE"),
                    ("documents_0", "node-c", "LEADER"),
                ]),
            )]),
        );
        let obsolete = PublishedTransition {
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 1,
            from: String::from("OFFLINE"),
            to: String::from("DROPPED"),
            message_type: String::from("STATE_TRANSITION"),
            message_id: String::from("obsolete-1"),
        };

        let (_, next_pending) = compute_outputs_with_pending_policy(
            &pending_snapshot,
            &[obsolete],
            ReconcileMode::SessionReplacement,
        )
        .unwrap();
        assert_eq!(next_pending.len(), 1);
        assert_eq!(next_pending[0].instance, "node-a");
        assert_eq!(next_pending[0].from, "OFFLINE");
        assert_eq!(next_pending[0].to, "STANDBY");
    }

    #[test]
    fn runtime_handles_empty_and_crush_plans() {
        let empty = snapshot(&[], &[], BTreeMap::new());
        let (external, pending) =
            compute_outputs_with_mode(&empty, &[], ReconcileMode::Normal).unwrap();
        assert!(external.entries().is_empty());
        assert!(pending.is_empty());

        let (config_key, config_value) = config();
        let crush = r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"CRUSH","replicas":1,"partitions":["documents_0"]}}"#;
        let pending_snapshot = snapshot(
            &[
                (config_key, config_value),
                ("controller/resources/documents", crush),
            ],
            &["node-a"],
            BTreeMap::new(),
        );
        let (_, pending) =
            compute_outputs_with_mode(&pending_snapshot, &[], ReconcileMode::Normal).unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn runtime_applies_topology_aware_crush_policy() {
        let snapshot = snapshot(
            &[
                (
                    "controller/instance-configs",
                    r#"[{"name":"node-a","zone":"zone-a"},{"name":"node-b","zone":"zone-b"},{"name":"node-c","zone":"zone-a"},{"name":"node-d","zone":"zone-b"}]"#,
                ),
                (
                    "controller/resources/objects",
                    r#"{"name":"objects","state_model":"LeaderStandby","placement":{"kind":"CRUSH","replicas":2,"partitions":["objects_0"],"topology":{"path":"/zone/instance","fault_zone_type":"zone","end_node_type":"instance"}}}"#,
                ),
            ],
            &["node-a", "node-b", "node-c", "node-d"],
            BTreeMap::new(),
        );
        let configs = super::instance_configs(&snapshot).unwrap();
        let plans = resource_plans(
            &snapshot,
            &configs,
            &snapshot
                .participants()
                .live_instances()
                .keys()
                .cloned()
                .collect(),
        )
        .unwrap();
        let preference = plans[0]
            .ideal
            .preference_list(&PartitionId::new("objects_0").unwrap())
            .unwrap();
        assert_eq!(preference.len(), 2);
        assert_ne!(
            configs.get(&preference[0]).unwrap(),
            configs.get(&preference[1]).unwrap()
        );
    }

    #[test]
    fn runtime_rejects_malformed_controller_metadata() {
        let cases = [
            (
                vec![("controller/instance-configs", "not-json")],
                "expected instance config error",
            ),
            (
                vec![("controller/resources/wrong", r#"{"name":"other"}"#)],
                "expected resource name error",
            ),
            (
                vec![(
                    "controller/resources/documents",
                    r#"{"name":"documents","state_model":"Other","placement":{"kind":"SEMI_AUTO","replicas":1}}"#,
                )],
                "expected state model error",
            ),
            (
                vec![(
                    "controller/resources/documents",
                    r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"TYPO","replicas":1}}"#,
                )],
                "expected placement kind error",
            ),
            (
                vec![(
                    "controller/throttles",
                    r#"[{"scope":"BAD","rebalance_type":"ANY","max_in_flight":1}]"#,
                )],
                "expected throttle scope error",
            ),
            (
                vec![(
                    "controller/throttles",
                    r#"[{"scope":"CLUSTER","rebalance_type":"BAD","max_in_flight":1}]"#,
                )],
                "expected throttle type error",
            ),
        ];
        for (entries, message) in cases {
            assert!(
                compute_outputs_with_mode(
                    &snapshot(&entries, &[], BTreeMap::new()),
                    &[],
                    ReconcileMode::Normal,
                )
                .is_err(),
                "{message}"
            );
        }
    }

    #[test]
    fn controller_input_watermark_is_monotonic_across_deletions() {
        let initial = snapshot_at(Revision::new(100).unwrap(), &[], &[], BTreeMap::new());
        let mut state = ControllerState::from_snapshot(&initial).unwrap();
        state.observe_input_revision(Revision::new(105).unwrap());
        state.observe_input_revision(Revision::new(103).unwrap());

        let later = snapshot_at(Revision::new(106).unwrap(), &[], &[], BTreeMap::new())
            .with_authoritative_revision(state.authoritative_revision);
        assert_eq!(later.authoritative_revision(), Revision::new(105).unwrap());
    }

    #[test]
    fn runtime_drops_stale_and_invalid_pending_messages() {
        let (config_key, config_value) = config();
        let (resource_key, resource_value) = resource();
        let pending_snapshot = snapshot(
            &[(config_key, config_value), (resource_key, resource_value)],
            &["node-a"],
            BTreeMap::from([(
                ResourceId::new("documents").unwrap(),
                current(&[("documents_0", "node-a", "STANDBY")]),
            )]),
        );
        let pending = vec![
            PublishedTransition {
                resource: String::from("documents"),
                partition: String::from("documents_0"),
                instance: String::from("node-a"),
                target_session: 99,
                from: String::from("STANDBY"),
                to: String::from("LEADER"),
                message_type: String::from("STATE_TRANSITION"),
                message_id: String::from("stale-session"),
            },
            PublishedTransition {
                resource: String::from("documents"),
                partition: String::from("documents_0"),
                instance: String::from("node-a"),
                target_session: 1,
                from: String::from("OFFLINE"),
                to: String::from("STANDBY"),
                message_type: String::from("STATE_TRANSITION"),
                message_id: String::from("invalid-source"),
            },
            PublishedTransition {
                resource: String::from("documents"),
                partition: String::from("documents_0"),
                instance: String::from("node-a"),
                target_session: 1,
                from: String::from("STANDBY"),
                to: String::from("LEADER"),
                message_type: String::from("STATE_TRANSITION"),
                message_id: String::from("pending-1"),
            },
        ];
        let (_, retained) =
            compute_outputs_with_mode(&pending_snapshot, &pending, ReconcileMode::Normal).unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].to, "LEADER");
    }

    #[test]
    fn runtime_decodes_pending_metadata_and_serializes_external_view() {
        let pending = PublishedTransition {
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 1,
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
            message_id: String::from("pending-1"),
        };
        let pending_snapshot = snapshot(
            &[(
                "controller/output/pending-transitions",
                &serde_json::to_string(&vec![pending.clone()]).unwrap(),
            )],
            &[],
            BTreeMap::new(),
        );
        assert_eq!(read_pending(&pending_snapshot).unwrap(), vec![pending]);
        assert!(read_pending(&snapshot(
            &[("controller/output/pending-transitions", "bad")],
            &[],
            BTreeMap::new(),
        ))
        .is_err());
        assert!(read_pending(&snapshot(
            &[(
                "controller/output/pending-transitions",
                r#"[{"resource":"documents","partition":"documents_0","instance":"node-a","target_session":1,"from":"OFFLINE","to":"STANDBY","message_type":"STATE_TRANSITION","message_id":""}]"#,
            )],
            &[],
            BTreeMap::new(),
        ))
        .is_err());
        assert_eq!(external_view_json(&ExternalView::default()).unwrap(), "{}");
    }

    #[test]
    fn coordination_error_strings_cover_operator_facing_failures() {
        use crate::coordination::etcd::CoordinationError;
        let errors = [
            CoordinationError::InvalidPrefix,
            CoordinationError::InvalidKey,
            CoordinationError::InvalidValue,
            CoordinationError::InvalidLeaseTtl,
            CoordinationError::LeaseExpired,
            CoordinationError::InvalidRevision(0),
            CoordinationError::RevisionExhausted,
            CoordinationError::MissingRevision,
            CoordinationError::OutsidePrefix,
            CoordinationError::InvalidSession,
            CoordinationError::UnknownSession(SessionId::from_wire_value(1)),
            CoordinationError::RegistrationLost,
            CoordinationError::Contention,
            CoordinationError::StaleSession(InstanceId::new("node-a").unwrap()),
        ];
        for error in errors {
            assert!(!error.to_string().is_empty());
        }
    }

    #[test]
    fn published_transition_order_is_semantic_and_deterministic() {
        let mut transitions = BTreeSet::from([
            PublishedTransition {
                resource: String::from("b"),
                partition: String::from("p"),
                instance: String::from("i"),
                target_session: 1,
                from: String::from("OFFLINE"),
                to: String::from("STANDBY"),
                message_type: String::from("STATE_TRANSITION"),
                message_id: String::from("b-message"),
            },
            PublishedTransition {
                resource: String::from("a"),
                partition: String::from("p"),
                instance: String::from("i"),
                target_session: 1,
                from: String::from("OFFLINE"),
                to: String::from("STANDBY"),
                message_type: String::from("STATE_TRANSITION"),
                message_id: String::from("a-message"),
            },
        ]);
        assert_eq!(transitions.pop_first().unwrap().resource, "a");
    }
}
