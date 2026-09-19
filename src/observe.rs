//! Read-only semantic observations assembled from coordination state.

use crate::controller::PublishedTransition;
use crate::coordination::etcd::{
    is_lease_backed_delete, CoordinationError, CoordinationSnapshot, EtcdCoordination, Revision,
    WatchSubscription,
};
use crate::model::{InstanceId, PartitionId, ResourceId, State};
use crate::observability::{emit, RuntimeEvent, RuntimeEventHook};
use crate::routing::RoutingSnapshot;
use serde::{ser::Serializer, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const INSTANCE_CONFIGS_KEY: &str = "controller/instance-configs";
const EXTERNAL_VIEW_KEY: &str = "controller/output/external-view";
const PENDING_KEY: &str = "controller/output/pending-transitions";

/// Controller membership observed from lease-backed election records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ControllerMembership {
    pub active: Vec<String>,
    pub standby: Vec<String>,
}

impl ControllerMembership {
    /// Return the active controller, if one is elected.
    pub fn active(&self) -> Option<&str> {
        self.active.first().map(String::as_str)
    }

    /// Return standby controller candidates.
    pub fn standby(&self) -> &[String] {
        &self.standby
    }
}

/// Read-only view over live participant membership.
pub struct InstanceMembership<'a> {
    live_instances: &'a BTreeMap<String, u64>,
}

impl InstanceMembership<'_> {
    /// Return the number of configured live instances in this observation.
    pub fn len(&self) -> usize {
        self.live_instances.len()
    }

    /// Return whether no configured instances are live in this observation.
    pub fn is_empty(&self) -> bool {
        self.live_instances.is_empty()
    }

    /// Return whether an instance currently has a live session.
    pub fn is_live(&self, instance: impl AsRef<str>) -> bool {
        self.live_instances.contains_key(instance.as_ref())
    }
}

/// CurrentState records belonging to a live participant session.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ActiveCurrentStateObservation {
    pub session: u64,
    pub resources: BTreeMap<String, BTreeMap<String, String>>,
}

/// One routing result derived from the observed ExternalView.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RoutingResult {
    pub resource: String,
    pub partition: String,
    pub state: String,
    pub instances: Vec<String>,
}

/// Errors returned while waiting for an observed cluster condition.
#[derive(Debug)]
pub enum WaitError {
    Coordination(CoordinationError),
    Watch(crate::coordination::etcd::WatchError),
    Timeout(Duration),
}

impl std::fmt::Display for WaitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Coordination(error) => error.fmt(formatter),
            Self::Watch(error) => error.fmt(formatter),
            Self::Timeout(duration) => {
                write!(formatter, "observation timed out after {duration:?}")
            }
        }
    }
}

impl std::error::Error for WaitError {}

impl From<CoordinationError> for WaitError {
    fn from(error: CoordinationError) -> Self {
        Self::Coordination(error)
    }
}

impl From<crate::coordination::etcd::WatchError> for WaitError {
    fn from(error: crate::coordination::etcd::WatchError) -> Self {
        Self::Watch(error)
    }
}

/// The etcd MVCC revision represented by one observer observation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ObserverRevision(i64);

impl ObserverRevision {
    fn from_revision(revision: Revision) -> Self {
        Self(revision.value())
    }

    /// Construct an observation revision for fixtures and persisted evidence.
    pub const fn from_value(value: i64) -> Self {
        Self(value)
    }

    /// Return the etcd MVCC revision.
    pub const fn value(self) -> i64 {
        self.0
    }
}

/// The semantic cluster state exposed to operators and verification tools.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ClusterSnapshot {
    pub observer_revision: ObserverRevision,
    pub authoritative_revision: ObserverRevision,
    pub processed_revision: Option<ObserverRevision>,
    pub controllers: ControllerMembership,
    pub live_instances: BTreeMap<String, u64>,
    pub active_current_state: BTreeMap<String, ActiveCurrentStateObservation>,
    pub external_view: Value,
    pub pending_transitions: Vec<PublishedTransition>,
    pub routing_results: Vec<RoutingResult>,
}

impl ClusterSnapshot {
    fn with_authoritative_floor(mut self, floor: ObserverRevision) -> Self {
        self.authoritative_revision = self.authoritative_revision.max(floor);
        self
    }

    /// Parse and return the typed routing view without hiding malformed data.
    pub fn try_routing(&self) -> Result<RoutingSnapshot, CoordinationError> {
        let configured_instances = self
            .routing_results
            .iter()
            .flat_map(|result| result.instances.iter())
            .map(|instance| {
                InstanceId::new(instance.clone()).map_err(|_| CoordinationError::InvalidValue)
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        let external = external_view_model(&self.external_view)?;
        Ok(RoutingSnapshot::from_external_view(
            external,
            configured_instances,
        ))
    }

    /// Return the typed routing view derived from this snapshot.
    pub fn routing(&self) -> RoutingSnapshot {
        self.try_routing()
            .expect("ClusterSnapshot contains an invalid ExternalView")
    }

    /// Return typed controller membership.
    pub fn controllers(&self) -> &ControllerMembership {
        &self.controllers
    }

    /// Return a typed live-instance view.
    pub fn instances(&self) -> InstanceMembership<'_> {
        InstanceMembership {
            live_instances: &self.live_instances,
        }
    }

    /// Return published transition work awaiting completion.
    pub fn pending_transitions(&self) -> &[PublishedTransition] {
        &self.pending_transitions
    }
}

/// Typed application-facing view of one observed cluster snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    raw: ClusterSnapshot,
    routing: RoutingSnapshot,
}

impl Snapshot {
    fn with_authoritative_floor(mut self, floor: ObserverRevision) -> Self {
        self.raw = self.raw.with_authoritative_floor(floor);
        self
    }

    fn from_raw(raw: ClusterSnapshot) -> Result<Self, CoordinationError> {
        let routing = raw.try_routing()?;
        Ok(Self { raw, routing })
    }

    /// Return the immutable typed routing view constructed for this snapshot.
    pub fn routing(&self) -> &RoutingSnapshot {
        &self.routing
    }

    /// Return typed controller membership.
    pub fn controllers(&self) -> &ControllerMembership {
        &self.raw.controllers
    }

    /// Return a typed live-instance view.
    pub fn instances(&self) -> InstanceMembership<'_> {
        InstanceMembership {
            live_instances: &self.raw.live_instances,
        }
    }

    /// Return published transition work awaiting completion.
    pub fn pending_transitions(&self) -> &[PublishedTransition] {
        &self.raw.pending_transitions
    }

    /// Return the etcd revision represented by this snapshot.
    pub const fn observer_revision(&self) -> ObserverRevision {
        self.raw.observer_revision
    }

    /// Return the newest authoritative input revision represented here.
    pub const fn authoritative_revision(&self) -> ObserverRevision {
        self.raw.authoritative_revision
    }

    /// Return the controller's processed-input watermark, if one has been published.
    pub const fn processed_revision(&self) -> Option<ObserverRevision> {
        self.raw.processed_revision
    }
}

impl Serialize for Snapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.raw.serialize(serializer)
    }
}

/// Read-only access to the currently published cluster state.
#[derive(Clone)]
pub struct ClusterObserver {
    coordination: EtcdCoordination,
    event_hook: Option<RuntimeEventHook>,
}

/// Application-facing observer that exposes typed, precomputed snapshots.
#[derive(Clone)]
pub struct Observer {
    inner: ClusterObserver,
}

impl Observer {
    /// Create an observer for one coordination namespace.
    pub fn new(coordination: EtcdCoordination) -> Self {
        Self {
            inner: ClusterObserver::new(coordination),
        }
    }

    /// Register a callback for observer coordination recovery events.
    pub fn on_event<F>(mut self, callback: F) -> Self
    where
        F: Fn(RuntimeEvent) + Send + Sync + 'static,
    {
        self.inner = self.inner.on_event(callback);
        self
    }

    /// Read a typed semantic observation from shared coordination state.
    pub async fn snapshot(&self) -> Result<Snapshot, CoordinationError> {
        Snapshot::from_raw(self.inner.snapshot().await?)
    }

    /// Take a typed observation at an exact etcd MVCC revision.
    pub async fn snapshot_at_revision(
        &self,
        revision: Revision,
    ) -> Result<Snapshot, CoordinationError> {
        Snapshot::from_raw(self.inner.snapshot_at_revision(revision).await?)
    }

    /// Wait for a typed snapshot satisfying a predicate.
    pub async fn wait_until<F>(
        &self,
        timeout: Duration,
        mut predicate: F,
    ) -> Result<Snapshot, WaitError>
    where
        F: FnMut(&Snapshot) -> bool,
    {
        let wait = async {
            let (raw, mut watch) = self
                .inner
                .snapshot_and_watch_namespace_from_authoritative()
                .await?;
            let mut snapshot = Snapshot::from_raw(raw)?;
            let mut authoritative_floor = snapshot.authoritative_revision();
            let catch_up_revision = snapshot.observer_revision();
            let mut caught_up = authoritative_floor >= catch_up_revision;
            if caught_up && predicate(&snapshot) {
                return Ok(snapshot);
            }
            let mut poll = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {
                    result = watch.next() => match result {
                    Ok(event) => {
                        caught_up |= ObserverRevision::from_revision(event.revision()) >= catch_up_revision;
                        if is_lease_backed_delete(&event) {
                            authoritative_floor = authoritative_floor.max(ObserverRevision::from_revision(event.revision()));
                        }
                        snapshot = Snapshot::from_raw(self.inner.snapshot().await?)?.with_authoritative_floor(authoritative_floor);
                        if caught_up && predicate(&snapshot) {
                            return Ok(snapshot);
                        }
                    }
                    Err(crate::coordination::etcd::WatchError::Compacted { .. }) => {
                        let (replacement, replacement_watch) =
                            self.inner.snapshot_and_watch_namespace().await?;
                        snapshot = Snapshot::from_raw(replacement)?.with_authoritative_floor(authoritative_floor);
                        watch = replacement_watch;
                        authoritative_floor = authoritative_floor.max(snapshot.authoritative_revision());
                        caught_up = true;
                        if predicate(&snapshot) {
                            return Ok(snapshot);
                        }
                    }
                    Err(crate::coordination::etcd::WatchError::Disconnected) => {
                        emit(
                            self.inner.event_hook.as_ref(),
                            RuntimeEvent::CoordinationRetry {
                                operation: String::from("observer watch"),
                            },
                        );
                        watch.resume_until_available().await?;
                    }
                    Err(crate::coordination::etcd::WatchError::Coordination(
                        CoordinationError::Etcd(_),
                    )) => {
                        emit(
                            self.inner.event_hook.as_ref(),
                            RuntimeEvent::CoordinationRetry {
                                operation: String::from("observer coordination"),
                            },
                        );
                        watch.resume_until_available().await?;
                    }
                    Err(error) => return Err(WaitError::Watch(error)),
                    },
                    _ = poll.tick() => {
                        match self.inner.snapshot().await {
                            Ok(raw) => {
                                snapshot = Snapshot::from_raw(raw)?.with_authoritative_floor(authoritative_floor);
                                if caught_up && predicate(&snapshot) {
                                    return Ok(snapshot);
                                }
                            }
                            Err(CoordinationError::Etcd(_)) => {}
                            Err(error) => return Err(WaitError::Coordination(error)),
                        }
                    }
                }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| WaitError::Timeout(timeout))?
    }

    /// Wait until a controller is elected.
    pub async fn wait_for_controller(&self, timeout: Duration) -> Result<Snapshot, WaitError> {
        self.wait_until(timeout, |snapshot| {
            snapshot.controllers().active().is_some()
        })
        .await
    }

    /// Wait until no transitions remain and participant progress has caught up.
    pub async fn wait_for_idle(&self, timeout: Duration) -> Result<Snapshot, WaitError> {
        self.wait_until(timeout, |snapshot| {
            snapshot.pending_transitions().is_empty()
                && snapshot
                    .processed_revision()
                    .is_some_and(|revision| revision >= snapshot.authoritative_revision())
        })
        .await
    }
}

impl ClusterObserver {
    /// Create an observer for one coordination namespace.
    pub fn new(coordination: EtcdCoordination) -> Self {
        Self {
            coordination,
            event_hook: None,
        }
    }

    /// Register a callback for observer coordination recovery events.
    pub fn on_event<F>(mut self, callback: F) -> Self
    where
        F: Fn(RuntimeEvent) + Send + Sync + 'static,
    {
        self.event_hook = Some(std::sync::Arc::new(callback));
        self
    }

    /// Read a consistent semantic observation from shared coordination state.
    pub async fn snapshot(&self) -> Result<ClusterSnapshot, CoordinationError> {
        let coordination_snapshot = self.coordination.controller_snapshot().await?;
        Self::snapshot_from_coordination(&coordination_snapshot)
    }

    /// Take a semantic snapshot at an exact etcd MVCC revision.
    pub async fn snapshot_at_revision(
        &self,
        revision: Revision,
    ) -> Result<ClusterSnapshot, CoordinationError> {
        let coordination_snapshot = self
            .coordination
            .controller_snapshot_at_revision(revision)
            .await?;
        Self::snapshot_from_coordination(&coordination_snapshot)
    }

    /// Seed a gap-free semantic observation and namespace watch.
    pub async fn snapshot_and_watch_namespace(
        &self,
    ) -> Result<(ClusterSnapshot, WatchSubscription), CoordinationError> {
        let (coordination_snapshot, watch) =
            self.coordination.snapshot_and_watch_namespace().await?;
        Ok((
            Self::snapshot_from_coordination(&coordination_snapshot)?,
            watch,
        ))
    }

    async fn snapshot_and_watch_namespace_from_authoritative(
        &self,
    ) -> Result<(ClusterSnapshot, WatchSubscription), CoordinationError> {
        let (coordination_snapshot, watch) = self
            .coordination
            .snapshot_and_watch_namespace_from_authoritative()
            .await?;
        Ok((
            Self::snapshot_from_coordination(&coordination_snapshot)?,
            watch,
        ))
    }

    /// Wait for a predicate using a gap-free namespace watch.
    pub async fn wait_until<F>(
        &self,
        timeout: Duration,
        mut predicate: F,
    ) -> Result<ClusterSnapshot, WaitError>
    where
        F: FnMut(&ClusterSnapshot) -> bool,
    {
        let wait = async {
            let (mut snapshot, mut watch) = self
                .snapshot_and_watch_namespace_from_authoritative()
                .await?;
            let mut authoritative_floor = snapshot.authoritative_revision;
            let catch_up_revision = snapshot.observer_revision;
            let mut caught_up = authoritative_floor >= catch_up_revision;
            if caught_up && predicate(&snapshot) {
                return Ok(snapshot);
            }
            let mut poll = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {
                    result = watch.next() => match result {
                    Ok(event) => {
                        caught_up |= ObserverRevision::from_revision(event.revision()) >= catch_up_revision;
                        if is_lease_backed_delete(&event) {
                            authoritative_floor = authoritative_floor.max(ObserverRevision::from_revision(event.revision()));
                        }
                        snapshot = self.snapshot().await?.with_authoritative_floor(authoritative_floor);
                        if caught_up && predicate(&snapshot) {
                            return Ok(snapshot);
                        }
                    }
                    Err(crate::coordination::etcd::WatchError::Compacted { .. }) => {
                        (snapshot, watch) = self.snapshot_and_watch_namespace().await?;
                        snapshot = snapshot.with_authoritative_floor(authoritative_floor);
                        authoritative_floor = authoritative_floor.max(snapshot.authoritative_revision);
                        caught_up = true;
                        if predicate(&snapshot) {
                            return Ok(snapshot);
                        }
                    }
                    Err(crate::coordination::etcd::WatchError::Disconnected) => {
                        emit(
                            self.event_hook.as_ref(),
                            RuntimeEvent::CoordinationRetry {
                                operation: String::from("observer watch"),
                            },
                        );
                        watch.resume_until_available().await?;
                    }
                    Err(crate::coordination::etcd::WatchError::Coordination(
                        CoordinationError::Etcd(_),
                    )) => {
                        emit(
                            self.event_hook.as_ref(),
                            RuntimeEvent::CoordinationRetry {
                                operation: String::from("observer coordination"),
                            },
                        );
                        watch.resume_until_available().await?;
                    }
                    Err(error) => return Err(WaitError::Watch(error)),
                    },
                    _ = poll.tick() => {
                        match self.snapshot().await {
                            Ok(candidate) => {
                                snapshot = candidate.with_authoritative_floor(authoritative_floor);
                                if caught_up && predicate(&snapshot) {
                                    return Ok(snapshot);
                                }
                            }
                            Err(CoordinationError::Etcd(_)) => {}
                            Err(error) => return Err(WaitError::Coordination(error)),
                        }
                    }
                }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| WaitError::Timeout(timeout))?
    }

    /// Wait until a controller is elected.
    pub async fn wait_for_controller(
        &self,
        timeout: Duration,
    ) -> Result<ClusterSnapshot, WaitError> {
        self.wait_until(timeout, |snapshot| snapshot.controllers.active().is_some())
            .await
    }

    /// Wait until no transitions remain and participant progress has caught up.
    pub async fn wait_for_idle(&self, timeout: Duration) -> Result<ClusterSnapshot, WaitError> {
        self.wait_until(timeout, |snapshot| {
            snapshot.pending_transitions.is_empty()
                && snapshot
                    .processed_revision
                    .is_some_and(|revision| revision >= snapshot.authoritative_revision)
        })
        .await
    }

    fn snapshot_from_coordination(
        coordination_snapshot: &CoordinationSnapshot,
    ) -> Result<ClusterSnapshot, CoordinationError> {
        let controllers = controllers(coordination_snapshot);
        let processed_revision = coordination_snapshot
            .metadata()
            .get("controller/output/processed-revision")
            .and_then(|entry| entry.value())
            .map(str::parse::<i64>)
            .transpose()
            .map_err(|_| CoordinationError::InvalidValue)?
            .map(Revision::new)
            .transpose()?;
        let external_view = coordination_snapshot
            .metadata()
            .get(EXTERNAL_VIEW_KEY)
            .and_then(|entry| entry.value())
            .map(|value| serde_json::from_str(value).map_err(|_| CoordinationError::InvalidValue))
            .transpose()?
            .unwrap_or_else(|| serde_json::json!({}));
        let pending_transitions = coordination_snapshot
            .metadata()
            .get(PENDING_KEY)
            .and_then(|entry| entry.value())
            .map(|value| serde_json::from_str(value).map_err(|_| CoordinationError::InvalidValue))
            .transpose()?
            .unwrap_or_default();
        let active_current_state = active_current_state(coordination_snapshot);
        let live_instances = coordination_snapshot
            .participants()
            .live_instances()
            .iter()
            .map(|(instance, live)| (instance.to_string(), live.session_id().wire_value()))
            .collect();
        let configured_instances = configured_instances(coordination_snapshot)?;
        let routing = routing_snapshot(&external_view, configured_instances)?;
        let routing_results = routing_results(&routing);
        Ok(ClusterSnapshot {
            observer_revision: ObserverRevision::from_revision(coordination_snapshot.revision()),
            authoritative_revision: ObserverRevision::from_revision(
                coordination_snapshot.authoritative_revision(),
            ),
            processed_revision: processed_revision.map(ObserverRevision::from_revision),
            controllers,
            live_instances,
            active_current_state,
            external_view,
            pending_transitions,
            routing_results,
        })
    }

    /// Return whether any physical CurrentState record exists for a session.
    pub async fn session_current_state_exists(
        &self,
        instance: &str,
        session: &str,
    ) -> Result<bool, CoordinationError> {
        let instance_id = InstanceId::new(instance).map_err(|_| CoordinationError::InvalidKey)?;
        let session_value = session
            .parse::<u64>()
            .map_err(|_| CoordinationError::InvalidSession)?;
        let session_id = crate::model::SessionId::from_wire_value(session_value);
        let (_, key_values) = self
            .coordination
            .raw_snapshot(
                self.coordination
                    .current_state_session_prefix(&instance_id, session_id)
                    .into_bytes(),
                Some(etcd_client::GetOptions::new().with_prefix()),
            )
            .await?;
        Ok(!key_values.is_empty())
    }
}

fn controllers(snapshot: &crate::coordination::etcd::CoordinationSnapshot) -> ControllerMembership {
    let active = snapshot
        .controller_election()
        .active()
        .map(|(controller, _)| vec![controller.to_owned()])
        .unwrap_or_default();
    let standby = snapshot
        .controller_election()
        .candidates()
        .keys()
        .filter(|candidate| !active.iter().any(|active| active == *candidate))
        .cloned()
        .collect();
    ControllerMembership { active, standby }
}

fn active_current_state(
    snapshot: &crate::coordination::etcd::CoordinationSnapshot,
) -> BTreeMap<String, ActiveCurrentStateObservation> {
    snapshot
        .participants()
        .active_current_state()
        .iter()
        .map(|(instance, active)| {
            let resources = active
                .resources()
                .iter()
                .map(|(resource, current)| {
                    let partitions = current
                        .entries()
                        .iter()
                        .flat_map(|(partition, replicas)| {
                            replicas
                                .values()
                                .map(move |state| (partition.to_string(), state.to_string()))
                        })
                        .collect();
                    (resource.to_string(), partitions)
                })
                .collect();
            (
                instance.to_string(),
                ActiveCurrentStateObservation {
                    session: active.session_id().wire_value(),
                    resources,
                },
            )
        })
        .collect()
}

fn configured_instances(
    snapshot: &crate::coordination::etcd::CoordinationSnapshot,
) -> Result<BTreeSet<InstanceId>, CoordinationError> {
    let Some(value) = snapshot
        .metadata()
        .get(INSTANCE_CONFIGS_KEY)
        .and_then(|entry| entry.value())
    else {
        return Ok(BTreeSet::new());
    };
    #[derive(serde::Deserialize)]
    struct Record {
        name: String,
    }
    serde_json::from_str::<Vec<Record>>(value)
        .map_err(|_| CoordinationError::InvalidValue)?
        .into_iter()
        .map(|record| InstanceId::new(record.name).map_err(|_| CoordinationError::InvalidKey))
        .collect()
}

fn routing_snapshot(
    external_view: &Value,
    configured_instances: BTreeSet<InstanceId>,
) -> Result<RoutingSnapshot, CoordinationError> {
    Ok(RoutingSnapshot::from_external_view(
        external_view_model(external_view)?,
        configured_instances,
    ))
}

fn external_view_model(
    external_view: &Value,
) -> Result<crate::model::ExternalView, CoordinationError> {
    let wire: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>> =
        serde_json::from_value(external_view.clone())
            .map_err(|_| CoordinationError::InvalidValue)?;
    let mut current = BTreeMap::new();
    for (resource_name, partitions) in wire {
        let resource = ResourceId::new(resource_name).map_err(|_| CoordinationError::InvalidKey)?;
        let mut resource_current = crate::model::CurrentState::builder();
        for (partition_name, instances) in partitions {
            let partition =
                PartitionId::new(partition_name).map_err(|_| CoordinationError::InvalidKey)?;
            for (instance_name, state_name) in instances {
                let instance =
                    InstanceId::new(instance_name).map_err(|_| CoordinationError::InvalidKey)?;
                let state = State::try_from(state_name.as_str())
                    .map_err(|_| CoordinationError::InvalidValue)?;
                resource_current
                    .set_state(partition.clone(), instance, state)
                    .map_err(|_| CoordinationError::InvalidValue)?;
            }
        }
        current.insert(resource, resource_current.build());
    }
    Ok(crate::model::ExternalView::from_current_states(current))
}

fn routing_results(routing: &RoutingSnapshot) -> Vec<RoutingResult> {
    let mut results = Vec::new();
    for (resource, partitions) in routing.external_view().entries() {
        for (partition, instances) in partitions {
            let states = instances.values().cloned().collect::<BTreeSet<_>>();
            for state in states {
                let routed = routing.instances_for(resource, partition, &state);
                results.push(RoutingResult {
                    resource: resource.to_string(),
                    partition: partition.to_string(),
                    state: state.to_string(),
                    instances: routed
                        .into_iter()
                        .map(|instance| instance.to_string())
                        .collect(),
                });
            }
        }
    }
    results
}

#[cfg(test)]
mod application_snapshot_tests {
    use super::{ClusterSnapshot, ControllerMembership, ObserverRevision, Snapshot};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn raw_snapshot(external_view: serde_json::Value) -> ClusterSnapshot {
        ClusterSnapshot {
            observer_revision: ObserverRevision::from_value(1),
            authoritative_revision: ObserverRevision::from_value(1),
            processed_revision: None,
            controllers: ControllerMembership {
                active: vec![],
                standby: vec![],
            },
            live_instances: BTreeMap::from([(String::from("node-a"), 1)]),
            active_current_state: BTreeMap::new(),
            external_view,
            pending_transitions: vec![],
            routing_results: vec![super::RoutingResult {
                resource: String::from("documents"),
                partition: String::from("documents_0"),
                state: String::from("LEADER"),
                instances: vec![String::from("node-a")],
            }],
        }
    }

    #[test]
    fn application_snapshot_precomputes_and_reuses_typed_routing() {
        let snapshot = Snapshot::from_raw(raw_snapshot(
            json!({"documents": {"documents_0": {"node-a": "LEADER"}}}),
        ))
        .expect("snapshot routing is valid");

        let first = snapshot.routing();
        let second = snapshot.routing();
        assert!(std::ptr::eq(first, second));
        assert_eq!(
            first
                .leader("documents", "documents_0")
                .expect("routing lookup is valid")
                .map(|instance| instance.to_string()),
            Some(String::from("node-a"))
        );
        assert!(snapshot.instances().is_live("node-a"));
    }

    #[test]
    fn application_snapshot_rejects_malformed_external_view() {
        assert!(matches!(
            Snapshot::from_raw(raw_snapshot(json!("not-an-external-view"))),
            Err(crate::coordination::etcd::CoordinationError::InvalidValue)
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::etcd::{
        CoordinationError, CoordinationSnapshot, MetadataEntry, Revision,
    };
    use crate::model::{
        ActiveCurrentState, CurrentState, InstanceId, LiveInstance, ParticipantSessionSnapshot,
        SessionId,
    };

    fn id<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: std::fmt::Debug,
    {
        value
            .to_owned()
            .try_into()
            .expect("test identifier is valid")
    }

    fn revision(value: i64) -> Revision {
        Revision::new(value).expect("test revision is valid")
    }

    fn metadata(value: &str, revision_value: i64) -> MetadataEntry {
        MetadataEntry {
            value: Some(value.to_owned()),
            revision: revision(revision_value),
        }
    }

    fn snapshot_with_state() -> CoordinationSnapshot {
        let instance = id::<InstanceId>("node-a");
        let session = SessionId::from_wire_value(7);
        let resource = id("documents");
        let mut current = CurrentState::builder();
        current
            .set_state(id("documents_0"), instance.clone(), id("LEADER"))
            .unwrap()
            .set_state(id("documents_1"), instance.clone(), id("STANDBY"))
            .unwrap();
        let mut resources = BTreeMap::new();
        resources.insert(resource, current.build());
        let active = ActiveCurrentState::new(session, resources);
        let mut live = BTreeMap::new();
        live.insert(
            instance.clone(),
            LiveInstance::new(instance.clone(), session),
        );
        let mut active_current_state = BTreeMap::new();
        active_current_state.insert(instance, active);
        CoordinationSnapshot::from_parts(
            revision(9),
            BTreeMap::from([
                (
                    INSTANCE_CONFIGS_KEY.to_owned(),
                    metadata(r#"[{"name":"node-a"}]"#, 5),
                ),
                (
                    EXTERNAL_VIEW_KEY.to_owned(),
                    metadata(
                        r#"{"documents":{"documents_0":{"node-a":"LEADER"},"documents_1":{"node-a":"STANDBY"}}}"#,
                        8,
                    ),
                ),
                (
                    PENDING_KEY.to_owned(),
                    metadata(
                        r#"[{"resource":"documents","partition":"documents_0","instance":"node-a","target_session":7,"from":"OFFLINE","to":"STANDBY","message_type":"STATE_TRANSITION","message_id":"m1"}]"#,
                        8,
                    ),
                ),
                (
                    "controller/output/processed-revision".to_owned(),
                    metadata("8", 8),
                ),
            ]),
            ParticipantSessionSnapshot::from_parts(live, active_current_state),
        )
    }

    #[test]
    fn public_observation_views_expose_typed_membership_and_routing() {
        let snapshot = ClusterSnapshot {
            observer_revision: ObserverRevision::from_value(9),
            authoritative_revision: ObserverRevision::from_value(8),
            processed_revision: Some(ObserverRevision::from_value(8)),
            controllers: ControllerMembership {
                active: vec![String::from("controller-a")],
                standby: vec![String::from("controller-b")],
            },
            live_instances: BTreeMap::from([(String::from("node-a"), 7)]),
            active_current_state: BTreeMap::new(),
            external_view: serde_json::json!({
                "documents": {
                    "documents_0": {
                        "node-a": "LEADER",
                        "node-b": "STANDBY"
                    }
                }
            }),
            pending_transitions: Vec::new(),
            routing_results: vec![RoutingResult {
                resource: String::from("documents"),
                partition: String::from("documents_0"),
                state: String::from("LEADER"),
                instances: vec![String::from("node-a")],
            }],
        };
        assert_eq!(snapshot.controllers().active(), Some("controller-a"));
        assert_eq!(
            snapshot.controllers().standby(),
            [String::from("controller-b")]
        );
        assert_eq!(snapshot.instances().len(), 1);
        assert!(!snapshot.instances().is_empty());
        assert!(snapshot.instances().is_live("node-a"));
        assert!(!snapshot.instances().is_live("node-b"));
        assert!(snapshot.pending_transitions().is_empty());
        assert!(snapshot.try_routing().is_ok());
        let routing = snapshot.routing();
        assert_eq!(
            routing
                .leader("documents", "documents_0")
                .unwrap()
                .unwrap()
                .as_str(),
            "node-a"
        );
        let typed = Snapshot::from_raw(snapshot.clone()).unwrap();
        assert_eq!(typed.observer_revision().value(), 9);
        assert_eq!(typed.authoritative_revision().value(), 8);
        assert_eq!(typed.processed_revision().unwrap().value(), 8);
        assert_eq!(typed.controllers().active(), Some("controller-a"));
        assert_eq!(typed.instances().len(), 1);
        assert!(!typed.instances().is_empty());
        assert!(typed.instances().is_live("node-a"));
        assert!(typed.pending_transitions().is_empty());
        assert!(serde_json::to_value(&typed).is_ok());
        assert_eq!(ObserverRevision::from_value(9).value(), 9);

        let mut malformed = snapshot;
        malformed.external_view = serde_json::json!("malformed");
        assert!(malformed.try_routing().is_err());
        malformed.external_view = serde_json::json!({
            "documents": {"documents_0": {"node-a": "LEADER"}}
        });
        malformed.routing_results[0].instances = vec![String::new()];
        assert!(matches!(
            malformed.try_routing(),
            Err(CoordinationError::InvalidValue)
        ));
    }

    #[test]
    fn observation_helpers_decode_state_and_reject_malformed_inputs() {
        let snapshot = snapshot_with_state();
        let observed = ClusterObserver::snapshot_from_coordination(&snapshot).unwrap();
        assert_eq!(observed.observer_revision.value(), 9);
        assert_eq!(observed.authoritative_revision.value(), 9);
        assert_eq!(observed.processed_revision.unwrap().value(), 8);
        assert_eq!(observed.live_instances["node-a"], 7);
        assert_eq!(observed.active_current_state["node-a"].session, 7);
        assert_eq!(observed.routing_results.len(), 2);
        assert_eq!(configured_instances(&snapshot).unwrap().len(), 1);

        let external = external_view_model(&serde_json::json!({
            "documents": {"documents_0": {"node-a": "LEADER"}}
        }))
        .unwrap();
        assert_eq!(external.entries().len(), 1);
        assert!(external_view_model(&serde_json::json!([])).is_err());
        assert!(external_view_model(&serde_json::json!({
            "": {"documents_0": {"node-a": "LEADER"}}
        }))
        .is_err());
        assert!(external_view_model(&serde_json::json!({
            "documents": {"": {"node-a": "LEADER"}}
        }))
        .is_err());
        assert!(external_view_model(&serde_json::json!({
            "documents": {"documents_0": {"": "LEADER"}}
        }))
        .is_err());
        assert!(external_view_model(&serde_json::json!({
            "documents": {"documents_0": {"node-a": ""}}
        }))
        .is_err());
    }

    #[test]
    fn observation_snapshot_reports_malformed_metadata() {
        let snapshot = CoordinationSnapshot::from_parts(
            revision(9),
            BTreeMap::from([(
                "controller/output/processed-revision".to_owned(),
                metadata("not-a-number", 8),
            )]),
            ParticipantSessionSnapshot::default(),
        );
        assert!(matches!(
            ClusterObserver::snapshot_from_coordination(&snapshot),
            Err(CoordinationError::InvalidValue)
        ));
    }

    #[test]
    fn observation_snapshot_handles_missing_and_malformed_configuration() {
        let empty = CoordinationSnapshot::from_parts(
            revision(9),
            BTreeMap::new(),
            ParticipantSessionSnapshot::default(),
        );
        let observed = ClusterObserver::snapshot_from_coordination(&empty).unwrap();
        assert!(observed.external_view.as_object().unwrap().is_empty());
        assert!(observed.live_instances.is_empty());
        assert!(observed.routing_results.is_empty());

        let malformed = CoordinationSnapshot::from_parts(
            revision(9),
            BTreeMap::from([(INSTANCE_CONFIGS_KEY.to_owned(), metadata("not-json", 8))]),
            ParticipantSessionSnapshot::default(),
        );
        assert!(matches!(
            ClusterObserver::snapshot_from_coordination(&malformed),
            Err(CoordinationError::InvalidValue)
        ));

        let invalid_identifier = CoordinationSnapshot::from_parts(
            revision(9),
            BTreeMap::from([(
                INSTANCE_CONFIGS_KEY.to_owned(),
                metadata(r#"[{"name":""}]"#, 8),
            )]),
            ParticipantSessionSnapshot::default(),
        );
        assert!(matches!(
            ClusterObserver::snapshot_from_coordination(&invalid_identifier),
            Err(CoordinationError::InvalidKey)
        ));
    }

    #[test]
    fn observation_snapshot_rejects_invalid_processed_and_pending_values() {
        for value in ["0", "-1", "not-a-revision"] {
            let snapshot = CoordinationSnapshot::from_parts(
                revision(9),
                BTreeMap::from([(
                    "controller/output/processed-revision".to_owned(),
                    metadata(value, 8),
                )]),
                ParticipantSessionSnapshot::default(),
            );
            assert!(
                ClusterObserver::snapshot_from_coordination(&snapshot).is_err(),
                "processed revision {value:?} should be rejected"
            );
        }

        let snapshot = CoordinationSnapshot::from_parts(
            revision(9),
            BTreeMap::from([(PENDING_KEY.to_owned(), metadata("not-json", 8))]),
            ParticipantSessionSnapshot::default(),
        );
        assert!(matches!(
            ClusterObserver::snapshot_from_coordination(&snapshot),
            Err(CoordinationError::InvalidValue)
        ));
    }

    #[test]
    fn observation_snapshot_maps_active_and_standby_controllers() {
        let snapshot = CoordinationSnapshot::from_parts(
            revision(9),
            BTreeMap::new(),
            ParticipantSessionSnapshot::default(),
        )
        .with_controller_election(
            Some((String::from("controller-a"), 11)),
            BTreeMap::from([
                (String::from("controller-a"), 11),
                (String::from("controller-b"), 12),
            ]),
        );
        let observed = ClusterObserver::snapshot_from_coordination(&snapshot).unwrap();
        assert_eq!(observed.controllers.active(), Some("controller-a"));
        assert_eq!(
            observed.controllers.standby,
            vec![String::from("controller-b")]
        );
    }

    #[test]
    fn wait_error_and_controller_membership_handle_empty_values() {
        assert_eq!(
            ControllerMembership {
                active: Vec::new(),
                standby: Vec::new(),
            }
            .active(),
            None
        );
        assert_eq!(
            WaitError::Timeout(Duration::from_millis(25)).to_string(),
            "observation timed out after 25ms"
        );
        assert_eq!(
            WaitError::Coordination(CoordinationError::InvalidKey).to_string(),
            "coordination key is invalid"
        );
        assert_eq!(
            WaitError::Watch(crate::coordination::etcd::WatchError::Disconnected).to_string(),
            "watch stream disconnected"
        );
        let _: WaitError = CoordinationError::InvalidValue.into();
        let _: WaitError = crate::coordination::etcd::WatchError::Disconnected.into();
        assert_eq!(
            ObserverRevision::from_value(2),
            ObserverRevision::from_value(2)
        );
    }
}
