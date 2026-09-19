//! Participant-side execution of controller transition messages.

use crate::coordination::etcd::{
    CompletionResult, CoordinationError, EtcdCoordination, ParticipantCompletionFence,
    RegistrationOptions, Revision, TransitionClaim, WatchError, WatchEvent, WatchEventKind,
    WatchSubscription, PENDING_TRANSITIONS_KEY,
};
use crate::model::{
    leader_standby, InstanceId, PartitionId, ResourceId, SessionId, State, StateModelDefinition,
};
use crate::observability::{emit, RuntimeEvent, RuntimeEventHook};
use crate::transition::TransitionMessage;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(60);
const KEEPALIVE_PERIOD: Duration = Duration::from_millis(250);
const MAX_COMPLETION_FENCE_RETRIES: usize = 256;
const MAX_CLEANUP_RETRIES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeepaliveOutcome {
    Healthy,
    Expired,
    Shutdown,
}

type PendingQueue = (Vec<TransitionMessage>, Revision, WatchSubscription);

pub(crate) struct CompletionFrontier {
    state: Mutex<CompletionState>,
}

pub(crate) struct CompletionState {
    revisions: BTreeMap<Revision, BTreeSet<String>>,
    completed: BTreeSet<String>,
    frontier: Option<Revision>,
}

impl CompletionFrontier {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(CompletionState::new()),
        }
    }

    pub(crate) async fn observe(&self, revision: Revision, message_ids: &[String]) {
        if message_ids.is_empty() {
            return;
        }
        let mut state = self.state.lock().await;
        state.observe(revision, message_ids);
    }

    pub(crate) async fn complete(&self, message_id: &str) -> Option<Revision> {
        let mut state = self.state.lock().await;
        state.complete(message_id)
    }
}

impl CompletionState {
    pub(crate) fn new() -> Self {
        Self {
            revisions: BTreeMap::new(),
            completed: BTreeSet::new(),
            frontier: None,
        }
    }

    pub(crate) fn observe(&mut self, revision: Revision, message_ids: &[String]) {
        self.revisions
            .entry(revision)
            .or_default()
            .extend(message_ids.iter().cloned());
    }

    pub(crate) fn complete(&mut self, message_id: &str) -> Option<Revision> {
        self.completed.insert(message_id.to_owned());
        let mut frontier = None;
        while let Some((&revision, message_ids)) = self.revisions.first_key_value() {
            if !message_ids
                .iter()
                .all(|message_id| self.completed.contains(message_id))
            {
                break;
            }
            let message_ids = self
                .revisions
                .pop_first()
                .expect("completion frontier entry exists")
                .1;
            frontier = Some(revision);
            for message_id in message_ids {
                if !self.revisions.values().any(|ids| ids.contains(&message_id)) {
                    self.completed.remove(&message_id);
                }
            }
        }
        if let Some(revision) = frontier {
            self.frontier = Some(revision);
        }
        frontier
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) const fn frontier(&self) -> Option<Revision> {
        self.frontier
    }
}

struct ParticipantWorkers {
    in_flight: Arc<Mutex<BTreeMap<String, String>>>,
    frontier: Arc<CompletionFrontier>,
    #[allow(dead_code)]
    cancellation: CancellationToken,
    workers: JoinSet<Result<(), ParticipantRuntimeError>>,
}

impl ParticipantWorkers {
    fn new() -> Self {
        Self {
            in_flight: Arc::new(Mutex::new(BTreeMap::new())),
            frontier: Arc::new(CompletionFrontier::new()),
            cancellation: CancellationToken::new(),
            workers: JoinSet::new(),
        }
    }
}

/// A cooperative cancellation signal supplied to an application transition.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl CancellationToken {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Request cancellation of work using this token.
    pub fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    /// Return whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Wait until cancellation has been requested.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        self.notify.notified().await;
    }
}

/// Context supplied to one application transition.
#[derive(Clone, Debug)]
pub struct TransitionContext {
    cancellation: CancellationToken,
}

impl TransitionContext {
    #[allow(dead_code)]
    fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }

    /// Return the cooperative cancellation signal for this transition.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// The typed LeaderStandby state projection exposed by the application facade.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResourceState {
    Offline,
    Standby,
    Leader,
    Dropped,
    Error,
    /// A state from a low-level/custom model not represented by the facade.
    Other(String),
}

impl ResourceState {
    #[allow(dead_code)]
    fn from_state(state: &State) -> Self {
        match state.as_str() {
            "OFFLINE" => Self::Offline,
            "STANDBY" => Self::Standby,
            "LEADER" => Self::Leader,
            "DROPPED" => Self::Dropped,
            "ERROR" => Self::Error,
            other => Self::Other(other.to_owned()),
        }
    }

    /// Return the low-level state name.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Offline => "OFFLINE",
            Self::Standby => "STANDBY",
            Self::Leader => "LEADER",
            Self::Dropped => "DROPPED",
            Self::Error => "ERROR",
            Self::Other(state) => state,
        }
    }
}

impl fmt::Display for ResourceState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable identity of one controller transition attempt.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransitionAttemptId(String);

impl TransitionAttemptId {
    /// Return the stable transition identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TransitionAttemptId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Owned application-facing description of one resource transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceTransition {
    partition: PartitionId,
    source: ResourceState,
    target: ResourceState,
    attempt_id: TransitionAttemptId,
}

impl ResourceTransition {
    /// Return the partition being transitioned.
    pub fn partition(&self) -> &PartitionId {
        &self.partition
    }

    /// Return the state reported before the transition.
    pub fn source(&self) -> ResourceState {
        self.source.clone()
    }

    /// Return the state requested by the controller.
    pub fn target(&self) -> ResourceState {
        self.target.clone()
    }

    /// Return the stable identity of this transition attempt.
    pub fn attempt_id(&self) -> &TransitionAttemptId {
        &self.attempt_id
    }

    #[allow(dead_code)]
    fn from_execution(execution: &TransitionExecution) -> Self {
        Self {
            partition: execution.partition.clone(),
            source: ResourceState::from_state(&execution.source_state),
            target: ResourceState::from_state(&execution.target_state),
            attempt_id: TransitionAttemptId(execution.transition_id.clone()),
        }
    }
}

/// Application boundary for realizing a facade resource transition.
pub trait ResourceHandler: Send + Sync + 'static {
    fn transition(
        &self,
        transition: ResourceTransition,
        context: TransitionContext,
    ) -> impl Future<Output = Result<(), TransitionError>> + Send;
}

/// The application/data-plane request for one state-model transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionExecution {
    resource: ResourceId,
    partition: PartitionId,
    source_state: State,
    target_state: State,
    transition_id: String,
}

impl TransitionExecution {
    fn new(message: &TransitionMessage) -> Result<Self, ParticipantRuntimeError> {
        Ok(Self {
            resource: ResourceId::try_from(message.resource.as_str())
                .map_err(|_| ParticipantRuntimeError::InvalidMessage)?,
            partition: PartitionId::try_from(message.partition.as_str())
                .map_err(|_| ParticipantRuntimeError::InvalidMessage)?,
            source_state: State::try_from(message.from.as_str())
                .map_err(|_| ParticipantRuntimeError::InvalidMessage)?,
            target_state: State::try_from(message.to.as_str())
                .map_err(|_| ParticipantRuntimeError::InvalidMessage)?,
            transition_id: message.message_id.clone(),
        })
    }

    /// Return the resource being transitioned.
    pub fn resource(&self) -> &ResourceId {
        &self.resource
    }
    /// Return the partition being transitioned.
    pub fn partition(&self) -> &PartitionId {
        &self.partition
    }
    /// Return the state reported before the transition.
    pub fn source_state(&self) -> &State {
        &self.source_state
    }
    /// Return the state requested by the controller.
    pub fn target_state(&self) -> &State {
        &self.target_state
    }
    /// Return the stable controller message identity.
    pub fn transition_id(&self) -> &str {
        &self.transition_id
    }
}

/// An application transition failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionHandlerError(String);

impl TransitionHandlerError {
    /// Construct an application transition error.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    /// Construct an error indicating cooperative transition cancellation.
    pub fn cancelled() -> Self {
        Self::new("transition cancelled")
    }
}

impl fmt::Display for TransitionHandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for TransitionHandlerError {}

/// The application-facing transition error used by the facade.
pub type TransitionError = TransitionHandlerError;

/// Application boundary for realizing a participant transition.
pub trait TransitionHandler: Send + Sync + 'static {
    fn handle(&self, execution: &TransitionExecution) -> Result<(), TransitionHandlerError>;
}

type HandlerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), HandlerInvocationError>> + Send + 'a>>;

enum HandlerInvocationError {
    Application,
    Join(String),
}

trait ErasedTransitionHandler: Send + Sync {
    fn transition<'a>(
        &'a self,
        execution: TransitionExecution,
        context: TransitionContext,
    ) -> HandlerFuture<'a>;
}

struct SyncHandlerAdapter<H>(Arc<H>);

impl<H> ErasedTransitionHandler for SyncHandlerAdapter<H>
where
    H: TransitionHandler,
{
    fn transition<'a>(
        &'a self,
        execution: TransitionExecution,
        _context: TransitionContext,
    ) -> HandlerFuture<'a> {
        let handler = Arc::clone(&self.0);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || handler.handle(&execution))
                .await
                .map_err(|error| HandlerInvocationError::Join(error.to_string()))?
                .map_err(|_| HandlerInvocationError::Application)
        })
    }
}

pub(crate) trait ErasedResourceHandler: Send + Sync {
    fn transition<'a>(
        &'a self,
        transition: ResourceTransition,
        context: TransitionContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransitionError>> + Send + 'a>>;
}

struct ResourceHandlerAdapter<H>(H);

impl<H> ErasedResourceHandler for ResourceHandlerAdapter<H>
where
    H: ResourceHandler,
{
    fn transition<'a>(
        &'a self,
        transition: ResourceTransition,
        context: TransitionContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransitionError>> + Send + 'a>> {
        Box::pin(self.0.transition(transition, context))
    }
}

pub(crate) fn erase_resource_handler<H>(handler: H) -> Box<dyn ErasedResourceHandler>
where
    H: ResourceHandler,
{
    Box::new(ResourceHandlerAdapter(handler))
}

pub(crate) struct AsyncScopedResourceHandler {
    handlers: BTreeMap<ResourceId, Box<dyn ErasedResourceHandler>>,
}

impl AsyncScopedResourceHandler {
    pub(crate) fn new(handlers: BTreeMap<ResourceId, Box<dyn ErasedResourceHandler>>) -> Self {
        Self { handlers }
    }
}

impl ErasedTransitionHandler for AsyncScopedResourceHandler {
    fn transition<'a>(
        &'a self,
        execution: TransitionExecution,
        context: TransitionContext,
    ) -> HandlerFuture<'a> {
        let Some(handler) = self.handlers.get(execution.resource()) else {
            return Box::pin(async { Err(HandlerInvocationError::Application) });
        };
        let transition = ResourceTransition::from_execution(&execution);
        Box::pin(async move {
            handler
                .transition(transition, context)
                .await
                .map_err(|_| HandlerInvocationError::Application)
        })
    }
}

/// A type-erased collection of resource-scoped transition handlers.
pub struct ScopedTransitionHandler {
    handlers: BTreeMap<ResourceId, Box<dyn TransitionHandler>>,
}

impl ScopedTransitionHandler {
    /// Construct a dispatcher from one handler per resource.
    pub fn new(handlers: BTreeMap<ResourceId, Box<dyn TransitionHandler>>) -> Self {
        Self { handlers }
    }
}

impl TransitionHandler for ScopedTransitionHandler {
    fn handle(&self, execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        self.handlers
            .get(execution.resource())
            .ok_or_else(|| {
                TransitionHandlerError::new(format!(
                    "no transition handler is registered for resource {}",
                    execution.resource()
                ))
            })?
            .handle(execution)
    }
}

/// A participant runtime that composes M9 session/watch semantics with an
/// application transition handler.
pub struct ParticipantRuntime<H> {
    backend: EtcdCoordination,
    instance: InstanceId,
    state_model: Option<StateModelDefinition>,
    handler: Arc<dyn ErasedTransitionHandler>,
    lease_ttl: Duration,
    event_hook: Option<RuntimeEventHook>,
    marker: PhantomData<fn() -> H>,
}

impl<H> ParticipantRuntime<H> {
    /// Construct a participant runtime with the default lease duration.
    pub fn new(
        backend: EtcdCoordination,
        instance: InstanceId,
        state_model: StateModelDefinition,
        handler: H,
    ) -> Self
    where
        H: TransitionHandler,
    {
        Self {
            backend,
            instance,
            state_model: Some(state_model),
            handler: Arc::new(SyncHandlerAdapter(Arc::new(handler))),
            lease_ttl: DEFAULT_LEASE_TTL,
            event_hook: None,
            marker: PhantomData,
        }
    }

    /// Construct a participant whose facade handlers are scoped by resource.
    pub(crate) fn new_async_scoped(
        backend: EtcdCoordination,
        instance: InstanceId,
        handlers: BTreeMap<ResourceId, Box<dyn ErasedResourceHandler>>,
    ) -> ParticipantRuntime<AsyncScopedResourceHandler> {
        ParticipantRuntime {
            backend,
            instance,
            state_model: None,
            handler: Arc::new(AsyncScopedResourceHandler::new(handlers)),
            lease_ttl: DEFAULT_LEASE_TTL,
            event_hook: None,
            marker: PhantomData,
        }
    }

    /// Construct a participant whose low-level handlers are scoped by resource.
    pub fn new_scoped(
        backend: EtcdCoordination,
        instance: InstanceId,
        handlers: BTreeMap<ResourceId, Box<dyn TransitionHandler>>,
    ) -> ParticipantRuntime<ScopedTransitionHandler> {
        ParticipantRuntime {
            backend,
            instance,
            state_model: None,
            handler: Arc::new(SyncHandlerAdapter(Arc::new(ScopedTransitionHandler::new(
                handlers,
            )))),
            lease_ttl: DEFAULT_LEASE_TTL,
            event_hook: None,
            marker: PhantomData,
        }
    }

    /// Set the participant lease duration.
    pub fn with_lease_ttl(mut self, lease_ttl: Duration) -> Result<Self, ParticipantRuntimeError> {
        RegistrationOptions::new(lease_ttl).map_err(ParticipantRuntimeError::Coordination)?;
        self.lease_ttl = lease_ttl;
        Ok(self)
    }

    /// Register a callback for operational lifecycle and recovery events.
    pub fn on_event<F>(mut self, callback: F) -> Self
    where
        F: Fn(RuntimeEvent) + Send + Sync + 'static,
    {
        self.event_hook = Some(Arc::new(callback));
        self
    }

    /// Return the metadata key used to record processed queue revisions.
    pub fn processed_revision_key(&self) -> String {
        processed_revision_key(&self.instance)
    }

    pub async fn run(
        self,
        ready: impl FnOnce() -> Result<(), ParticipantRuntimeError>,
    ) -> Result<(), ParticipantRuntimeError> {
        self.run_until(std::future::pending::<()>(), ready).await
    }

    /// Run until cancellation, draining started handlers before revocation.
    pub async fn run_until<F>(
        self,
        shutdown: F,
        ready: impl FnOnce() -> Result<(), ParticipantRuntimeError>,
    ) -> Result<(), ParticipantRuntimeError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let options = RegistrationOptions::new(self.lease_ttl)
            .map_err(ParticipantRuntimeError::Coordination)?;
        tokio::pin!(shutdown);
        let Some(mut session) =
            register_session_until(&self.backend, &self.instance, options, shutdown.as_mut())
                .await?
        else {
            emit(
                self.event_hook.as_ref(),
                RuntimeEvent::GracefulShutdown {
                    identity: self.instance.to_string(),
                },
            );
            return Ok(());
        };
        crate::failpoints::hard_abort("participant_after_live_registration_before_watch");
        let run_result = async {
            let (mut messages, mut queue_revision, mut watch) = self.pending_queue().await?;
            let mut workers = ParticipantWorkers::new();
            process_messages(&self, &session, &messages, queue_revision, &mut workers).await?;
            ready()?;

            let mut keepalive = tokio::time::interval(KEEPALIVE_PERIOD);
            let mut last_keepalive = tokio::time::Instant::now();
            loop {
                tokio::select! {
                    () = &mut shutdown => {
                        workers.cancellation.cancel();
                        return drain_workers_with_session(&session, &mut workers).await;
                    }
                    result = watch.next() => {
                        match result {
                            Ok(event) if event.key() == PENDING_TRANSITIONS_KEY => {
                                queue_revision = event.revision();
                                messages = parse_event_messages(&event)?;
                                process_messages(&self, &session, &messages, queue_revision, &mut workers).await?;
                            }
                            Ok(_) => {}
                            Err(WatchError::Compacted { .. }) => {
                                let (replacement_messages, revision, replacement_watch) = self.pending_queue().await?;
                                messages = replacement_messages;
                                queue_revision = revision;
                                watch = replacement_watch;
                                process_messages(&self, &session, &messages, queue_revision, &mut workers).await?;
                            }
                            Err(WatchError::Disconnected)
                            | Err(WatchError::Coordination(CoordinationError::Etcd(_))) => {
                                emit(
                                    self.event_hook.as_ref(),
                                    RuntimeEvent::CoordinationRetry {
                                        operation: String::from("participant watch"),
                                    },
                                );
                                tokio::select! {
                                    () = &mut shutdown => {
                                        workers.cancellation.cancel();
                                        return drain_workers_with_session(&session, &mut workers).await;
                                    }
                                    result = async {
                                        crate::failpoints::hard_abort("participant_before_watch_resume");
                                        watch.resume_until_available().await
                                    } => {
                                        result.map_err(ParticipantRuntimeError::from)?;
                                    }
                                }
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    _ = keepalive.tick() => {
                        let keepalive_outcome = keep_alive_until_available(
                            &session,
                            shutdown.as_mut(),
                            &mut last_keepalive,
                            self.lease_ttl,
                        ).await?;
                        if keepalive_outcome == KeepaliveOutcome::Shutdown {
                            workers.cancellation.cancel();
                            return drain_workers_with_session(&session, &mut workers).await;
                        }
                        if keepalive_outcome == KeepaliveOutcome::Expired {
                            emit(
                                self.event_hook.as_ref(),
                                RuntimeEvent::LeaseRecovery {
                                    identity: self.instance.to_string(),
                                },
                            );
                            workers.cancellation.cancel();
                            drain_workers_without_session(&mut workers).await?;
                            loop {
                                match reconnect(&self).await {
                                    Ok(replacement) => {
                                        emit(
                                            self.event_hook.as_ref(),
                                            RuntimeEvent::LeaseReregistered {
                                                identity: self.instance.to_string(),
                                                session_id: replacement.session_id().wire_value(),
                                            },
                                        );
                                        session = replacement;
                                        last_keepalive = tokio::time::Instant::now();
                                        break;
                                    }
                                    Err(ParticipantRuntimeError::Coordination(
                                        CoordinationError::Etcd(_),
                                    )) => {
                                        tokio::select! {
                                            () = &mut shutdown => return Ok(()),
                                            _ = tokio::time::sleep(KEEPALIVE_PERIOD) => {}
                                        }
                                    }
                                    Err(error) => return Err(error),
                                }
                            }
                            workers.cancellation = CancellationToken::new();
                            let (replacement_messages, revision, replacement_watch) = loop {
                                match self.pending_queue().await {
                                    Ok(queue) => break queue,
                                    Err(ParticipantRuntimeError::Coordination(
                                        CoordinationError::Etcd(_),
                                    )) => {
                                        tokio::select! {
                                            () = &mut shutdown => return Ok(()),
                                            _ = tokio::time::sleep(KEEPALIVE_PERIOD) => {}
                                        }
                                    }
                                    Err(error) => return Err(error),
                                }
                            };
                            messages = replacement_messages;
                            queue_revision = revision;
                            watch = replacement_watch;
                            process_messages(&self, &session, &messages, queue_revision, &mut workers).await?;
                        }
                    }
                    worker = workers.workers.join_next(), if !workers.workers.is_empty() => {
                        handle_worker_result(worker)?;
                        // A watch event may have been consumed while this
                        // worker was running. Re-evaluate the latest queue
                        // image after releasing the execution key so a
                        // superseding message does not wait for another
                        // etcd write to wake the participant.
                        process_messages(&self, &session, &messages, queue_revision, &mut workers).await?;
                    }
                }
            }
        }
        .await;
        let revoked = session
            .revoke()
            .await
            .map_err(ParticipantRuntimeError::Coordination);
        match (run_result, revoked) {
            (Err(error), _) => {
                emit(
                    self.event_hook.as_ref(),
                    RuntimeEvent::FatalRuntimeError {
                        identity: self.instance.to_string(),
                        message: error.to_string(),
                    },
                );
                Err(error)
            }
            (Ok(()), Err(error)) => {
                emit(
                    self.event_hook.as_ref(),
                    RuntimeEvent::FatalRuntimeError {
                        identity: self.instance.to_string(),
                        message: error.to_string(),
                    },
                );
                Err(error)
            }
            (Ok(()), Ok(())) => {
                emit(
                    self.event_hook.as_ref(),
                    RuntimeEvent::GracefulShutdown {
                        identity: self.instance.to_string(),
                    },
                );
                Ok(())
            }
        }
    }

    async fn state_model_for(
        &self,
        resource: &str,
    ) -> Result<StateModelDefinition, ParticipantRuntimeError> {
        if let Some(model) = &self.state_model {
            return Ok(model.clone());
        }
        let key = format!("controller/resources/{resource}");
        let entry = self
            .backend
            .get_metadata(&key)
            .await
            .map_err(ParticipantRuntimeError::Coordination)?
            .ok_or(ParticipantRuntimeError::InvalidMessage)?;
        #[derive(serde::Deserialize)]
        struct ResourceMetadata {
            state_model: String,
        }
        let metadata: ResourceMetadata = serde_json::from_str(entry.value().unwrap_or_default())
            .map_err(|_| ParticipantRuntimeError::InvalidMessage)?;
        if metadata.state_model != "LeaderStandby" {
            return Err(ParticipantRuntimeError::InvalidMessage);
        }
        Ok(leader_standby())
    }

    async fn pending_queue(&self) -> Result<PendingQueue, ParticipantRuntimeError> {
        self.backend
            .snapshot_and_watch_pending_transitions()
            .await
            .map_err(ParticipantRuntimeError::Coordination)
    }
}
#[cfg(test)]
async fn drain_workers(workers: &mut ParticipantWorkers) -> Result<(), ParticipantRuntimeError> {
    drain_workers_without_session(workers).await
}

async fn drain_workers_without_session(
    workers: &mut ParticipantWorkers,
) -> Result<(), ParticipantRuntimeError> {
    while let Some(worker) = workers.workers.join_next().await {
        handle_worker_result(Some(worker))?;
    }
    Ok(())
}

async fn drain_workers_with_session(
    session: &crate::coordination::etcd::EtcdParticipantSession,
    workers: &mut ParticipantWorkers,
) -> Result<(), ParticipantRuntimeError> {
    let mut keepalive = tokio::time::interval(KEEPALIVE_PERIOD);
    while !workers.workers.is_empty() {
        tokio::select! {
            worker = workers.workers.join_next() => {
                handle_worker_result(worker)?;
            }
            _ = keepalive.tick() => {
                session.keep_alive().await.map_err(ParticipantRuntimeError::Coordination)?;
            }
        }
    }
    Ok(())
}

fn handle_worker_result(
    worker: Option<Result<Result<(), ParticipantRuntimeError>, tokio::task::JoinError>>,
) -> Result<(), ParticipantRuntimeError> {
    match worker {
        Some(Ok(Ok(()))) | None => Ok(()),
        Some(Ok(Err(error))) => Err(error),
        Some(Err(error)) => Err(ParticipantRuntimeError::WorkerJoin(error.to_string())),
    }
}

async fn reconnect<H>(
    runtime: &ParticipantRuntime<H>,
) -> Result<crate::coordination::etcd::EtcdParticipantSession, ParticipantRuntimeError> {
    let options = RegistrationOptions::new(runtime.lease_ttl)
        .map_err(ParticipantRuntimeError::Coordination)?;
    register_session(&runtime.backend, &runtime.instance, options).await
}

async fn keep_alive_until_available<F>(
    session: &crate::coordination::etcd::EtcdParticipantSession,
    mut shutdown: std::pin::Pin<&mut F>,
    last_keepalive: &mut tokio::time::Instant,
    lease_ttl: Duration,
) -> Result<KeepaliveOutcome, ParticipantRuntimeError>
where
    F: std::future::Future<Output = ()> + Send,
{
    loop {
        if last_keepalive.elapsed() >= lease_ttl {
            return Ok(KeepaliveOutcome::Expired);
        }
        match session.is_current().await {
            Ok(false) => return Ok(KeepaliveOutcome::Expired),
            Ok(true) => {}
            Err(CoordinationError::Etcd(_)) => {
                let remaining = lease_ttl.saturating_sub(last_keepalive.elapsed());
                if remaining.is_zero() {
                    return Ok(KeepaliveOutcome::Expired);
                }
                tokio::select! {
                    () = shutdown.as_mut() => return Ok(KeepaliveOutcome::Shutdown),
                    () = tokio::time::sleep(KEEPALIVE_PERIOD.min(remaining)) => {}
                }
                continue;
            }
            Err(error) => return Err(error.into()),
        }
        match session.keep_alive().await {
            Ok(()) => match session.is_current().await {
                Ok(true) => {
                    *last_keepalive = tokio::time::Instant::now();
                    return Ok(KeepaliveOutcome::Healthy);
                }
                Ok(false) => return Ok(KeepaliveOutcome::Expired),
                Err(CoordinationError::Etcd(_)) => {
                    let remaining = lease_ttl.saturating_sub(last_keepalive.elapsed());
                    if remaining.is_zero() {
                        return Ok(KeepaliveOutcome::Expired);
                    }
                    tokio::select! {
                        () = shutdown.as_mut() => return Ok(KeepaliveOutcome::Shutdown),
                        () = tokio::time::sleep(KEEPALIVE_PERIOD.min(remaining)) => {}
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            },
            Err(CoordinationError::Etcd(_)) => {
                let remaining = lease_ttl.saturating_sub(last_keepalive.elapsed());
                if remaining.is_zero() {
                    return Ok(KeepaliveOutcome::Expired);
                }
                tokio::select! {
                    () = shutdown.as_mut() => return Ok(KeepaliveOutcome::Shutdown),
                    () = tokio::time::sleep(KEEPALIVE_PERIOD.min(remaining)) => {}
                }
            }
            Err(CoordinationError::LeaseExpired) => return Ok(KeepaliveOutcome::Expired),
            Err(error) => return Err(error.into()),
        }
    }
}

async fn register_session(
    backend: &EtcdCoordination,
    instance: &InstanceId,
    options: RegistrationOptions,
) -> Result<crate::coordination::etcd::EtcdParticipantSession, ParticipantRuntimeError> {
    let mut shutdown = Box::pin(std::future::pending::<()>());
    register_session_until(backend, instance, options, shutdown.as_mut())
        .await?
        .ok_or(ParticipantRuntimeError::InvalidMessage)
}

async fn register_session_until<F>(
    backend: &EtcdCoordination,
    instance: &InstanceId,
    options: RegistrationOptions,
    mut shutdown: std::pin::Pin<&mut F>,
) -> Result<Option<crate::coordination::etcd::EtcdParticipantSession>, ParticipantRuntimeError>
where
    F: std::future::Future<Output = ()> + Send,
{
    loop {
        if backend.live_session(instance).await?.is_none() {
            match backend.register(instance.clone(), options.clone()).await {
                Ok(session) => return Ok(Some(session)),
                Err(CoordinationError::RegistrationLost) => {}
                Err(error) => return Err(error.into()),
            }
        }
        tokio::select! {
            () = shutdown.as_mut() => return Ok(None),
            _ = tokio::time::sleep(KEEPALIVE_PERIOD) => {}
        }
    }
}

async fn process_messages<H>(
    runtime: &ParticipantRuntime<H>,
    session: &crate::coordination::etcd::EtcdParticipantSession,
    messages: &[TransitionMessage],
    queue_revision: Revision,
    workers: &mut ParticipantWorkers,
) -> Result<(), ParticipantRuntimeError> {
    let mut scheduled = Vec::new();
    let mut observed_ids = Vec::new();
    let mut guard = workers.in_flight.lock().await;
    for message in messages {
        if message.instance != runtime.instance.as_str() || message.message_id.is_empty() {
            continue;
        }
        let execution_key = format!("{}/{}", message.resource, message.partition);
        match guard.get(&execution_key) {
            Some(existing) if existing == &message.message_id => {
                observed_ids.push(message.message_id.clone());
            }
            Some(_) => {}
            None => {
                guard.insert(execution_key.clone(), message.message_id.clone());
                observed_ids.push(message.message_id.clone());
                scheduled.push((message.clone(), execution_key));
            }
        }
    }
    drop(guard);
    workers
        .frontier
        .observe(queue_revision, &observed_ids)
        .await;

    for (message, execution_key) in scheduled {
        let backend = runtime.backend.clone();
        let instance = runtime.instance.clone();
        let model = runtime.state_model_for(&message.resource).await?;
        let handler = Arc::clone(&runtime.handler);
        let in_flight = Arc::clone(&workers.in_flight);
        let frontier = Arc::clone(&workers.frontier);
        let cancellation = workers.cancellation.clone();
        let session_id = session.session_id();
        workers.workers.spawn(async move {
            let mut result = execute_message(
                &backend,
                &instance,
                session_id,
                &model,
                handler,
                TransitionContext::new(cancellation),
                &message,
                queue_revision,
            )
            .await;
            if matches!(
                result,
                Ok(CompletionResult::Applied(_))
                    | Ok(CompletionResult::SessionLost)
                    | Ok(CompletionResult::AlreadyCompleted)
            ) {
                if let Some(processed_revision) = frontier.complete(&message.message_id).await {
                    if let Err(error) = backend
                        .advance_metadata_revision(
                            &processed_revision_key(&instance),
                            processed_revision,
                        )
                        .await
                        .map_err(ParticipantRuntimeError::Coordination)
                    {
                        result = Err(error);
                    }
                }
            }
            let mut guard = in_flight.lock().await;
            if guard.get(&execution_key) == Some(&message.message_id) {
                guard.remove(&execution_key);
            }
            drop(guard);
            result.map(|_| ())
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_message(
    backend: &EtcdCoordination,
    instance: &InstanceId,
    session_id: SessionId,
    model: &StateModelDefinition,
    handler: Arc<dyn ErasedTransitionHandler>,
    context: TransitionContext,
    message: &TransitionMessage,
    queue_revision: Revision,
) -> Result<CompletionResult, ParticipantRuntimeError> {
    let resource = resource_id(message)?;
    let partition = partition_id(message)?;
    let current = backend
        .current_state(instance, session_id, &resource, &partition)
        .await?
        .unwrap_or_else(|| model.initial_state().clone());

    if message.message_type != "STATE_TRANSITION"
        || message.target_session != session_id.wire_value()
    {
        cleanup(backend, queue_revision, message).await?;
        return Ok(CompletionResult::AlreadyCompleted);
    }
    let from = State::try_from(message.from.as_str())
        .map_err(|_| ParticipantRuntimeError::InvalidMessage)?;
    let to = State::try_from(message.to.as_str())
        .map_err(|_| ParticipantRuntimeError::InvalidMessage)?;
    if current == to || current != from || model.next_state_toward(&current, &to) != Some(&to) {
        cleanup(backend, queue_revision, message).await?;
        return Ok(CompletionResult::AlreadyCompleted);
    }

    let execution = TransitionExecution::new(message)?;
    crate::failpoints::controlled_error_or_abort(
        "participant_after_transition_delivery_before_callback",
    )
    .map_err(ParticipantRuntimeError::Io)?;
    crate::failpoints::await_async_pause("participant_after_transition_delivery_before_callback")
        .await;
    let claim_revision = match backend
        .claim_pending_transition(instance, session_id, message)
        .await?
    {
        TransitionClaim::Claimed(revision) => revision,
        TransitionClaim::SessionLost => {
            cleanup(backend, queue_revision, message).await?;
            return Ok(CompletionResult::SessionLost);
        }
        TransitionClaim::Withdrawn => return Ok(CompletionResult::AlreadyCompleted),
    };
    let outcome = handler.transition(execution, context).await;
    let resulting_state = match outcome {
        Ok(()) if to.is_dropped() => None,
        Ok(()) => Some(to),
        Err(HandlerInvocationError::Application) => {
            Some(State::try_from("ERROR").expect("ERROR is a valid state"))
        }
        Err(HandlerInvocationError::Join(error)) => {
            return Err(ParticipantRuntimeError::HandlerJoin(error));
        }
    };
    crate::failpoints::controlled_error_or_abort(
        "participant_after_callback_success_before_current_state",
    )
    .map_err(ParticipantRuntimeError::Io)?;
    crate::failpoints::hard_abort("participant_before_completion_txn");
    let mut completion_revision = claim_revision;
    for _ in 0..MAX_COMPLETION_FENCE_RETRIES {
        let fence = ParticipantCompletionFence::new(
            instance.clone(),
            session_id,
            completion_revision,
            message.message_id.clone(),
        );
        match backend
            .complete_pending_transition_with_fence(&fence, message, resulting_state.as_ref())
            .await
        {
            Ok(result) => return Ok(result),
            Err(CoordinationError::StaleRevision) => {
                // The callback completed against an older queue image. Re-read
                // the queue and establish a new exact fence before publishing;
                // the public completion primitive never silently advances its
                // caller-provided fence.
                let Some(entry) = backend
                    .get_metadata(PENDING_TRANSITIONS_KEY)
                    .await
                    .map_err(ParticipantRuntimeError::Coordination)?
                else {
                    return Ok(CompletionResult::AlreadyCompleted);
                };
                completion_revision = entry.revision();
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(ParticipantRuntimeError::Coordination(
        CoordinationError::Contention,
    ))
}

async fn cleanup(
    backend: &EtcdCoordination,
    queue_revision: Revision,
    message: &TransitionMessage,
) -> Result<(), ParticipantRuntimeError> {
    let mut expected_revision = queue_revision;
    for _ in 0..MAX_CLEANUP_RETRIES {
        match backend
            .remove_pending_transition(expected_revision, &message.message_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(CoordinationError::StaleRevision) => {
                // Revalidate a stale queue observation before retrying cleanup.
                // remove_pending_transition itself remains strictly fenced.
                let Some(entry) = backend
                    .get_metadata(PENDING_TRANSITIONS_KEY)
                    .await
                    .map_err(ParticipantRuntimeError::Coordination)?
                else {
                    return Ok(());
                };
                expected_revision = entry.revision();
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(ParticipantRuntimeError::Coordination(
        CoordinationError::Contention,
    ))
}

fn parse_event_messages(
    event: &WatchEvent,
) -> Result<Vec<TransitionMessage>, ParticipantRuntimeError> {
    match event.kind() {
        WatchEventKind::Delete => Ok(Vec::new()),
        WatchEventKind::Put => parse_queue_value(
            event
                .value()
                .ok_or(ParticipantRuntimeError::InvalidMessage)?,
        ),
    }
}

fn parse_queue_value(value: &str) -> Result<Vec<TransitionMessage>, ParticipantRuntimeError> {
    let messages: Vec<TransitionMessage> =
        serde_json::from_str(value).map_err(|_| ParticipantRuntimeError::InvalidMessage)?;
    if messages.iter().any(|message| message.message_id.is_empty()) {
        return Err(ParticipantRuntimeError::InvalidMessage);
    }
    Ok(messages)
}

fn resource_id(message: &TransitionMessage) -> Result<ResourceId, ParticipantRuntimeError> {
    ResourceId::try_from(message.resource.as_str())
        .map_err(|_| ParticipantRuntimeError::InvalidMessage)
}

fn partition_id(message: &TransitionMessage) -> Result<PartitionId, ParticipantRuntimeError> {
    PartitionId::try_from(message.partition.as_str())
        .map_err(|_| ParticipantRuntimeError::InvalidMessage)
}

pub fn processed_revision_key(instance: &InstanceId) -> String {
    format!("participant/{}/processed-revision", instance)
}

#[derive(Debug)]
pub enum ParticipantRuntimeError {
    Coordination(CoordinationError),
    Watch(WatchError),
    Io(String),
    InvalidMessage,
    HandlerJoin(String),
    WorkerJoin(String),
    ProgressContention,
}

impl fmt::Display for ParticipantRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Coordination(error) => error.fmt(formatter),
            Self::Watch(error) => error.fmt(formatter),
            Self::Io(error) => formatter.write_str(error),
            Self::InvalidMessage => formatter.write_str("invalid participant transition message"),
            Self::HandlerJoin(error) => {
                write!(formatter, "transition handler task failed: {error}")
            }
            Self::WorkerJoin(error) => write!(formatter, "participant worker task failed: {error}"),
            Self::ProgressContention => {
                formatter.write_str("participant progress contention exceeded retry bound")
            }
        }
    }
}

impl std::error::Error for ParticipantRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Coordination(error) => Some(error),
            Self::Watch(error) => Some(error),
            Self::Io(_)
            | Self::InvalidMessage
            | Self::HandlerJoin(_)
            | Self::WorkerJoin(_)
            | Self::ProgressContention => None,
        }
    }
}

impl From<CoordinationError> for ParticipantRuntimeError {
    fn from(error: CoordinationError) -> Self {
        Self::Coordination(error)
    }
}

impl From<WatchError> for ParticipantRuntimeError {
    fn from(error: WatchError) -> Self {
        Self::Watch(error)
    }
}

impl From<std::io::Error> for ParticipantRuntimeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "shuttle"))]
    use super::SyncHandlerAdapter;
    #[cfg(not(feature = "shuttle"))]
    use super::PENDING_TRANSITIONS_KEY;
    use super::{
        drain_workers, erase_resource_handler, parse_event_messages, parse_queue_value,
        partition_id, processed_revision_key, resource_id, AsyncScopedResourceHandler,
        CancellationToken, CompletionFrontier, CompletionState, ErasedResourceHandler,
        ErasedTransitionHandler, HandlerInvocationError, ParticipantRuntimeError,
        ParticipantWorkers, ResourceHandler, ResourceHandlerAdapter, ResourceState,
        ResourceTransition, ScopedTransitionHandler, TransitionAttemptId, TransitionContext,
        TransitionExecution, TransitionHandler, TransitionHandlerError,
    };
    #[cfg(not(feature = "shuttle"))]
    use super::{
        drain_workers_without_session, execute_message, process_messages, reconnect,
        register_session, register_session_until, ParticipantRuntime,
    };
    #[cfg(not(feature = "shuttle"))]
    use crate::coordination::etcd::{CompletionResult, RegistrationOptions};
    use crate::coordination::etcd::{
        CoordinationError, Revision, WatchError, WatchEvent, WatchEventKind,
    };
    #[cfg(not(feature = "shuttle"))]
    use crate::model::leader_standby;
    use crate::model::{InstanceId, ResourceId};
    use crate::transition::TransitionMessage;
    use std::collections::BTreeMap;
    #[cfg(not(feature = "shuttle"))]
    use std::sync::Arc;
    #[cfg(not(feature = "shuttle"))]
    use std::time::Duration;

    struct RecordingHandler;

    impl TransitionHandler for RecordingHandler {
        fn handle(&self, _execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
            Ok(())
        }
    }

    struct FacadeHandler;

    impl ResourceHandler for FacadeHandler {
        async fn transition(
            &self,
            transition: ResourceTransition,
            context: TransitionContext,
        ) -> Result<(), super::TransitionError> {
            assert_eq!(transition.partition().as_str(), "documents_0");
            assert_eq!(transition.source(), ResourceState::Offline);
            assert_eq!(transition.target(), ResourceState::Standby);
            assert_eq!(transition.attempt_id().as_str(), "m1");
            assert!(!context.cancellation().is_cancelled());
            Ok(())
        }
    }

    struct FailingHandler;

    impl TransitionHandler for FailingHandler {
        fn handle(&self, _execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
            Err(TransitionHandlerError::new("application failure"))
        }
    }

    struct FailingFacadeHandler;

    impl ResourceHandler for FailingFacadeHandler {
        async fn transition(
            &self,
            _transition: ResourceTransition,
            _context: TransitionContext,
        ) -> Result<(), super::TransitionError> {
            Err(TransitionHandlerError::new("facade failure"))
        }
    }

    #[tokio::test]
    async fn facade_handler_is_erased_without_exposing_runtime_execution() {
        let message = TransitionMessage {
            message_id: String::from("m1"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 7,
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
        };
        let execution = TransitionExecution::new(&message).expect("valid transition");
        erase_resource_handler(FacadeHandler)
            .transition(
                ResourceTransition::from_execution(&execution),
                TransitionContext::new(CancellationToken::new()),
            )
            .await
            .expect("facade handler succeeds");
    }

    #[tokio::test]
    async fn cancellation_token_notifies_waiters_and_is_idempotent() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        let waiter = {
            let token = token.clone();
            tokio::spawn(async move {
                token.cancelled().await;
                token.is_cancelled()
            })
        };
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
                .await
                .expect("cancellation waiter is notified")
                .expect("cancellation waiter task succeeds")
        );

        let already_cancelled = token.clone();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            already_cancelled.cancelled(),
        )
        .await
        .expect("already-cancelled tokens return immediately");
    }

    #[test]
    fn facade_state_and_transition_projections_cover_all_states() {
        let states = [
            ("OFFLINE", ResourceState::Offline),
            ("STANDBY", ResourceState::Standby),
            ("LEADER", ResourceState::Leader),
            ("DROPPED", ResourceState::Dropped),
            ("ERROR", ResourceState::Error),
            ("CUSTOM", ResourceState::Other(String::from("CUSTOM"))),
        ];
        for (name, expected) in states {
            let state = crate::model::State::try_from(name).unwrap();
            let projected = ResourceState::from_state(&state);
            assert_eq!(projected, expected);
            assert_eq!(projected.as_str(), name);
            assert_eq!(projected.to_string(), name);
        }

        let attempt = TransitionAttemptId(String::from("attempt-1"));
        assert_eq!(attempt.as_str(), "attempt-1");
        assert_eq!(attempt.to_string(), "attempt-1");
    }

    #[test]
    fn resource_transition_is_owned_and_exposes_every_field() {
        let message = TransitionMessage {
            message_id: String::from("m1"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 7,
            from: String::from("CUSTOM_FROM"),
            to: String::from("CUSTOM_TO"),
            message_type: String::from("STATE_TRANSITION"),
        };
        let execution = TransitionExecution::new(&message).unwrap();
        let transition = ResourceTransition::from_execution(&execution);
        assert_eq!(transition.partition().as_str(), "documents_0");
        assert_eq!(
            transition.source(),
            ResourceState::Other(String::from("CUSTOM_FROM"))
        );
        assert_eq!(
            transition.target(),
            ResourceState::Other(String::from("CUSTOM_TO"))
        );
        assert_eq!(transition.attempt_id().as_str(), "m1");
    }

    #[test]
    fn handler_request_preserves_message_identity_and_transition_fields() {
        let message = TransitionMessage {
            message_id: String::from("m1"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 7,
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
        };
        let execution = TransitionExecution::new(&message).expect("valid transition");
        assert_eq!(execution.transition_id(), "m1");
        assert_eq!(execution.resource().as_str(), "documents");
        assert_eq!(execution.partition().as_str(), "documents_0");
        assert_eq!(execution.source_state().as_str(), "OFFLINE");
        assert_eq!(execution.target_state().as_str(), "STANDBY");
        assert_eq!(TransitionHandlerError::new("failed").to_string(), "failed");
        assert_eq!(
            TransitionHandlerError::cancelled().to_string(),
            "transition cancelled"
        );
    }

    #[cfg(not(feature = "shuttle"))]
    #[tokio::test]
    async fn synchronous_adapter_reports_success_application_and_join_outcomes() {
        let message = TransitionMessage {
            message_id: String::from("m1"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 1,
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
        };
        let execution = TransitionExecution::new(&message).unwrap();
        let context = TransitionContext::new(CancellationToken::new());
        assert!(SyncHandlerAdapter(Arc::new(RecordingHandler))
            .transition(execution.clone(), context.clone())
            .await
            .is_ok());
        assert!(matches!(
            SyncHandlerAdapter(Arc::new(FailingHandler))
                .transition(execution.clone(), context.clone())
                .await,
            Err(HandlerInvocationError::Application)
        ));

        struct PanickingHandler;
        impl TransitionHandler for PanickingHandler {
            fn handle(
                &self,
                _execution: &TransitionExecution,
            ) -> Result<(), TransitionHandlerError> {
                panic!("test handler panic")
            }
        }
        assert!(matches!(
            SyncHandlerAdapter(Arc::new(PanickingHandler))
                .transition(execution, context)
                .await,
            Err(HandlerInvocationError::Join(_))
        ));
    }

    #[tokio::test]
    async fn resource_adapters_preserve_success_and_application_errors() {
        let message = TransitionMessage {
            message_id: String::from("m1"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 1,
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
        };
        let execution = TransitionExecution::new(&message).unwrap();
        let transition = ResourceTransition::from_execution(&execution);
        assert!(ResourceHandlerAdapter(FacadeHandler)
            .transition(
                transition.clone(),
                TransitionContext::new(CancellationToken::new())
            )
            .await
            .is_ok());

        let mut handlers = BTreeMap::new();
        handlers.insert(
            ResourceId::new("documents").unwrap(),
            super::erase_resource_handler(FailingFacadeHandler),
        );
        assert!(matches!(
            AsyncScopedResourceHandler::new(handlers)
                .transition(
                    execution.clone(),
                    TransitionContext::new(CancellationToken::new())
                )
                .await,
            Err(HandlerInvocationError::Application)
        ));
        assert!(matches!(
            AsyncScopedResourceHandler::new(BTreeMap::new())
                .transition(execution, TransitionContext::new(CancellationToken::new()))
                .await,
            Err(HandlerInvocationError::Application)
        ));
    }

    #[test]
    fn queue_decoder_requires_explicit_ids() {
        let value = r#"[{"resource":"documents","partition":"documents_0","instance":"node-a","target_session":7,"from":"OFFLINE","to":"STANDBY","message_type":"STATE_TRANSITION"}]"#;
        assert!(matches!(
            parse_queue_value(value),
            Err(ParticipantRuntimeError::InvalidMessage)
        ));
        assert!(matches!(
            parse_queue_value("not-json"),
            Err(ParticipantRuntimeError::InvalidMessage)
        ));
    }

    #[test]
    fn message_identifiers_are_validated_at_the_participant_boundary() {
        let valid = TransitionMessage {
            message_id: String::from("m1"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 1,
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
        };
        assert_eq!(resource_id(&valid).unwrap().as_str(), "documents");
        assert_eq!(partition_id(&valid).unwrap().as_str(), "documents_0");

        for (field, message) in [
            (
                "resource",
                TransitionMessage {
                    resource: String::new(),
                    ..valid.clone()
                },
            ),
            (
                "partition",
                TransitionMessage {
                    partition: String::new(),
                    ..valid.clone()
                },
            ),
            (
                "source state",
                TransitionMessage {
                    from: String::new(),
                    ..valid.clone()
                },
            ),
            (
                "target state",
                TransitionMessage {
                    to: String::new(),
                    ..valid
                },
            ),
        ] {
            assert!(
                TransitionExecution::new(&message).is_err(),
                "invalid {field} should be rejected"
            );
        }
    }

    #[test]
    fn participant_error_and_progress_keys_are_stable() {
        let instance = InstanceId::new("node-a").expect("valid instance");
        assert_eq!(
            processed_revision_key(&instance),
            "participant/node-a/processed-revision"
        );
        assert_eq!(
            ParticipantRuntimeError::InvalidMessage.to_string(),
            "invalid participant transition message"
        );
        assert_eq!(
            ParticipantRuntimeError::HandlerJoin(String::from("panic")).to_string(),
            "transition handler task failed: panic"
        );
        assert_eq!(
            ParticipantRuntimeError::ProgressContention.to_string(),
            "participant progress contention exceeded retry bound"
        );
        assert_eq!(
            ParticipantRuntimeError::Io(String::from("io")).to_string(),
            "io"
        );
        assert_eq!(
            ParticipantRuntimeError::Coordination(CoordinationError::InvalidKey).to_string(),
            "coordination key is invalid"
        );
        assert_eq!(
            ParticipantRuntimeError::Watch(WatchError::Disconnected).to_string(),
            "watch stream disconnected"
        );
        assert_eq!(
            ParticipantRuntimeError::WorkerJoin(String::from("join")).to_string(),
            "participant worker task failed: join"
        );
        assert_eq!(
            ParticipantRuntimeError::from(std::io::Error::other("io-error")).to_string(),
            "io-error"
        );
    }

    #[test]
    fn participant_error_sources_and_conversions_preserve_categories() {
        let coordination = ParticipantRuntimeError::from(CoordinationError::InvalidKey);
        assert!(std::error::Error::source(&coordination).is_some());
        assert!(matches!(
            ParticipantRuntimeError::from(WatchError::Disconnected),
            ParticipantRuntimeError::Watch(WatchError::Disconnected)
        ));
        let watch = ParticipantRuntimeError::Watch(WatchError::Disconnected);
        assert!(std::error::Error::source(&watch).is_some());
        for error in [
            ParticipantRuntimeError::Io(String::from("io")),
            ParticipantRuntimeError::InvalidMessage,
            ParticipantRuntimeError::HandlerJoin(String::from("join")),
            ParticipantRuntimeError::WorkerJoin(String::from("worker")),
            ParticipantRuntimeError::ProgressContention,
        ] {
            assert!(std::error::Error::source(&error).is_none());
        }
    }

    #[test]
    fn processed_revision_waits_for_every_message_at_a_frontier() {
        let mut state = CompletionState::new();
        let revision = Revision::new(7).expect("valid revision");
        state.observe(revision, &[String::from("a"), String::from("b")]);
        assert_eq!(state.complete("a"), None);
        assert_eq!(state.frontier, None);
        assert_eq!(state.complete("b"), Some(revision));
        assert_eq!(state.frontier, Some(revision));
        assert_eq!(state.frontier(), Some(revision));
        let mut overlapping = CompletionState::new();
        overlapping.observe(Revision::new(1).unwrap(), &[String::from("shared")]);
        overlapping.observe(
            Revision::new(2).unwrap(),
            &[String::from("shared"), String::from("later")],
        );
        assert_eq!(
            overlapping.complete("shared"),
            Some(Revision::new(1).unwrap())
        );
        assert_eq!(
            overlapping.complete("later"),
            Some(Revision::new(2).unwrap())
        );
    }

    #[test]
    fn scoped_handlers_dispatch_and_report_missing_resources() {
        let resource = ResourceId::new("documents").unwrap();
        let mut handlers = BTreeMap::new();
        handlers.insert(
            resource.clone(),
            Box::new(RecordingHandler) as Box<dyn TransitionHandler>,
        );
        let dispatcher = ScopedTransitionHandler::new(handlers);
        let message = TransitionMessage {
            message_id: String::from("m1"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: 1,
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
        };
        let execution = TransitionExecution::new(&message).unwrap();
        assert!(dispatcher.handle(&execution).is_ok());
        let valid_execution = execution.clone();

        let missing = TransitionMessage {
            resource: String::from("missing"),
            ..message
        };
        let execution = TransitionExecution::new(&missing).unwrap();
        assert_eq!(
            dispatcher.handle(&execution).unwrap_err().to_string(),
            "no transition handler is registered for resource missing"
        );

        let mut failing_handlers = BTreeMap::new();
        failing_handlers.insert(
            resource,
            Box::new(FailingHandler) as Box<dyn TransitionHandler>,
        );
        let error = ScopedTransitionHandler::new(failing_handlers)
            .handle(&valid_execution)
            .unwrap_err();
        assert_eq!(error.to_string(), "application failure");
    }

    #[cfg(not(feature = "shuttle"))]
    #[test]
    fn participant_workers_start_empty_and_with_live_cancellation() {
        let workers = ParticipantWorkers::new();
        assert!(workers.in_flight.blocking_lock().is_empty());
        assert_eq!(workers.frontier.state.try_lock().unwrap().frontier, None);
        assert!(!workers.cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn completion_frontier_ignores_empty_observations_and_tracks_order() {
        let frontier = CompletionFrontier::new();
        frontier.observe(Revision::new(3).unwrap(), &[]).await;
        frontier
            .observe(
                Revision::new(4).unwrap(),
                &[String::from("m1"), String::from("m2")],
            )
            .await;
        assert_eq!(frontier.complete("unknown").await, None);
        assert_eq!(frontier.complete("m1").await, None);
        assert_eq!(
            frontier.complete("m2").await,
            Some(Revision::new(4).unwrap())
        );
    }

    #[tokio::test]
    async fn worker_drain_reports_success_handler_and_join_failures() {
        let mut workers = ParticipantWorkers::new();
        workers.workers.spawn(async { Ok(()) });
        assert!(drain_workers(&mut workers).await.is_ok());

        let mut workers = ParticipantWorkers::new();
        workers
            .workers
            .spawn(async { Err(ParticipantRuntimeError::Io(String::from("handler failed"))) });
        assert!(matches!(
            drain_workers(&mut workers).await,
            Err(ParticipantRuntimeError::Io(message)) if message == "handler failed"
        ));

        let mut workers = ParticipantWorkers::new();
        workers
            .workers
            .spawn(async { std::future::pending::<Result<(), ParticipantRuntimeError>>().await });
        workers.workers.abort_all();
        assert!(matches!(
            drain_workers(&mut workers).await,
            Err(ParticipantRuntimeError::WorkerJoin(_))
        ));
    }

    #[test]
    fn queue_watch_events_decode_puts_and_deletes() {
        let delete = WatchEvent::from_parts(
            Revision::new(1).unwrap(),
            String::from("pending-transitions"),
            None,
            WatchEventKind::Delete,
        );
        assert!(parse_event_messages(&delete).unwrap().is_empty());

        let missing_value = WatchEvent::from_parts(
            Revision::new(1).unwrap(),
            String::from("pending-transitions"),
            None,
            WatchEventKind::Put,
        );
        assert!(matches!(
            parse_event_messages(&missing_value),
            Err(ParticipantRuntimeError::InvalidMessage)
        ));

        let valid_value = WatchEvent::from_parts(
            Revision::new(1).unwrap(),
            String::from("pending-transitions"),
            Some(String::from(
                r#"[{"message_id":"m1","resource":"documents","partition":"documents_0","instance":"node-a","target_session":1,"from":"OFFLINE","to":"STANDBY","message_type":"STATE_TRANSITION"}]"#,
            )),
            WatchEventKind::Put,
        );
        assert_eq!(parse_event_messages(&valid_value).unwrap().len(), 1);
    }

    #[cfg(not(feature = "shuttle"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_etcd_participant_helpers_cover_completion_and_recovery_paths() {
        let backend = crate::participant_test_support::connect("participant-helpers")
            .await
            .expect("etcd is required for participant library coverage");
        let instance = InstanceId::new("node-a").unwrap();
        let options = RegistrationOptions::new(Duration::from_secs(10)).unwrap();
        let session = register_session(&backend, &instance, options.clone())
            .await
            .unwrap();
        let runtime = ParticipantRuntime::new(
            backend.clone(),
            instance.clone(),
            leader_standby(),
            RecordingHandler,
        );
        assert_eq!(
            runtime.state_model_for("documents").await.unwrap(),
            leader_standby()
        );
        backend
            .put_metadata(
                "controller/resources/documents",
                r#"{"name":"documents","state_model":"LeaderStandby"}"#,
            )
            .await
            .unwrap();
        let scoped = ParticipantRuntime::<AsyncScopedResourceHandler>::new_async_scoped(
            backend.clone(),
            instance.clone(),
            BTreeMap::new(),
        );
        assert_eq!(
            scoped.state_model_for("documents").await.unwrap(),
            leader_standby()
        );
        backend
            .put_metadata(
                "controller/resources/documents",
                r#"{"name":"documents","state_model":"Other"}"#,
            )
            .await
            .unwrap();
        assert!(matches!(
            scoped.state_model_for("documents").await,
            Err(ParticipantRuntimeError::InvalidMessage)
        ));
        assert!(matches!(
            scoped.state_model_for("missing").await,
            Err(ParticipantRuntimeError::InvalidMessage)
        ));
        backend
            .put_metadata("controller/resources/documents", "not-json")
            .await
            .unwrap();
        assert!(matches!(
            scoped.state_model_for("documents").await,
            Err(ParticipantRuntimeError::InvalidMessage)
        ));
        let low_level = ParticipantRuntime::<ScopedTransitionHandler>::new_scoped(
            backend.clone(),
            instance.clone(),
            BTreeMap::new(),
        )
        .with_lease_ttl(Duration::from_secs(5))
        .unwrap();
        assert_eq!(
            low_level.processed_revision_key(),
            runtime.processed_revision_key()
        );
        backend
            .put_metadata(
                "controller/resources/documents",
                r#"{"name":"documents","state_model":"LeaderStandby"}"#,
            )
            .await
            .unwrap();
        let (initial_messages, _, _watch) = runtime.pending_queue().await.unwrap();
        assert!(initial_messages.is_empty());

        let mut foreign = TransitionMessage {
            message_id: String::from("helper-foreign"),
            instance: String::from("other-node"),
            ..TransitionMessage {
                message_id: String::from("helper-template"),
                resource: String::from("documents"),
                partition: String::from("documents_ignored"),
                instance: String::from("node-a"),
                target_session: session.session_id().wire_value(),
                from: String::from("OFFLINE"),
                to: String::from("STANDBY"),
                message_type: String::from("STATE_TRANSITION"),
            }
        };
        let empty_id = TransitionMessage {
            message_id: String::new(),
            ..foreign.clone()
        };
        let mut skipped_workers = ParticipantWorkers::new();
        process_messages(
            &runtime,
            &session,
            &[foreign.clone(), empty_id],
            Revision::new(1).unwrap(),
            &mut skipped_workers,
        )
        .await
        .unwrap();
        assert!(skipped_workers.workers.is_empty());
        foreign.instance = instance.as_str().to_owned();
        foreign.resource = String::from("missing");
        let mut missing_model_workers = ParticipantWorkers::new();
        assert!(matches!(
            process_messages(
                &scoped,
                &session,
                &[foreign],
                Revision::new(1).unwrap(),
                &mut missing_model_workers,
            )
            .await,
            Err(ParticipantRuntimeError::InvalidMessage)
        ));

        let valid = TransitionMessage {
            message_id: String::from("helper-valid"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: String::from("node-a"),
            target_session: session.session_id().wire_value(),
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
        };
        let queue_revision = backend.inject_pending_transition(&valid).await.unwrap();
        let mut workers = ParticipantWorkers::new();
        process_messages(
            &runtime,
            &session,
            std::slice::from_ref(&valid),
            queue_revision,
            &mut workers,
        )
        .await
        .unwrap();
        drain_workers_without_session(&mut workers).await.unwrap();
        assert_eq!(
            backend
                .current_state(
                    &instance,
                    session.session_id(),
                    &ResourceId::new("documents").unwrap(),
                    &crate::model::PartitionId::new("documents_0").unwrap(),
                )
                .await
                .unwrap()
                .unwrap()
                .as_str(),
            "STANDBY"
        );

        let mut stale = valid.clone();
        stale.message_id = String::from("helper-stale");
        stale.target_session += 1;
        let stale_revision = backend.inject_pending_transition(&stale).await.unwrap();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::clone(&runtime.handler),
                TransitionContext::new(CancellationToken::new()),
                &stale,
                stale_revision,
            )
            .await
            .unwrap(),
            CompletionResult::AlreadyCompleted
        ));

        let mut wrong_type = valid.clone();
        wrong_type.message_id = String::from("helper-wrong-type");
        wrong_type.message_type = String::from("OTHER");
        let wrong_type_revision = backend
            .inject_pending_transition(&wrong_type)
            .await
            .unwrap();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::clone(&runtime.handler),
                TransitionContext::new(CancellationToken::new()),
                &wrong_type,
                wrong_type_revision,
            )
            .await
            .unwrap(),
            CompletionResult::AlreadyCompleted
        ));

        let mut current_mismatch = valid.clone();
        current_mismatch.message_id = String::from("helper-current-mismatch");
        current_mismatch.to = String::from("LEADER");
        let current_mismatch_revision = backend
            .inject_pending_transition(&current_mismatch)
            .await
            .unwrap();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::clone(&runtime.handler),
                TransitionContext::new(CancellationToken::new()),
                &current_mismatch,
                current_mismatch_revision,
            )
            .await
            .unwrap(),
            CompletionResult::AlreadyCompleted
        ));

        let mut invalid_target = valid.clone();
        invalid_target.message_id = String::from("helper-invalid-target");
        invalid_target.to = String::new();
        let invalid_target_revision = backend
            .inject_pending_transition(&invalid_target)
            .await
            .unwrap();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::clone(&runtime.handler),
                TransitionContext::new(CancellationToken::new()),
                &invalid_target,
                invalid_target_revision,
            )
            .await,
            Err(ParticipantRuntimeError::InvalidMessage)
        ));

        let mut unreachable = valid.clone();
        unreachable.message_id = String::from("helper-unreachable");
        unreachable.partition = String::from("documents_unreachable");
        unreachable.to = String::from("LEADER");
        let unreachable_revision = backend
            .inject_pending_transition(&unreachable)
            .await
            .unwrap();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::clone(&runtime.handler),
                TransitionContext::new(CancellationToken::new()),
                &unreachable,
                unreachable_revision,
            )
            .await
            .unwrap(),
            CompletionResult::AlreadyCompleted
        ));

        let mut already_applied = valid.clone();
        already_applied.message_id = String::from("helper-already-applied");
        let already_revision = backend
            .inject_pending_transition(&already_applied)
            .await
            .unwrap();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::clone(&runtime.handler),
                TransitionContext::new(CancellationToken::new()),
                &already_applied,
                already_revision,
            )
            .await
            .unwrap(),
            CompletionResult::AlreadyCompleted
        ));

        let mut non_adjacent = valid.clone();
        non_adjacent.message_id = String::from("helper-non-adjacent");
        non_adjacent.from = String::from("STANDBY");
        non_adjacent.to = String::from("DROPPED");
        let non_adjacent_revision = backend
            .inject_pending_transition(&non_adjacent)
            .await
            .unwrap();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::clone(&runtime.handler),
                TransitionContext::new(CancellationToken::new()),
                &non_adjacent,
                non_adjacent_revision,
            )
            .await
            .unwrap(),
            CompletionResult::AlreadyCompleted
        ));

        let mut failed = valid.clone();
        failed.message_id = String::from("helper-failure");
        failed.partition = String::from("documents_1");
        let failed_revision = backend.inject_pending_transition(&failed).await.unwrap();
        let failing_handler: Arc<dyn ErasedTransitionHandler> =
            Arc::new(SyncHandlerAdapter(Arc::new(FailingHandler)));
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                failing_handler,
                TransitionContext::new(CancellationToken::new()),
                &failed,
                failed_revision,
            )
            .await
            .unwrap(),
            CompletionResult::Applied(_)
        ));
        assert_eq!(
            backend
                .current_state(
                    &instance,
                    session.session_id(),
                    &ResourceId::new("documents").unwrap(),
                    &crate::model::PartitionId::new("documents_1").unwrap(),
                )
                .await
                .unwrap()
                .unwrap()
                .as_str(),
            "ERROR"
        );

        let mut dropped = valid.clone();
        dropped.message_id = String::from("helper-dropped");
        dropped.partition = String::from("documents_2");
        dropped.to = String::from("DROPPED");
        let dropped_revision = backend.inject_pending_transition(&dropped).await.unwrap();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::clone(&runtime.handler),
                TransitionContext::new(CancellationToken::new()),
                &dropped,
                dropped_revision,
            )
            .await
            .unwrap(),
            CompletionResult::Applied(_)
        ));
        assert!(backend
            .current_state(
                &instance,
                session.session_id(),
                &ResourceId::new("documents").unwrap(),
                &crate::model::PartitionId::new("documents_2").unwrap(),
            )
            .await
            .unwrap()
            .is_none());

        struct PanickingHandler;
        impl TransitionHandler for PanickingHandler {
            fn handle(
                &self,
                _execution: &TransitionExecution,
            ) -> Result<(), TransitionHandlerError> {
                panic!("participant test handler panic")
            }
        }
        let mut panicking = valid.clone();
        panicking.message_id = String::from("helper-panic");
        panicking.partition = String::from("documents_3");
        let panicking_revision = backend.inject_pending_transition(&panicking).await.unwrap();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::new(SyncHandlerAdapter(Arc::new(PanickingHandler))),
                TransitionContext::new(CancellationToken::new()),
                &panicking,
                panicking_revision,
            )
            .await,
            Err(ParticipantRuntimeError::HandlerJoin(_))
        ));
        backend
            .remove_pending_transition(panicking_revision, &panicking.message_id)
            .await
            .unwrap();

        let mut in_flight = ParticipantWorkers::new();
        in_flight
            .in_flight
            .lock()
            .await
            .insert(String::from("documents/documents_0"), String::from("same"));
        let mut same = valid.clone();
        same.message_id = String::from("same");
        let mut conflicting = same.clone();
        conflicting.message_id = String::from("different");
        process_messages(
            &runtime,
            &session,
            &[same, conflicting],
            queue_revision,
            &mut in_flight,
        )
        .await
        .unwrap();
        assert!(in_flight.workers.is_empty());

        let mut invalid_state = valid.clone();
        invalid_state.message_id = String::from("helper-invalid-state");
        invalid_state.from.clear();
        assert!(matches!(
            execute_message(
                &backend,
                &instance,
                session.session_id(),
                &leader_standby(),
                Arc::clone(&runtime.handler),
                TransitionContext::new(CancellationToken::new()),
                &invalid_state,
                queue_revision,
            )
            .await,
            Err(ParticipantRuntimeError::InvalidMessage)
        ));

        let mut shutdown = Box::pin(async {});
        assert!(
            register_session_until(&backend, &instance, options, shutdown.as_mut(),)
                .await
                .unwrap()
                .is_none()
        );
        session.revoke().await.unwrap();
        let replacement = reconnect(&runtime).await.unwrap();
        replacement.revoke().await.unwrap();
    }

    #[cfg(not(feature = "shuttle"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_etcd_participant_registration_and_run_error_paths() {
        let backend = crate::participant_test_support::connect("participant-registration")
            .await
            .expect("etcd is required for participant library coverage");
        let instance = InstanceId::new("node-registration").unwrap();
        let options = RegistrationOptions::new(Duration::from_secs(10)).unwrap();
        let existing = register_session(&backend, &instance, options.clone())
            .await
            .unwrap();
        let runtime = ParticipantRuntime::new(
            backend.clone(),
            instance.clone(),
            leader_standby(),
            RecordingHandler,
        );
        assert!(runtime.run_until(async {}, || Ok(())).await.is_ok());
        existing.revoke().await.unwrap();

        let runtime = ParticipantRuntime::new(
            backend,
            InstanceId::new("node-run").unwrap(),
            leader_standby(),
            RecordingHandler,
        );
        assert!(matches!(
            runtime
                .run(|| Err(ParticipantRuntimeError::Io(String::from("ready failed"))))
                .await,
            Err(ParticipantRuntimeError::Io(message)) if message == "ready failed"
        ));
    }

    #[cfg(not(feature = "shuttle"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_etcd_participant_worker_and_watch_error_paths() {
        let backend = crate::participant_test_support::connect("participant-errors")
            .await
            .expect("etcd is required for participant library coverage");

        let invalid_instance = InstanceId::new("node-invalid-worker").unwrap();
        let invalid_runtime = ParticipantRuntime::new(
            backend.clone(),
            invalid_instance.clone(),
            leader_standby(),
            RecordingHandler,
        );
        let invalid_task = tokio::spawn(invalid_runtime.run(|| Ok(())));
        let invalid_session = loop {
            if let Some(session) = backend.live_session(&invalid_instance).await.unwrap() {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let invalid = TransitionMessage {
            message_id: String::from("invalid-worker-message"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: invalid_instance.as_str().to_owned(),
            target_session: invalid_session.wire_value(),
            from: String::new(),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
        };
        backend.inject_pending_transition(&invalid).await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), invalid_task)
                .await
                .unwrap()
                .unwrap(),
            Err(ParticipantRuntimeError::InvalidMessage)
        ));

        struct PanickingHandler;
        impl TransitionHandler for PanickingHandler {
            fn handle(
                &self,
                _execution: &TransitionExecution,
            ) -> Result<(), TransitionHandlerError> {
                panic!("participant worker test panic")
            }
        }
        let panic_instance = InstanceId::new("node-panic-worker").unwrap();
        let panic_runtime = ParticipantRuntime::new(
            backend.clone(),
            panic_instance.clone(),
            leader_standby(),
            PanickingHandler,
        );
        let panic_task = tokio::spawn(panic_runtime.run(|| Ok(())));
        let panic_session = loop {
            if let Some(session) = backend.live_session(&panic_instance).await.unwrap() {
                break session;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let mut panic_message = invalid.clone();
        panic_message.message_id = String::from("panic-worker-message");
        panic_message.instance = panic_instance.as_str().to_owned();
        panic_message.target_session = panic_session.wire_value();
        panic_message.from = String::from("OFFLINE");
        backend
            .inject_pending_transition(&panic_message)
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), panic_task)
                .await
                .unwrap()
                .unwrap(),
            Err(ParticipantRuntimeError::HandlerJoin(_))
        ));

        let malformed_instance = InstanceId::new("node-malformed-watch").unwrap();
        let malformed_runtime = ParticipantRuntime::new(
            backend.clone(),
            malformed_instance.clone(),
            leader_standby(),
            RecordingHandler,
        );
        let (malformed_ready_tx, malformed_ready_rx) = tokio::sync::oneshot::channel();
        let malformed_task = tokio::spawn(malformed_runtime.run(move || {
            malformed_ready_tx
                .send(())
                .map_err(|_| ParticipantRuntimeError::Io(String::from("ready receiver dropped")))
        }));
        malformed_ready_rx.await.unwrap();
        backend
            .put_metadata(PENDING_TRANSITIONS_KEY, "not-json")
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), malformed_task)
                .await
                .unwrap()
                .unwrap(),
            Err(ParticipantRuntimeError::InvalidMessage)
        ));
    }

    #[cfg(not(feature = "shuttle"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn real_etcd_library_runtime_executes_async_facade_and_shutdown_paths() {
        let backend = crate::participant_test_support::connect("participant-runtime")
            .await
            .expect("etcd is required for participant library coverage");
        backend
            .put_metadata(
                "controller/resources/documents",
                r#"{"name":"documents","state_model":"LeaderStandby"}"#,
            )
            .await
            .unwrap();
        let instance = InstanceId::new("node-runtime").unwrap();
        let handlers = BTreeMap::from([(
            ResourceId::new("documents").unwrap(),
            erase_resource_handler(FacadeHandler),
        )]);
        let runtime = ParticipantRuntime::<AsyncScopedResourceHandler>::new_async_scoped(
            backend.clone(),
            instance.clone(),
            handlers,
        );
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(runtime.run_until(
            async move {
                let _ = shutdown_rx.await;
            },
            move || {
                ready_tx.send(()).map_err(|_| {
                    ParticipantRuntimeError::Io(String::from("ready receiver dropped"))
                })
            },
        ));
        ready_rx.await.unwrap();
        let session = backend
            .live_session(&instance)
            .await
            .unwrap()
            .expect("library participant registered");
        let barrier = backend
            .put_metadata("participant-library-compaction-barrier", "ready")
            .await
            .unwrap();
        backend.compact(barrier).await.unwrap();
        let message = TransitionMessage {
            message_id: String::from("m1"),
            resource: String::from("documents"),
            partition: String::from("documents_0"),
            instance: instance.as_str().to_owned(),
            target_session: session.wire_value(),
            from: String::from("OFFLINE"),
            to: String::from("STANDBY"),
            message_type: String::from("STATE_TRANSITION"),
        };
        backend.inject_pending_transition(&message).await.unwrap();
        for _ in 0..200 {
            if backend
                .current_state(
                    &instance,
                    session,
                    &ResourceId::new("documents").unwrap(),
                    &crate::model::PartitionId::new("documents_0").unwrap(),
                )
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            backend
                .current_state(
                    &instance,
                    session,
                    &ResourceId::new("documents").unwrap(),
                    &crate::model::PartitionId::new("documents_0").unwrap(),
                )
                .await
                .unwrap()
                .unwrap()
                .as_str(),
            "STANDBY"
        );
        shutdown_tx.send(()).unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .is_ok());
    }
}
