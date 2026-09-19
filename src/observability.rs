//! Stable runtime signals for metrics, logs, and health endpoints.

use std::sync::Arc;

/// Events emitted by controller, participant, and observer runtime layers.
///
/// The events intentionally describe control-plane behavior rather than etcd
/// key names. Applications can map them to counters, structured logs, and
/// readiness/liveness state without coupling themselves to the storage layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeEvent {
    /// A controller acquired the fenced authority for its identity.
    AuthorityAcquired { identity: String },
    /// A controller observed that it no longer owns the fenced authority.
    AuthorityLost { identity: String },
    /// A participant lease expired or became unavailable and recovery began.
    LeaseRecovery { identity: String },
    /// A participant successfully registered a replacement session.
    LeaseReregistered { identity: String, session_id: u64 },
    /// A coordination operation is waiting before a transient retry.
    CoordinationRetry { operation: String },
    /// A computed publication was rejected by a fencing or revision check.
    PublicationRejected { reason: String },
    /// The runtime stopped because an unrecoverable error occurred.
    FatalRuntimeError { identity: String, message: String },
    /// The application requested an orderly shutdown.
    GracefulShutdown { identity: String },
}

/// A thread-safe callback used to consume runtime events.
pub type RuntimeEventHook = Arc<dyn Fn(RuntimeEvent) + Send + Sync + 'static>;

pub(crate) fn emit(hook: Option<&RuntimeEventHook>, event: RuntimeEvent) {
    if let Some(hook) = hook {
        hook(event);
    }
}
