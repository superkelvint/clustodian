//! Standalone Rust implementation of selected Apache Helix semantics.
//!
//! The crate follows the control-plane flow documented in the README:
//! validated model snapshots become desired placement, transition decisions,
//! coordination records, and participant-side application requests.

#[cfg(all(feature = "shuttle", test))]
extern crate shuttle_tokio as tokio;
#[cfg(not(all(feature = "shuttle", test)))]
extern crate tokio;

pub mod admin;
mod cluster;
pub mod controller;
pub mod coordination;
pub mod election;
pub mod model;
pub mod observability;
pub mod observe;
pub mod participant;
pub mod rebalance;
pub mod routing;
pub mod runtime;
pub mod transition;

pub use cluster::{
    Admin, ApplicationError, Cluster, ClusterConfig, ClusterSpec, ConfigError, ControllerBuilder,
    InstanceSpec, ParticipantBuilder, Placement, PlacementBuilder, ResourceSpec, StateModel,
    Topology, TransitionLimit, TransitionType,
};
pub use coordination::etcd::{EtcdConnectionOptions, EtcdCoordinationConfig};
pub use observability::{RuntimeEvent, RuntimeEventHook};
pub use observe::{Observer, Snapshot};
pub use participant::{
    CancellationToken, ResourceHandler, ResourceState, ResourceTransition, TransitionAttemptId,
    TransitionContext, TransitionError,
};

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests;

#[cfg(all(test, not(feature = "shuttle")))]
pub(crate) mod participant_test_support;

#[cfg(feature = "m13-failpoints")]
pub(crate) mod failpoints {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use tokio::sync::Notify;

    struct AsyncPauseState {
        entered: AtomicBool,
        entered_notify: Notify,
        released: AtomicBool,
        release_notify: Notify,
    }

    static ASYNC_PAUSES: OnceLock<Mutex<BTreeMap<String, Arc<AsyncPauseState>>>> = OnceLock::new();

    fn async_pauses() -> &'static Mutex<BTreeMap<String, Arc<AsyncPauseState>>> {
        ASYNC_PAUSES.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    /// An async-aware pause used by integration tests that exercise races.
    ///
    /// This is exposed only with the failpoint feature and is not part of the
    /// normal runtime API.
    pub struct AsyncPause {
        name: String,
        state: Arc<AsyncPauseState>,
    }

    impl AsyncPause {
        /// Install a pause at the given failpoint name.
        pub fn install(name: impl Into<String>) -> Self {
            let name = name.into();
            let state = Arc::new(AsyncPauseState {
                entered: AtomicBool::new(false),
                entered_notify: Notify::new(),
                released: AtomicBool::new(false),
                release_notify: Notify::new(),
            });
            let previous = async_pauses()
                .lock()
                .expect("async failpoint registry is not poisoned")
                .insert(name.clone(), Arc::clone(&state));
            assert!(
                previous.is_none(),
                "async failpoint {name} is already installed"
            );
            Self { name, state }
        }

        /// Wait until the runtime reaches this pause.
        pub async fn wait_until_entered(&self) {
            while !self.state.entered.load(Ordering::Acquire) {
                let notified = self.state.entered_notify.notified();
                if self.state.entered.load(Ordering::Acquire) {
                    break;
                }
                notified.await;
            }
        }

        /// Allow the paused runtime task to continue.
        pub fn release(&self) {
            self.state.released.store(true, Ordering::Release);
            self.state.release_notify.notify_waiters();
        }
    }

    impl Drop for AsyncPause {
        fn drop(&mut self) {
            self.release();
            let mut pauses = async_pauses()
                .lock()
                .expect("async failpoint registry is not poisoned");
            if pauses
                .get(&self.name)
                .is_some_and(|state| Arc::ptr_eq(state, &self.state))
            {
                pauses.remove(&self.name);
            }
        }
    }

    pub(crate) async fn await_async_pause(name: &str) {
        let state = async_pauses()
            .lock()
            .expect("async failpoint registry is not poisoned")
            .get(name)
            .cloned();
        let Some(state) = state else {
            return;
        };
        state.entered.store(true, Ordering::Release);
        state.entered_notify.notify_waiters();
        while !state.released.load(Ordering::Acquire) {
            let notified = state.release_notify.notified();
            if state.released.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    fn record_hit(name: &str) {
        let Some(path) = std::env::var_os("CLUSTODIAN_CHAOS_HITS_FILE") else {
            return;
        };
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            use std::io::Write;
            let _ = writeln!(file, "failpoint/{name}");
        }
    }

    pub(crate) fn controlled_error(name: &str) -> Result<(), String> {
        record_hit(name);
        match fail::eval(name, |value| value) {
            Some(Some(message)) => Err(message),
            Some(None) | None => Ok(()),
        }
    }

    pub(crate) fn controlled_error_or_abort(name: &str) -> Result<(), String> {
        record_hit(name);
        let result = fail::eval(name, |value| value);
        if result
            .as_ref()
            .is_some_and(|value| value.as_deref() == Some("abort"))
        {
            std::process::abort();
        }
        match result {
            Some(Some(message)) => Err(message),
            Some(None) | None => Ok(()),
        }
    }

    pub(crate) fn hard_abort(name: &str) {
        record_hit(name);
        if fail::eval(name, |value| value).is_some_and(|value| value.as_deref() == Some("abort")) {
            std::process::abort();
        }
    }
}

#[cfg(not(feature = "m13-failpoints"))]
pub(crate) mod failpoints {
    pub(crate) async fn await_async_pause(_name: &str) {}

    pub(crate) fn controlled_error(_name: &str) -> Result<(), String> {
        Ok(())
    }

    pub(crate) fn controlled_error_or_abort(_name: &str) -> Result<(), String> {
        Ok(())
    }

    pub(crate) fn hard_abort(_name: &str) {}
}

#[cfg(feature = "m13-failpoints")]
#[doc(hidden)]
pub mod test_support {
    pub use crate::failpoints::AsyncPause;
}

/// The fallible result type used by the application-facing runtime APIs.
pub type Result<T> = std::result::Result<T, Error>;

/// A compact application/runtime error that remains compatible with anyhow callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error(String);

impl Error {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::new(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{failpoints, Error};

    #[test]
    fn runtime_error_and_noop_failpoints_are_stable() {
        let error = Error::new("runtime failure");
        assert_eq!(error.to_string(), "runtime failure");
        assert_eq!(
            Error::from(std::io::Error::other("io failure")).to_string(),
            "io failure"
        );
        assert!(failpoints::controlled_error("test-failpoint").is_ok());
        failpoints::hard_abort("test-failpoint");
    }
}
