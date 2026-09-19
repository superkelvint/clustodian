//! Public controller process runtime.

use crate::controller::{ControllerReconciler, LeaderRunResult};
use crate::coordination::etcd::EtcdCoordination;
use crate::election::{
    ControllerAuthority, ControllerElection, ControllerElectionConfig, ElectionError,
};
use crate::observability::{emit, RuntimeEvent, RuntimeEventHook};
use std::error::Error;
use std::future::Future;

pub use crate::transition::TransitionRequest;

/// Configuration for a controller process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerRuntimeConfig {
    pub cluster: String,
    pub controller_id: String,
    pub lease_ttl_ms: u64,
}

/// A controller process that campaigns and runs the existing M10 reconciler.
pub struct ControllerRuntime {
    coordination: EtcdCoordination,
    config: ControllerRuntimeConfig,
    ready_callback: Option<ReadyCallback>,
    authority_callback: Option<AuthorityReadyCallback>,
    event_hook: Option<RuntimeEventHook>,
}

type ReadyCallback = Box<dyn FnOnce() -> Result<(), Box<dyn Error + Send + Sync>> + Send + Sync>;
type AuthorityReadyCallback = Box<dyn FnOnce(ControllerAuthority) + Send + 'static>;

impl ControllerRuntime {
    /// Construct a failover-aware controller runtime.
    pub async fn new(
        coordination: EtcdCoordination,
        config: ControllerRuntimeConfig,
    ) -> crate::Result<Self> {
        ControllerElection::new(
            coordination.clone(),
            ControllerElectionConfig {
                cluster: config.cluster.clone(),
                controller_id: config.controller_id.clone(),
                lease_ttl_ms: config.lease_ttl_ms,
            },
        )
        .await
        .map_err(boxed)?;
        Ok(Self {
            coordination,
            config,
            ready_callback: None,
            authority_callback: None,
            event_hook: None,
        })
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

    /// Set a one-shot callback invoked when this process acquires controller
    /// authority. The callback receives the same lease-backed authority used
    /// by the reconciler, so application-specific controller work can use the
    /// coordination fence without manufacturing an election record.
    pub fn on_authority_ready<F>(mut self, callback: F) -> Self
    where
        F: FnOnce(ControllerAuthority) + Send + 'static,
    {
        self.authority_callback = Some(Box::new(callback));
        self
    }

    /// Register a callback for operational lifecycle and recovery events.
    pub fn on_event<F>(mut self, callback: F) -> Self
    where
        F: Fn(RuntimeEvent) + Send + Sync + 'static,
    {
        self.event_hook = Some(std::sync::Arc::new(callback));
        self
    }

    /// Campaign forever, returning only on an unrecoverable coordination error.
    pub async fn run(self) -> crate::Result<()> {
        self.run_until(std::future::pending()).await
    }

    /// Campaign until cancellation, relinquishing an acquired lease promptly.
    pub async fn run_until<F>(self, shutdown: F) -> crate::Result<()>
    where
        F: Future<Output = ()> + Send,
    {
        let mut reconciler = ControllerReconciler::new(self.coordination.clone());
        if let Some(event_hook) = self.event_hook.clone() {
            reconciler = reconciler.on_event_callback(event_hook);
        }
        let mut authority_callback = self.authority_callback;
        if let Some(callback) = self.ready_callback {
            reconciler = reconciler.on_ready_callback(callback);
        }
        tokio::pin!(shutdown);
        loop {
            let election = ControllerElection::new(
                self.coordination.clone(),
                ControllerElectionConfig {
                    cluster: self.config.cluster.clone(),
                    controller_id: self.config.controller_id.clone(),
                    lease_ttl_ms: self.config.lease_ttl_ms,
                },
            )
            .await
            .map_err(boxed)?;
            let mut leadership = match election.acquire_until(shutdown.as_mut()).await {
                Ok(Some(leadership)) => leadership,
                Ok(None) => {
                    emit(
                        self.event_hook.as_ref(),
                        RuntimeEvent::GracefulShutdown {
                            identity: self.config.controller_id.clone(),
                        },
                    );
                    return Ok(());
                }
                Err(ElectionError::LeaseExpired) => continue,
                Err(error) => {
                    let error = boxed(error);
                    emit(
                        self.event_hook.as_ref(),
                        RuntimeEvent::FatalRuntimeError {
                            identity: self.config.controller_id.clone(),
                            message: error.to_string(),
                        },
                    );
                    return Err(error);
                }
            };
            emit(
                self.event_hook.as_ref(),
                RuntimeEvent::AuthorityAcquired {
                    identity: self.config.controller_id.clone(),
                },
            );
            if let Some(callback) = authority_callback.take() {
                callback(leadership.authority());
            }
            crate::failpoints::hard_abort("controller_after_election_acquisition");
            let leader_result = reconciler
                .run_as_leader_until(&leadership, shutdown.as_mut())
                .await
                .map_err(boxed);
            match leader_result {
                Err(error) => {
                    // A fatal reconciler error must not leave the active
                    // election key around until the lease expires. If etcd
                    // is unavailable, Leadership's drop behavior still
                    // provides the best possible fallback and the original
                    // reconciler error remains the one reported to callers.
                    emit(
                        self.event_hook.as_ref(),
                        RuntimeEvent::FatalRuntimeError {
                            identity: self.config.controller_id.clone(),
                            message: error.to_string(),
                        },
                    );
                    let _ = leadership.relinquish().await;
                    return Err(error);
                }
                Ok(LeaderRunResult::LeadershipLost) => {
                    emit(
                        self.event_hook.as_ref(),
                        RuntimeEvent::AuthorityLost {
                            identity: self.config.controller_id.clone(),
                        },
                    );
                }
                Ok(LeaderRunResult::Shutdown) => {
                    leadership.relinquish().await.map_err(boxed)?;
                    emit(
                        self.event_hook.as_ref(),
                        RuntimeEvent::GracefulShutdown {
                            identity: self.config.controller_id.clone(),
                        },
                    );
                    return Ok(());
                }
            }
        }
    }
}

fn boxed<E>(error: E) -> crate::Error
where
    E: std::fmt::Display,
{
    crate::Error::new(error.to_string())
}
