//! Lease-backed controller election and the authority used to fence writes.

use crate::coordination::etcd::{
    is_authoritative_metadata_key, CoordinationError, EtcdCoordination, Revision,
};
use crate::model::{InstanceId, SessionId};
use etcd_client::{Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

const CAMPAIGN_PERIOD: Duration = Duration::from_millis(50);
const RELINQUISH_CONFIRM_ATTEMPTS: usize = 200;
const RELINQUISH_CONFIRM_DELAY: Duration = Duration::from_millis(10);

/// Configuration for one controller election candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerElectionConfig {
    pub cluster: String,
    pub controller_id: String,
    pub lease_ttl_ms: u64,
}

/// An opaque controller authority. Its lease identity is intentionally private.
#[derive(Clone)]
pub struct ControllerAuthority {
    pub(crate) namespace: String,
    pub(crate) controller_id: String,
    pub(crate) lease_id: i64,
    pub(crate) lost: Arc<AtomicBool>,
}

/// Internal publication identity shared by the controller runtime and its
/// deterministic concurrency model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ControllerCommitFence {
    pub(crate) namespace: String,
    pub(crate) controller_id: String,
    pub(crate) lease_id: i64,
    pub(crate) observed_revision: Revision,
    pub(crate) live_sessions: BTreeMap<InstanceId, SessionId>,
}

impl ControllerCommitFence {
    #[cfg(all(test, feature = "shuttle"))]
    pub(crate) fn matches(
        &self,
        active_controller_id: &str,
        active_lease_id: i64,
        input_revision: Revision,
        live_sessions: &BTreeMap<InstanceId, SessionId>,
    ) -> bool {
        self.controller_id == active_controller_id
            && self.lease_id == active_lease_id
            && self.observed_revision == input_revision
            && self.live_sessions == *live_sessions
    }
}

impl ControllerAuthority {
    pub(crate) fn commit_fence(
        &self,
        observed_revision: Revision,
        live_sessions: BTreeMap<InstanceId, SessionId>,
    ) -> ControllerCommitFence {
        ControllerCommitFence {
            namespace: self.namespace.clone(),
            controller_id: self.controller_id.clone(),
            lease_id: self.lease_id,
            observed_revision,
            live_sessions,
        }
    }
}

impl fmt::Debug for ControllerAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControllerAuthority")
            .field("controller_id", &self.controller_id)
            .finish_non_exhaustive()
    }
}

/// A controller lease and its authority guard.
pub struct Leadership {
    authority: ControllerAuthority,
    lost: Arc<Notify>,
    keepalive: Option<JoinHandle<()>>,
    coordination: EtcdCoordination,
}

struct KeepaliveGuard(Option<JoinHandle<()>>);

impl KeepaliveGuard {
    fn new(task: JoinHandle<()>) -> Self {
        Self(Some(task))
    }

    fn abort(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }

    fn disarm(&mut self) -> Option<JoinHandle<()>> {
        self.0.take()
    }
}

impl Drop for KeepaliveGuard {
    fn drop(&mut self) {
        self.abort();
    }
}

impl Leadership {
    /// Return an opaque authority suitable for controller-owned writes.
    pub fn authority(&self) -> ControllerAuthority {
        self.authority.clone()
    }

    pub(crate) fn is_lost(&self) -> bool {
        self.authority.lost.load(Ordering::Acquire)
    }

    pub(crate) async fn lost(&self) {
        if self.is_lost() {
            return;
        }
        self.lost.notified().await;
    }

    /// Relinquish this exact election lease immediately.
    pub(crate) async fn relinquish(&mut self) -> Result<(), ElectionError> {
        if let Some(task) = self.keepalive.take() {
            task.abort();
        }

        // etcd may acknowledge a lease revoke before the lease-attached keys
        // have been removed from the visible keyspace. Delete our election
        // records explicitly first so a caller that observes successful
        // shutdown cannot still see this controller as active. The lease
        // comparison prevents an old leadership handle from deleting a
        // replacement record after leadership has been lost.
        for key in [
            self.coordination
                .controller_candidate_key(&self.authority.controller_id),
            self.coordination.controller_active_key(),
        ] {
            let _ = self.delete_owned_election_key(key).await;
        }
        self.coordination
            .client()
            .lease_client()
            .revoke(self.authority.lease_id)
            .await?;
        self.confirm_election_records_released().await
    }

    async fn delete_owned_election_key(&self, key: String) -> Result<(), ElectionError> {
        self.coordination
            .client()
            .kv_client()
            .txn(
                Txn::new()
                    .when([Compare::lease(
                        key.clone(),
                        CompareOp::Equal,
                        self.authority.lease_id,
                    )])
                    .and_then([TxnOp::delete(key, None)]),
            )
            .await?;
        Ok(())
    }

    async fn confirm_election_records_released(&self) -> Result<(), ElectionError> {
        let keys = [
            self.coordination
                .controller_candidate_key(&self.authority.controller_id),
            self.coordination.controller_active_key(),
        ];
        let mut kv_client = self.coordination.client().kv_client();
        for attempt in 0..RELINQUISH_CONFIRM_ATTEMPTS {
            let mut owned_record_remains = false;
            for key in &keys {
                let response = kv_client.get(key.clone(), None).await?;
                owned_record_remains |= response
                    .kvs()
                    .first()
                    .is_some_and(|record| record.lease() == self.authority.lease_id);
            }
            if !owned_record_remains {
                return Ok(());
            }
            if attempt + 1 < RELINQUISH_CONFIRM_ATTEMPTS {
                tokio::time::sleep(RELINQUISH_CONFIRM_DELAY).await;
            }
        }
        Err(ElectionError::Coordination(CoordinationError::Contention))
    }
}

impl Drop for Leadership {
    fn drop(&mut self) {
        if let Some(task) = self.keepalive.take() {
            task.abort();
        }
    }
}

/// A reusable controller campaigner.
pub struct ControllerElection {
    coordination: EtcdCoordination,
    config: ControllerElectionConfig,
}

impl ControllerElection {
    /// Validate an election candidate against its coordination namespace.
    pub async fn new(
        coordination: EtcdCoordination,
        config: ControllerElectionConfig,
    ) -> Result<Self, ElectionError> {
        validate_config(&coordination, &config)?;
        Ok(Self {
            coordination,
            config,
        })
    }

    /// Wait until this candidate owns the active controller lease.
    pub async fn acquire(&self) -> Result<Leadership, ElectionError> {
        let mut shutdown = Box::pin(std::future::pending::<()>());
        self.acquire_until(shutdown.as_mut())
            .await?
            .ok_or(ElectionError::LeaseExpired)
    }

    /// Wait for leadership while allowing the caller to cancel the campaign.
    pub async fn acquire_until<F>(
        &self,
        mut shutdown: Pin<&mut F>,
    ) -> Result<Option<Leadership>, ElectionError>
    where
        F: Future<Output = ()> + Send,
    {
        loop {
            let lease_seconds = lease_seconds(self.config.lease_ttl_ms)?;
            let lease_ttl = Duration::from_secs(lease_seconds as u64);
            let lease_id = loop {
                let result = tokio::select! {
                    () = shutdown.as_mut() => return Ok(None),
                    result = self.grant_lease(lease_seconds) => result,
                };
                match result {
                    Ok(lease_id) => break lease_id,
                    Err(error) if error.is_transient() => {
                        tokio::select! {
                            () = shutdown.as_mut() => return Ok(None),
                            () = tokio::time::sleep(CAMPAIGN_PERIOD) => {}
                        }
                    }
                    Err(error) => return Err(error),
                }
            };
            let candidate_key = self
                .coordination
                .controller_candidate_key(&self.config.controller_id);
            let candidate_prefix = self
                .coordination
                .namespaced_key("controller/election/candidates/");
            let active_key = self.coordination.controller_active_key();
            let client = self.coordination.client();
            let mut kv_client = client.kv_client();
            let registered = loop {
                let result = tokio::select! {
                    () = shutdown.as_mut() => {
                        let _ = client.lease_client().revoke(lease_id).await;
                        return Ok(None);
                    }
                    result = kv_client.txn(
                        Txn::new()
                            .when([Compare::version(candidate_key.clone(), CompareOp::Equal, 0)])
                            .and_then([TxnOp::put(
                                candidate_key.clone(),
                                self.config.controller_id.clone(),
                                Some(PutOptions::new().with_lease(lease_id)),
                            )]),
                    ) => result,
                };
                match result {
                    Ok(response) => break response,
                    Err(error) => {
                        let error = CoordinationError::from(error);
                        if !error.is_transient() {
                            return Err(error.into());
                        }
                        tokio::select! {
                            () = shutdown.as_mut() => {
                                let _ = client.lease_client().revoke(lease_id).await;
                                return Ok(None);
                            }
                            () = tokio::time::sleep(CAMPAIGN_PERIOD) => {}
                        }
                    }
                }
            };
            if !registered.succeeded() {
                let _ = client.lease_client().revoke(lease_id).await;
                tokio::select! {
                    () = shutdown.as_mut() => return Ok(None),
                    () = tokio::time::sleep(CAMPAIGN_PERIOD) => {}
                }
                continue;
            }
            crate::failpoints::hard_abort("controller_after_election_candidate_registration");

            let (lost, lost_notify, keepalive_task) =
                spawn_keepalive(self.coordination.clone(), lease_id, lease_ttl);
            let mut keepalive = KeepaliveGuard::new(keepalive_task);
            loop {
                if lost.load(Ordering::Acquire) {
                    keepalive.abort();
                    return Err(ElectionError::LeaseExpired);
                }
                let candidates = match kv_client
                    .get(
                        candidate_prefix.clone(),
                        Some(GetOptions::new().with_prefix()),
                    )
                    .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        let error = CoordinationError::from(error);
                        if !error.is_transient() {
                            keepalive.abort();
                            return Err(error.into());
                        }
                        tokio::select! {
                            () = shutdown.as_mut() => {
                                keepalive.abort();
                                let _ = client.lease_client().revoke(lease_id).await;
                                return Ok(None);
                            }
                            () = lost_notify.notified() => {
                                keepalive.abort();
                                return Err(ElectionError::LeaseExpired);
                            }
                            () = tokio::time::sleep(CAMPAIGN_PERIOD) => {}
                        }
                        continue;
                    }
                };
                let Some(candidate) = candidates
                    .kvs()
                    .iter()
                    .find(|candidate| candidate.key() == candidate_key.as_bytes())
                else {
                    keepalive.abort();
                    return Err(ElectionError::LeaseExpired);
                };
                // Keep acquisition FIFO. A controller that loses its lease
                // must not race a standby that was already campaigning and
                // reclaim authority merely because it happened to retry
                // first after the active key disappeared.
                let predecessor_keys = candidates
                    .kvs()
                    .iter()
                    .filter(|other| other.create_revision() < candidate.create_revision())
                    .map(|other| other.key().to_vec())
                    .collect::<Vec<_>>();
                if !predecessor_keys.is_empty() {
                    tokio::select! {
                        () = shutdown.as_mut() => {
                            keepalive.abort();
                            client.lease_client().revoke(lease_id).await?;
                            return Ok(None);
                        }
                        () = lost_notify.notified() => {
                            keepalive.abort();
                            return Err(ElectionError::LeaseExpired);
                        }
                        () = tokio::time::sleep(CAMPAIGN_PERIOD) => {}
                    }
                    continue;
                }
                let compares =
                    std::iter::once(Compare::version(active_key.clone(), CompareOp::Equal, 0))
                        .chain(std::iter::once(Compare::create_revision(
                            candidate_key.clone(),
                            CompareOp::Equal,
                            candidate.create_revision(),
                        )))
                        .chain(std::iter::once(Compare::lease(
                            candidate_key.clone(),
                            CompareOp::Equal,
                            lease_id,
                        )))
                        .collect::<Vec<_>>();
                let response = match kv_client
                    .txn(Txn::new().when(compares).and_then([TxnOp::put(
                        active_key.clone(),
                        self.config.controller_id.clone(),
                        Some(PutOptions::new().with_lease(lease_id)),
                    )]))
                    .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        let error = CoordinationError::from(error);
                        if !error.is_transient() {
                            keepalive.abort();
                            return Err(error.into());
                        }
                        tokio::select! {
                            () = shutdown.as_mut() => {
                                keepalive.abort();
                                let _ = client.lease_client().revoke(lease_id).await;
                                return Ok(None);
                            }
                            () = lost_notify.notified() => {
                                keepalive.abort();
                                return Err(ElectionError::LeaseExpired);
                            }
                            () = tokio::time::sleep(CAMPAIGN_PERIOD) => {}
                        }
                        continue;
                    }
                };
                if response.succeeded() {
                    crate::failpoints::hard_abort("controller_after_active_election_txn");
                    return Ok(Some(Leadership {
                        authority: ControllerAuthority {
                            namespace: self.coordination.namespace().to_owned(),
                            controller_id: self.config.controller_id.clone(),
                            lease_id,
                            lost,
                        },
                        lost: lost_notify,
                        keepalive: keepalive.disarm(),
                        coordination: self.coordination.clone(),
                    }));
                }
                tokio::select! {
                    () = shutdown.as_mut() => {
                        keepalive.abort();
                        client.lease_client().revoke(lease_id).await?;
                        return Ok(None);
                    }
                    () = lost_notify.notified() => {
                        keepalive.abort();
                        return Err(ElectionError::LeaseExpired);
                    }
                    () = tokio::time::sleep(CAMPAIGN_PERIOD) => {}
                }
            }
        }
    }

    async fn grant_lease(&self, seconds: i64) -> Result<i64, ElectionError> {
        let response = self
            .coordination
            .client()
            .lease_client()
            .grant(seconds, None)
            .await?;
        crate::failpoints::hard_abort("controller_after_lease_grant");
        Ok(response.id())
    }
}

pub(crate) async fn put_controller_owned(
    coordination: &EtcdCoordination,
    authority: &ControllerAuthority,
    key: &str,
    value: Vec<u8>,
) -> Result<Revision, CoordinationError> {
    if authority.namespace != coordination.namespace() {
        return Err(CoordinationError::StaleController);
    }
    let active_key = coordination.controller_active_key();
    let response = coordination
        .client()
        .kv_client()
        .txn(
            Txn::new()
                .when([
                    Compare::value(
                        active_key.clone(),
                        CompareOp::Equal,
                        authority.controller_id.clone(),
                    ),
                    Compare::lease(active_key, CompareOp::Equal, authority.lease_id),
                ])
                .and_then({
                    let mut operations =
                        vec![TxnOp::put(coordination.metadata_key(key), value, None)];
                    if is_authoritative_metadata_key(key) {
                        operations.push(coordination.authoritative_input_marker_operation());
                    }
                    operations
                }),
        )
        .await?;
    if !response.succeeded() {
        return Err(CoordinationError::StaleController);
    }
    crate::coordination::etcd::revision_from_header(response.header())
}

fn spawn_keepalive(
    coordination: EtcdCoordination,
    lease_id: i64,
    lease_ttl: Duration,
) -> (Arc<AtomicBool>, Arc<Notify>, JoinHandle<()>) {
    let lost = Arc::new(AtomicBool::new(false));
    let lost_notify = Arc::new(Notify::new());
    let task_lost = Arc::clone(&lost);
    let task_notify = Arc::clone(&lost_notify);
    let keepalive_interval = (lease_ttl / 3)
        .min(Duration::from_secs(1))
        .max(Duration::from_millis(100));
    let keepalive_attempt_timeout = keepalive_interval.min(Duration::from_millis(500));
    let keepalive_retry_delay = Duration::from_millis(100);
    let task = tokio::spawn(async move {
        let mut last_keepalive = tokio::time::Instant::now();
        let mut next_keepalive = Duration::ZERO;
        loop {
            tokio::time::sleep(next_keepalive).await;
            if last_keepalive.elapsed() >= lease_ttl {
                task_lost.store(true, Ordering::Release);
                task_notify.notify_waiters();
                return;
            }
            let result = tokio::time::timeout(keepalive_attempt_timeout, async {
                let client = coordination.client();
                let (mut keeper, mut responses) =
                    client.lease_client().keep_alive(lease_id).await?;
                keeper.keep_alive().await?;
                responses.message().await
            })
            .await;
            next_keepalive = match result {
                Ok(Ok(Some(response))) if response.ttl() > 0 => {
                    last_keepalive = tokio::time::Instant::now();
                    keepalive_interval
                }
                Ok(Ok(Some(_))) => {
                    task_lost.store(true, Ordering::Release);
                    task_notify.notify_waiters();
                    return;
                }
                Ok(Ok(None)) | Ok(Err(_)) | Err(_) if last_keepalive.elapsed() < lease_ttl => {
                    keepalive_retry_delay
                }
                _ => {
                    task_lost.store(true, Ordering::Release);
                    task_notify.notify_waiters();
                    return;
                }
            };
        }
    });
    (lost, lost_notify, task)
}

fn validate_config(
    coordination: &EtcdCoordination,
    config: &ControllerElectionConfig,
) -> Result<(), ElectionError> {
    if config.cluster != coordination.cluster() {
        return Err(ElectionError::ClusterMismatch);
    }
    if config.controller_id.trim().is_empty() || config.controller_id.as_bytes().contains(&0) {
        return Err(ElectionError::InvalidControllerId);
    }
    lease_seconds(config.lease_ttl_ms)?;
    Ok(())
}

fn lease_seconds(ttl_ms: u64) -> Result<i64, ElectionError> {
    let seconds = ttl_ms
        .checked_add(999)
        .ok_or(ElectionError::InvalidLeaseTtl)?
        / 1_000;
    if seconds == 0 || seconds > i64::MAX as u64 {
        return Err(ElectionError::InvalidLeaseTtl);
    }
    Ok(seconds as i64)
}

/// Errors raised while campaigning for controller authority.
#[derive(Debug)]
pub enum ElectionError {
    Coordination(CoordinationError),
    ClusterMismatch,
    InvalidControllerId,
    InvalidLeaseTtl,
    LeaseExpired,
}

impl ElectionError {
    pub(crate) const fn is_transient(&self) -> bool {
        matches!(self, Self::Coordination(error) if error.is_transient())
    }
}

impl fmt::Display for ElectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Coordination(error) => error.fmt(formatter),
            Self::ClusterMismatch => {
                formatter.write_str("election cluster does not match coordination")
            }
            Self::InvalidControllerId => formatter.write_str("controller id must not be empty"),
            Self::InvalidLeaseTtl => {
                formatter.write_str("controller lease TTL must be at least one second")
            }
            Self::LeaseExpired => formatter.write_str("controller election lease expired"),
        }
    }
}

impl std::error::Error for ElectionError {}

impl From<CoordinationError> for ElectionError {
    fn from(error: CoordinationError) -> Self {
        Self::Coordination(error)
    }
}

impl From<etcd_client::Error> for ElectionError {
    fn from(error: etcd_client::Error) -> Self {
        Self::Coordination(CoordinationError::from(error))
    }
}

#[cfg(test)]
mod tests {
    use super::{lease_seconds, ControllerAuthority, ElectionError, KeepaliveGuard};
    use crate::coordination::etcd::{CoordinationError, Revision};
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    #[test]
    fn lease_ttl_rounds_up_at_the_etcd_second_boundary() {
        assert_eq!(lease_seconds(1_000).unwrap(), 1);
        assert_eq!(lease_seconds(1_500).unwrap(), 2);
        assert_eq!(lease_seconds(1_999).unwrap(), 2);
        assert!(lease_seconds(0).is_err());
        assert!(lease_seconds(u64::MAX).is_err());
    }

    #[test]
    fn election_errors_and_authority_debug_are_descriptive() {
        assert_eq!(
            ElectionError::Coordination(CoordinationError::InvalidKey).to_string(),
            "coordination key is invalid"
        );
        assert_eq!(
            ElectionError::ClusterMismatch.to_string(),
            "election cluster does not match coordination"
        );
        assert_eq!(
            ElectionError::InvalidControllerId.to_string(),
            "controller id must not be empty"
        );
        assert_eq!(
            ElectionError::InvalidLeaseTtl.to_string(),
            "controller lease TTL must be at least one second"
        );
        assert_eq!(
            ElectionError::LeaseExpired.to_string(),
            "controller election lease expired"
        );

        let authority = ControllerAuthority {
            namespace: String::from("namespace"),
            controller_id: String::from("controller-a"),
            lease_id: 7,
            lost: Arc::new(AtomicBool::new(false)),
        };
        assert!(format!("{authority:?}").contains("controller-a"));
        let fence = authority.commit_fence(Revision::new(9).unwrap(), BTreeMap::new());
        assert_eq!(fence.namespace, "namespace");
        assert_eq!(fence.controller_id, "controller-a");
        assert_eq!(fence.lease_id, 7);
        assert_eq!(fence.observed_revision, Revision::new(9).unwrap());
    }

    #[tokio::test]
    async fn keepalive_guard_can_abort_and_disarm_tasks() {
        let mut guard = KeepaliveGuard::new(tokio::spawn(async {
            std::future::pending::<()>().await;
        }));
        guard.abort();
        assert!(guard.disarm().is_none());

        let task = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        let mut guard = KeepaliveGuard::new(task);
        let task = guard.disarm().expect("task is armed");
        task.abort();
    }
}
