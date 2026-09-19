//! Concrete etcd coordination backend.

mod metadata;
mod session;
mod watch;

use crate::model::{
    ActiveCurrentState, CurrentState, InstanceId, LiveInstance, ParticipantSessionSnapshot,
    PartitionId, ResourceId, SessionId, State,
};
use crate::transition::TransitionMessage;
use etcd_client::Client;
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// Connection settings for one isolated etcd coordination namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EtcdCoordinationConfig {
    pub endpoint: String,
    pub prefix: String,
    pub cluster: String,
}

/// Optional etcd transport and authentication settings.
///
/// The default is intentionally suitable for a local, unsecured etcd started
/// on `127.0.0.1`. Production deployments should configure HTTPS, a trusted
/// CA, and authentication explicitly. Certificate values are PEM encoded.
///
/// This type owns the secret values needed to build `etcd_client::ConnectOptions`
/// but does not expose them through `Debug`.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct EtcdConnectionOptions {
    username: Option<String>,
    password: Option<String>,
    ca_certificate: Option<Vec<u8>>,
    client_certificate: Option<Vec<u8>>,
    client_key: Option<Vec<u8>>,
    tls_domain_name: Option<String>,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    keep_alive_interval: Option<Duration>,
    keep_alive_timeout: Option<Duration>,
    keep_alive_while_idle: bool,
}

impl fmt::Debug for EtcdConnectionOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EtcdConnectionOptions")
            .field("username", &self.username.as_ref().map(|_| "<redacted>"))
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field(
                "ca_certificate",
                &self.ca_certificate.as_ref().map(|_| "<configured>"),
            )
            .field(
                "client_certificate",
                &self.client_certificate.as_ref().map(|_| "<configured>"),
            )
            .field(
                "client_key",
                &self.client_key.as_ref().map(|_| "<redacted>"),
            )
            .field("tls_domain_name", &self.tls_domain_name)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("keep_alive_interval", &self.keep_alive_interval)
            .field("keep_alive_timeout", &self.keep_alive_timeout)
            .field("keep_alive_while_idle", &self.keep_alive_while_idle)
            .finish()
    }
}

impl EtcdConnectionOptions {
    /// Create unsecured local-development options.
    pub const fn new() -> Self {
        Self {
            username: None,
            password: None,
            ca_certificate: None,
            client_certificate: None,
            client_key: None,
            tls_domain_name: None,
            connect_timeout: None,
            request_timeout: None,
            keep_alive_interval: None,
            keep_alive_timeout: None,
            keep_alive_while_idle: true,
        }
    }

    /// Configure etcd username/password authentication.
    pub fn with_user(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// Alias for [`Self::with_user`] that makes the authentication intent explicit.
    pub fn with_credentials(
        self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.with_user(username, password)
    }

    /// Configure a PEM-encoded CA certificate for server verification.
    pub fn with_ca_certificate(mut self, certificate_pem: impl Into<Vec<u8>>) -> Self {
        self.ca_certificate = Some(certificate_pem.into());
        self
    }

    /// Alias for [`Self::with_ca_certificate`].
    pub fn with_tls_ca(self, certificate_pem: impl Into<Vec<u8>>) -> Self {
        self.with_ca_certificate(certificate_pem)
    }

    /// Configure a PEM-encoded client certificate and private key for mTLS.
    pub fn with_client_identity(
        mut self,
        certificate_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_certificate = Some(certificate_pem.into());
        self.client_key = Some(key_pem.into());
        self
    }

    /// Alias for [`Self::with_client_identity`].
    pub fn with_client_certificate(
        self,
        certificate_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.with_client_identity(certificate_pem, key_pem)
    }

    /// Override the TLS server-name used for certificate verification.
    pub fn with_tls_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.tls_domain_name = Some(domain_name.into());
        self
    }

    /// Set the timeout for establishing a connection.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set the timeout applied to each gRPC request.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Set HTTP/2 keepalive interval and timeout.
    pub fn with_keep_alive(mut self, interval: Duration, timeout: Duration) -> Self {
        self.keep_alive_interval = Some(interval);
        self.keep_alive_timeout = Some(timeout);
        self
    }

    /// Enable or disable HTTP/2 keepalive pings while no streams are active.
    pub fn with_keep_alive_while_idle(mut self, enabled: bool) -> Self {
        self.keep_alive_while_idle = enabled;
        self
    }

    /// Return whether TLS material or a TLS server name was configured.
    pub fn tls_enabled(&self) -> bool {
        self.ca_certificate.is_some()
            || self.client_certificate.is_some()
            || self.client_key.is_some()
            || self.tls_domain_name.is_some()
    }

    /// Validate settings before attempting a network connection.
    pub fn validate(&self) -> Result<(), String> {
        if self.username.is_some() != self.password.is_some() {
            return Err("etcd username and password must be configured together".to_owned());
        }
        if self.client_certificate.is_some() != self.client_key.is_some() {
            return Err("etcd client certificate and key must be configured together".to_owned());
        }
        if self.tls_enabled()
            && self
                .ca_certificate
                .as_ref()
                .is_some_and(|certificate| certificate.is_empty())
        {
            return Err("etcd TLS CA certificate must not be empty".to_owned());
        }
        if self.client_certificate.as_ref().is_some_and(Vec::is_empty)
            || self.client_key.as_ref().is_some_and(Vec::is_empty)
        {
            return Err("etcd client identity must not be empty".to_owned());
        }
        for (name, timeout) in [
            ("connect", self.connect_timeout),
            ("request", self.request_timeout),
            ("keepalive interval", self.keep_alive_interval),
            ("keepalive timeout", self.keep_alive_timeout),
        ] {
            if timeout.is_some_and(|value| value.is_zero()) {
                return Err(format!("etcd {name} timeout must be positive"));
            }
        }
        if self
            .tls_domain_name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty() || name.as_bytes().contains(&0))
        {
            return Err("etcd TLS domain name must not be empty or contain NUL".to_owned());
        }
        Ok(())
    }

    fn to_connect_options(&self) -> Result<etcd_client::ConnectOptions, CoordinationError> {
        self.validate()
            .map_err(CoordinationError::InvalidConnectionOptions)?;
        let mut options = etcd_client::ConnectOptions::new();
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            options = options.with_user(username.clone(), password.clone());
        }
        if let (Some(interval), Some(timeout)) = (self.keep_alive_interval, self.keep_alive_timeout)
        {
            options = options.with_keep_alive(interval, timeout);
        }
        if let Some(timeout) = self.connect_timeout {
            options = options.with_connect_timeout(timeout);
        }
        if let Some(timeout) = self.request_timeout {
            options = options.with_timeout(timeout);
        }
        options = options.with_keep_alive_while_idle(self.keep_alive_while_idle);
        if self.tls_enabled() {
            let mut tls = etcd_client::TlsOptions::new().with_enabled_roots();
            if let Some(certificate) = &self.ca_certificate {
                tls = tls.ca_certificate(etcd_client::Certificate::from_pem(certificate));
            }
            if let (Some(certificate), Some(key)) = (&self.client_certificate, &self.client_key) {
                tls = tls.identity(etcd_client::Identity::from_pem(certificate, key));
            }
            if let Some(domain_name) = &self.tls_domain_name {
                tls = tls.domain_name(domain_name);
            }
            options = options.with_tls(tls);
        }
        Ok(options)
    }
}

pub use metadata::{CasResult, MetadataEntry};
pub use session::{EtcdParticipantSession, RegistrationOptions};
pub(crate) use watch::is_lease_backed_delete;
#[cfg(all(test, feature = "shuttle"))]
pub(crate) use watch::WatchCursor;
pub use watch::{
    KeyValueSnapshot, WatchError, WatchEvent, WatchEventKind, WatchRecovery, WatchSubscription,
};
pub(crate) const INTERNAL_SESSION_SEQUENCE: &str = "internal/session-sequence";
pub(crate) const AUTHORITATIVE_INPUT_MARKER: &str = "internal/authoritative-input";

pub(crate) fn is_authoritative_metadata_key(key: &str) -> bool {
    key != AUTHORITATIVE_INPUT_MARKER
        && !key.starts_with("controller/output/")
        && !(key.starts_with("participant/") && key.ends_with("/processed-revision"))
}

// A namespace snapshot and its corresponding watch event can contain the
// complete published state for a large cluster. Keep this bounded while
// leaving enough room for the documented scale profiles.
const MAX_ETCD_DECODING_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// A key-value-store revision, distinct from a participant SessionId.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Revision(i64);

impl Revision {
    /// Construct a positive revision for an explicit historical read/watch.
    pub fn new(value: i64) -> Result<Self, CoordinationError> {
        if value <= 0 {
            return Err(CoordinationError::InvalidRevision(value));
        }
        Ok(Self(value))
    }

    /// Return the etcd revision value.
    pub const fn value(self) -> i64 {
        self.0
    }

    /// Return the next revision, or an error if the revision space is exhausted.
    pub fn next(self) -> Result<Self, CoordinationError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(CoordinationError::RevisionExhausted)
    }
}

/// Internal participant completion identity shared by the runtime and its
/// deterministic concurrency model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ParticipantCompletionFence {
    pub(crate) instance: InstanceId,
    pub(crate) session_id: SessionId,
    pub(crate) queue_revision: Revision,
    pub(crate) message_id: String,
}

impl ParticipantCompletionFence {
    pub(crate) fn new(
        instance: InstanceId,
        session_id: SessionId,
        queue_revision: Revision,
        message_id: String,
    ) -> Self {
        Self {
            instance,
            session_id,
            queue_revision,
            message_id,
        }
    }

    #[cfg(all(test, feature = "shuttle"))]
    pub(crate) fn matches(&self, active_session: SessionId, pending_message: bool) -> bool {
        self.session_id == active_session && pending_message
    }
}

/// One authoritative etcd snapshot used by the controller runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinationSnapshot {
    revision: Revision,
    authoritative_revision: Revision,
    metadata: BTreeMap<String, MetadataEntry>,
    participants: ParticipantSessionSnapshot,
    controller_election: ControllerElectionSnapshot,
}

/// Controller election records observed at one etcd revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerElectionSnapshot {
    active: Option<(String, i64)>,
    candidates: BTreeMap<String, i64>,
}

impl ControllerElectionSnapshot {
    /// Return the active controller record, including its lease id.
    pub fn active(&self) -> Option<(&str, i64)> {
        self.active
            .as_ref()
            .map(|(controller, lease)| (controller.as_str(), *lease))
    }

    /// Return candidate controller records and their lease ids.
    pub fn candidates(&self) -> &BTreeMap<String, i64> {
        &self.candidates
    }
}

impl CoordinationSnapshot {
    /// Return the etcd revision represented by this snapshot.
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Return the newest revision of authoritative, non-derived input data.
    pub const fn authoritative_revision(&self) -> Revision {
        self.authoritative_revision
    }

    pub(crate) fn with_authoritative_revision(mut self, revision: Revision) -> Self {
        self.authoritative_revision = revision;
        self
    }

    /// Return persistent metadata and their modification revisions.
    pub fn metadata(&self) -> &BTreeMap<String, MetadataEntry> {
        &self.metadata
    }

    /// Return the active participant/session view.
    pub fn participants(&self) -> &ParticipantSessionSnapshot {
        &self.participants
    }

    /// Return controller election records from this same etcd revision.
    pub fn controller_election(&self) -> &ControllerElectionSnapshot {
        &self.controller_election
    }

    #[cfg(test)]
    pub(crate) fn from_parts(
        revision: Revision,
        metadata: BTreeMap<String, MetadataEntry>,
        participants: ParticipantSessionSnapshot,
    ) -> Self {
        Self {
            revision,
            authoritative_revision: revision,
            metadata,
            participants,
            controller_election: ControllerElectionSnapshot {
                active: None,
                candidates: BTreeMap::new(),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn with_controller_election(
        mut self,
        active: Option<(String, i64)>,
        candidates: BTreeMap<String, i64>,
    ) -> Self {
        self.controller_election = ControllerElectionSnapshot { active, candidates };
        self
    }
}

/// An etcd-backed coordination namespace.
#[derive(Clone)]
pub struct EtcdCoordination {
    client: Client,
    prefix: String,
    cluster: String,
}

impl fmt::Debug for EtcdCoordination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EtcdCoordination")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl EtcdCoordination {
    /// Connect to one or more etcd endpoints using an isolated key prefix.
    pub async fn connect(config: EtcdCoordinationConfig) -> Result<Self, CoordinationError> {
        Self::connect_with_options(config, EtcdConnectionOptions::new()).await
    }

    /// Connect with explicit transport, TLS, and authentication settings.
    pub async fn connect_with_options(
        config: EtcdCoordinationConfig,
        options: EtcdConnectionOptions,
    ) -> Result<Self, CoordinationError> {
        Self::connect_endpoints_with_options(
            [config.endpoint],
            config.prefix,
            config.cluster,
            options,
        )
        .await
    }

    /// Connect using an ordered set of etcd endpoints for client failover.
    pub async fn connect_endpoints<I, S>(
        endpoints: I,
        prefix: String,
        cluster: String,
    ) -> Result<Self, CoordinationError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::connect_endpoints_with_options(
            endpoints,
            prefix,
            cluster,
            EtcdConnectionOptions::new(),
        )
        .await
    }

    /// Connect using multiple endpoints and explicit transport options.
    pub async fn connect_endpoints_with_options<I, S>(
        endpoints: I,
        prefix: String,
        cluster: String,
        options: EtcdConnectionOptions,
    ) -> Result<Self, CoordinationError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let prefix = normalize_prefix(prefix)?;
        if cluster.trim().is_empty() || cluster.as_bytes().contains(&0) {
            return Err(CoordinationError::InvalidCluster);
        }
        let endpoints: Vec<String> = endpoints
            .into_iter()
            .map(|endpoint| endpoint.as_ref().to_owned())
            .collect();
        if endpoints.is_empty() || endpoints.iter().any(|endpoint| endpoint.trim().is_empty()) {
            return Err(CoordinationError::InvalidValue);
        }
        let client = Client::connect(&endpoints, Some(options.to_connect_options()?)).await?;
        let coordination = Self {
            client,
            prefix,
            cluster,
        };
        metadata::initialize_authoritative_input_marker(&coordination).await?;
        Ok(coordination)
    }

    /// Return the logical cluster name associated with this connection.
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// Register an instance and create its lease-backed LiveInstance.
    pub async fn register(
        &self,
        instance_id: InstanceId,
        options: RegistrationOptions,
    ) -> Result<EtcdParticipantSession, CoordinationError> {
        session::register(self, instance_id, options).await
    }

    /// Read the currently live session for an instance.
    pub async fn live_session(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<SessionId>, CoordinationError> {
        session::live_session(self, instance_id).await
    }

    /// Build the active M8 participant/session view from one etcd snapshot.
    pub async fn participant_snapshot(
        &self,
    ) -> Result<ParticipantSessionSnapshot, CoordinationError> {
        session::participant_snapshot(self).await
    }

    /// Read one CurrentState value without scanning the coordination
    /// namespace. Participant transition workers use this targeted lookup so
    /// large clusters do not turn every callback into a full-cluster read.
    pub(crate) async fn current_state(
        &self,
        instance_id: &InstanceId,
        session_id: SessionId,
        resource: &ResourceId,
        partition: &PartitionId,
    ) -> Result<Option<State>, CoordinationError> {
        let (_, key_values) = self
            .raw_snapshot(
                self.current_state_key(instance_id, session_id, resource, partition)
                    .into_bytes(),
                None,
            )
            .await?;
        let Some(key_value) = key_values.first() else {
            return Ok(None);
        };
        State::try_from(
            std::str::from_utf8(key_value.value()).map_err(|_| CoordinationError::InvalidValue)?,
        )
        .map(Some)
        .map_err(|_| CoordinationError::InvalidValue)
    }

    /// Take one authoritative snapshot of metadata, liveness, and CurrentState.
    pub async fn controller_snapshot(&self) -> Result<CoordinationSnapshot, CoordinationError> {
        let (revision, kvs) = self
            .raw_snapshot(
                self.namespaced_key("").as_bytes().to_vec(),
                Some(etcd_client::GetOptions::new().with_prefix()),
            )
            .await?;
        crate::failpoints::controlled_error("coordination_after_snapshot_before_watch")
            .map_err(|_| CoordinationError::InvalidValue)?;
        self.controller_snapshot_from_kvs(revision, &kvs)
    }

    /// Take an authoritative snapshot as it existed at one MVCC revision.
    pub async fn controller_snapshot_at_revision(
        &self,
        revision: Revision,
    ) -> Result<CoordinationSnapshot, CoordinationError> {
        let (_, kvs) = self
            .raw_snapshot(
                self.namespaced_key("").as_bytes().to_vec(),
                Some(
                    etcd_client::GetOptions::new()
                        .with_prefix()
                        .with_revision(revision.value()),
                ),
            )
            .await?;
        self.controller_snapshot_from_kvs(revision, &kvs)
    }

    fn controller_snapshot_from_kvs(
        &self,
        revision: Revision,
        kvs: &[etcd_client::KeyValue],
    ) -> Result<CoordinationSnapshot, CoordinationError> {
        let metadata = metadata_from_snapshot(self, kvs)?;
        let authoritative_revision = authoritative_revision(self, revision, kvs)?;
        Ok(CoordinationSnapshot {
            revision,
            authoritative_revision,
            metadata,
            participants: session::participant_snapshot_from_kvs(self, kvs)?,
            controller_election: controller_election_from_kvs(self, kvs)?,
        })
    }

    /// Begin watching every key in this coordination namespace.
    pub async fn watch_namespace_from(
        &self,
        start_revision: Revision,
    ) -> Result<WatchSubscription, CoordinationError> {
        watch::watch_namespace(self, start_revision).await
    }

    /// Take an authoritative snapshot and begin watching after it.
    pub async fn snapshot_and_watch_namespace(
        &self,
    ) -> Result<(CoordinationSnapshot, WatchSubscription), CoordinationError> {
        let snapshot = self.controller_snapshot().await?;
        let watch = self.watch_namespace_from(snapshot.revision.next()?).await?;
        Ok((snapshot, watch))
    }

    pub(crate) async fn snapshot_and_watch_namespace_from_authoritative(
        &self,
    ) -> Result<(CoordinationSnapshot, WatchSubscription), CoordinationError> {
        let snapshot = self.controller_snapshot().await?;
        let watch = self
            .watch_namespace_from(snapshot.authoritative_revision.next()?)
            .await?;
        Ok((snapshot, watch))
    }

    /// Persist a supplied historical session record without granting it authority.
    pub async fn inject_session_current_state(
        &self,
        instance_id: &InstanceId,
        session_id: SessionId,
        resource_id: ResourceId,
        states: BTreeMap<PartitionId, State>,
    ) -> Result<(), CoordinationError> {
        session::inject_session_current_state(self, instance_id, session_id, resource_id, states)
            .await
    }

    /// Publish CurrentState for a known session using the backend fence.
    pub async fn publish_current_state_for_session(
        &self,
        instance_id: &InstanceId,
        session_id: SessionId,
        resource_id: ResourceId,
        states: BTreeMap<PartitionId, State>,
    ) -> Result<Revision, CoordinationError> {
        session::publish_current_state(self, instance_id, session_id, resource_id, states).await
    }

    /// Revoke the currently live session for an instance after checking its identity.
    pub async fn revoke_live(
        &self,
        instance_id: &InstanceId,
        session_id: SessionId,
    ) -> Result<Revision, CoordinationError> {
        session::revoke_live(self, instance_id, session_id).await
    }

    /// Remove session-owned CurrentState entries for a deleted resource.
    pub async fn remove_current_states_for_resource(
        &self,
        resource: &ResourceId,
    ) -> Result<Revision, CoordinationError> {
        session::remove_current_states_for_resource(self, resource).await
    }

    /// Read one persistent metadata entry and its etcd modification revision.
    pub async fn get_metadata(
        &self,
        key: &str,
    ) -> Result<Option<MetadataEntry>, CoordinationError> {
        metadata::get(self, key, None).await
    }

    /// Write one persistent metadata entry.
    pub async fn put_metadata(
        &self,
        key: &str,
        value: &str,
    ) -> Result<Revision, CoordinationError> {
        metadata::put(self, key, value, None).await
    }

    /// Compare the current modification revision and update if it still matches.
    pub async fn compare_and_put_metadata(
        &self,
        key: &str,
        expected_revision: Revision,
        value: &str,
    ) -> Result<CasResult, CoordinationError> {
        metadata::compare_and_put(self, key, expected_revision, value).await
    }

    pub(crate) async fn compare_and_put_metadata_if_live_absent(
        &self,
        key: &str,
        expected_revision: Revision,
        instance: &InstanceId,
        value: &str,
    ) -> Result<CasResult, CoordinationError> {
        metadata::compare_and_put_if_live_absent(self, key, expected_revision, instance, value)
            .await
    }

    /// Compare the current modification revision and delete the metadata key.
    pub async fn compare_and_delete_metadata(
        &self,
        key: &str,
        expected_revision: Revision,
    ) -> Result<CasResult, CoordinationError> {
        metadata::compare_and_delete(self, key, expected_revision).await
    }

    /// Put metadata only when the key does not already exist.
    pub async fn put_metadata_if_absent(
        &self,
        key: &str,
        value: &str,
    ) -> Result<bool, CoordinationError> {
        metadata::compare_and_put_absent(self, key, value).await
    }

    /// Advance a numeric metadata watermark without allowing it to regress.
    pub async fn advance_metadata_revision(
        &self,
        key: &str,
        revision: Revision,
    ) -> Result<Revision, CoordinationError> {
        for _ in 0..MAX_QUEUE_CAS_RETRIES {
            let value = revision.value().to_string();
            let Some(entry) = self.get_metadata(key).await? else {
                if metadata::compare_and_put_absent(self, key, &value).await? {
                    return Ok(revision);
                }
                continue;
            };
            let current = entry
                .value()
                .ok_or(CoordinationError::InvalidValue)?
                .parse::<i64>()
                .map_err(|_| CoordinationError::InvalidValue)?;
            let current = Revision::new(current)?;
            if current >= revision {
                return Ok(current);
            }
            if self
                .compare_and_put_metadata(key, entry.revision(), &value)
                .await?
                .applied()
            {
                return Ok(revision);
            }
        }
        Err(CoordinationError::Contention)
    }

    /// Take a gap-free snapshot and watch of the controller's transition queue.
    pub async fn snapshot_and_watch_pending_transitions(
        &self,
    ) -> Result<(Vec<TransitionMessage>, Revision, WatchSubscription), CoordinationError> {
        let (entry, watch) = self
            .snapshot_and_watch_metadata(PENDING_TRANSITIONS_KEY)
            .await?;
        let messages = parse_pending_messages(entry.value())?;
        Ok((messages, entry.revision(), watch))
    }

    /// Append a message to the shared M10 transition queue with a CAS loop.
    pub async fn inject_pending_transition(
        &self,
        message: &TransitionMessage,
    ) -> Result<Revision, CoordinationError> {
        if message.message_id.is_empty() {
            return Err(CoordinationError::InvalidValue);
        }
        for _ in 0..MAX_QUEUE_CAS_RETRIES {
            let entry = self.get_metadata(PENDING_TRANSITIONS_KEY).await?;
            let (expected, mut messages, exists) = match entry.as_ref() {
                Some(entry) => (
                    entry.revision(),
                    parse_pending_messages(entry.value())?,
                    true,
                ),
                None => (
                    self.controller_snapshot().await?.revision(),
                    Vec::new(),
                    false,
                ),
            };
            if messages
                .iter()
                .any(|candidate| candidate.message_id == message.message_id)
            {
                return Err(CoordinationError::DuplicateMessage(
                    message.message_id.clone(),
                ));
            }
            messages.push(message.clone());
            let value =
                serde_json::to_string(&messages).map_err(|_| CoordinationError::InvalidValue)?;
            let applied = if exists {
                self.compare_and_put_metadata(PENDING_TRANSITIONS_KEY, expected, &value)
                    .await?
                    .applied()
            } else {
                metadata::compare_and_put_absent(self, PENDING_TRANSITIONS_KEY, &value).await?
            };
            if applied {
                return self
                    .get_metadata(PENDING_TRANSITIONS_KEY)
                    .await?
                    .map(|entry| entry.revision())
                    .ok_or(CoordinationError::MissingRevision);
            }
        }
        Err(CoordinationError::Contention)
    }

    /// Claim a queued message at one etcd linearization point before an
    /// application callback is allowed to run.
    pub(crate) async fn claim_pending_transition(
        &self,
        instance: &InstanceId,
        session_id: SessionId,
        message: &TransitionMessage,
    ) -> Result<TransitionClaim, CoordinationError> {
        let client = self.client();
        let queue_key = self.metadata_key(PENDING_TRANSITIONS_KEY);
        let live_key = self.live_key(instance);
        for _ in 0..MAX_QUEUE_CAS_RETRIES {
            let Some(entry) = self.get_metadata(PENDING_TRANSITIONS_KEY).await? else {
                return Ok(TransitionClaim::Withdrawn);
            };
            let messages = parse_pending_messages(entry.value())?;
            if !messages.iter().any(|candidate| candidate == message) {
                return Ok(TransitionClaim::Withdrawn);
            }
            let queue_value = entry.value().ok_or(CoordinationError::InvalidValue)?;
            let response = client
                .kv_client()
                .txn(
                    etcd_client::Txn::new()
                        .when([
                            etcd_client::Compare::value(
                                live_key.clone(),
                                etcd_client::CompareOp::Equal,
                                session_id.wire_value().to_string(),
                            ),
                            etcd_client::Compare::mod_revision(
                                queue_key.clone(),
                                etcd_client::CompareOp::Equal,
                                entry.revision().value(),
                            ),
                            etcd_client::Compare::value(
                                queue_key.clone(),
                                etcd_client::CompareOp::Equal,
                                queue_value,
                            ),
                        ])
                        .and_then(Vec::<etcd_client::TxnOp>::new()),
                )
                .await?;
            if response.succeeded() {
                return Ok(TransitionClaim::Claimed(entry.revision()));
            }
            if self.live_session(instance).await? != Some(session_id) {
                return Ok(TransitionClaim::SessionLost);
            }
            // A failed compare may only mean that another queue entry was
            // appended/removed. Re-read the complete queue before retrying so
            // unrelated queue revisions do not invalidate this message.
        }
        Err(CoordinationError::Contention)
    }

    /// Complete one message and, when still authoritative, publish its state.
    ///
    /// The queue's modification revision must still equal `expected_revision`.
    pub async fn complete_pending_transition(
        &self,
        instance_id: &InstanceId,
        session_id: SessionId,
        expected_revision: Revision,
        message: &TransitionMessage,
        resulting_state: Option<&State>,
    ) -> Result<CompletionResult, CoordinationError> {
        let fence = ParticipantCompletionFence::new(
            instance_id.clone(),
            session_id,
            expected_revision,
            message.message_id.clone(),
        );
        self.complete_pending_transition_with_fence(&fence, message, resulting_state)
            .await
    }

    pub(crate) async fn complete_pending_transition_with_fence(
        &self,
        fence: &ParticipantCompletionFence,
        message: &TransitionMessage,
        resulting_state: Option<&State>,
    ) -> Result<CompletionResult, CoordinationError> {
        let client = self.client();
        let queue_key = self.metadata_key(PENDING_TRANSITIONS_KEY);
        let live_key = self.live_key(&fence.instance);
        let resource = ResourceId::try_from(message.resource.as_str())
            .map_err(|_| CoordinationError::InvalidKey)?;
        let partition = PartitionId::try_from(message.partition.as_str())
            .map_err(|_| CoordinationError::InvalidKey)?;
        let Some(entry) = self.get_metadata(PENDING_TRANSITIONS_KEY).await? else {
            return Ok(CompletionResult::AlreadyCompleted);
        };
        let mut messages = parse_pending_messages(entry.value())?;
        let Some(index) = messages
            .iter()
            .position(|candidate| candidate.message_id == fence.message_id)
        else {
            return Ok(CompletionResult::AlreadyCompleted);
        };
        if messages[index] != *message {
            return Ok(CompletionResult::AlreadyCompleted);
        }
        if entry.revision() != fence.queue_revision {
            return Err(CoordinationError::StaleRevision);
        }
        messages.remove(index);
        let queue_value =
            serde_json::to_string(&messages).map_err(|_| CoordinationError::InvalidValue)?;
        let mut operations = Vec::with_capacity(2);
        if let Some(state) = resulting_state {
            operations.push(etcd_client::TxnOp::put(
                self.current_state_key(&fence.instance, fence.session_id, &resource, &partition),
                state.as_str(),
                None,
            ));
        } else {
            operations.push(etcd_client::TxnOp::delete(
                self.current_state_key(&fence.instance, fence.session_id, &resource, &partition),
                None,
            ));
        }
        operations.push(etcd_client::TxnOp::put(
            queue_key.clone(),
            queue_value,
            None,
        ));
        operations.push(self.authoritative_input_marker_operation());
        let response = client
            .kv_client()
            .txn(
                etcd_client::Txn::new()
                    .when([
                        etcd_client::Compare::value(
                            live_key.clone(),
                            etcd_client::CompareOp::Equal,
                            fence.session_id.wire_value().to_string(),
                        ),
                        etcd_client::Compare::mod_revision(
                            self.metadata_key(PENDING_TRANSITIONS_KEY),
                            etcd_client::CompareOp::Equal,
                            fence.queue_revision.value(),
                        ),
                    ])
                    .and_then(operations),
            )
            .await?;
        if response.succeeded() {
            crate::failpoints::hard_abort("participant_after_completion_txn");
            return Ok(CompletionResult::Applied(revision_from_header(
                response.header(),
            )?));
        }

        if self.live_session(&fence.instance).await? == Some(fence.session_id) {
            return Err(CoordinationError::StaleRevision);
        }

        // Helix's task cleanup removes an in-flight message even if the
        // session changed, but its old callback cannot publish CurrentState.
        let Some(entry) = self.get_metadata(PENDING_TRANSITIONS_KEY).await? else {
            return Ok(CompletionResult::SessionLost);
        };
        let mut current = parse_pending_messages(entry.value())?;
        let before = current.len();
        current.retain(|candidate| candidate.message_id != fence.message_id);
        if current.len() == before {
            return Ok(CompletionResult::AlreadyCompleted);
        }
        if entry.revision() != fence.queue_revision {
            return Ok(CompletionResult::SessionLost);
        }
        let value = serde_json::to_string(&current).map_err(|_| CoordinationError::InvalidValue)?;
        if self
            .compare_and_put_metadata(PENDING_TRANSITIONS_KEY, fence.queue_revision, &value)
            .await?
            .applied()
        {
            return Ok(CompletionResult::SessionLost);
        }
        Ok(CompletionResult::SessionLost)
    }

    /// Remove one queue message with an exact revision guard, without touching state.
    ///
    /// A present message is removed only when the queue's modification revision
    /// equals `expected_revision`; otherwise `StaleRevision` is returned.
    pub async fn remove_pending_transition(
        &self,
        expected_revision: Revision,
        message_id: &str,
    ) -> Result<bool, CoordinationError> {
        let Some(entry) = self.get_metadata(PENDING_TRANSITIONS_KEY).await? else {
            return Ok(false);
        };
        let mut messages = parse_pending_messages(entry.value())?;
        let before = messages.len();
        messages.retain(|message| message.message_id != message_id);
        if messages.len() == before {
            return Ok(false);
        }
        if entry.revision() != expected_revision {
            return Err(CoordinationError::StaleRevision);
        }
        let value =
            serde_json::to_string(&messages).map_err(|_| CoordinationError::InvalidValue)?;
        if self
            .compare_and_put_metadata(PENDING_TRANSITIONS_KEY, expected_revision, &value)
            .await?
            .applied()
        {
            return Ok(true);
        }
        Err(CoordinationError::StaleRevision)
    }

    /// Remove queued transitions that target a resource deleted from metadata.
    pub async fn remove_pending_transitions_for_resource(
        &self,
        resource: &ResourceId,
    ) -> Result<(), CoordinationError> {
        for _ in 0..MAX_QUEUE_CAS_RETRIES {
            let Some(entry) = self.get_metadata(PENDING_TRANSITIONS_KEY).await? else {
                return Ok(());
            };
            let messages = parse_pending_messages(entry.value())?;
            let before = messages.len();
            let retained = messages
                .into_iter()
                .filter(|message| message.resource != resource.as_str())
                .collect::<Vec<_>>();
            if retained.len() == before {
                return Ok(());
            }
            let value =
                serde_json::to_string(&retained).map_err(|_| CoordinationError::InvalidValue)?;
            if self
                .compare_and_put_metadata(PENDING_TRANSITIONS_KEY, entry.revision(), &value)
                .await?
                .applied()
            {
                return Ok(());
            }
        }
        Err(CoordinationError::Contention)
    }

    /// Atomically publish controller outputs and the input revision they represent.
    pub async fn publish_controller_outputs(
        &self,
        authority: &crate::election::ControllerAuthority,
        external_view: &str,
        pending_transitions: &str,
        processed_revision: Revision,
        pending_revision: Option<Revision>,
    ) -> Result<Revision, CoordinationError> {
        let snapshot = self.controller_snapshot().await?;
        if processed_revision != snapshot.authoritative_revision() {
            return Err(CoordinationError::StaleRevision);
        }
        let live_sessions = snapshot
            .participants()
            .live_instances()
            .iter()
            .map(|(instance, live)| (instance.clone(), live.session_id()))
            .collect();
        let fence = authority.commit_fence(snapshot.authoritative_revision(), live_sessions);
        self.publish_controller_outputs_with_fence(
            &fence,
            external_view,
            pending_transitions,
            pending_revision,
        )
        .await
    }

    pub(crate) async fn publish_controller_outputs_with_fence(
        &self,
        fence: &crate::election::ControllerCommitFence,
        external_view: &str,
        pending_transitions: &str,
        pending_revision: Option<Revision>,
    ) -> Result<Revision, CoordinationError> {
        metadata::publish_controller_outputs(
            self,
            fence,
            external_view,
            pending_transitions,
            pending_revision,
        )
        .await
    }

    pub(crate) async fn record_lease_expiry(
        &self,
        authority: &crate::election::ControllerAuthority,
        event_revision: Revision,
    ) -> Result<Revision, CoordinationError> {
        metadata::record_lease_expiry(self, authority, event_revision).await
    }

    /// Write one controller-owned value behind the authoritative etcd fence.
    pub async fn put_controller_owned(
        &self,
        authority: &crate::election::ControllerAuthority,
        key: &str,
        value: impl Into<Vec<u8>>,
    ) -> Result<Revision, CoordinationError> {
        crate::election::put_controller_owned(self, authority, key, value.into()).await
    }

    /// Compact the etcd event history through a known revision.
    pub async fn compact(&self, revision: Revision) -> Result<(), CoordinationError> {
        let client = self.client.clone();
        client.kv_client().compact(revision.value(), None).await?;
        Ok(())
    }

    /// Take an authoritative metadata snapshot and begin watching after it.
    pub async fn snapshot_and_watch_metadata(
        &self,
        key: &str,
    ) -> Result<(MetadataEntry, WatchSubscription), CoordinationError> {
        let (snapshot_revision, kvs) = metadata::snapshot(self, key).await?;
        let entry = metadata_entry_from_snapshot(snapshot_revision, &kvs)?;
        let watch = self
            .watch_metadata_from(key, snapshot_revision.next()?)
            .await?;
        Ok((entry, watch))
    }

    /// Begin watching one metadata key from an inclusive revision.
    pub async fn watch_metadata_from(
        &self,
        key: &str,
        start_revision: Revision,
    ) -> Result<WatchSubscription, CoordinationError> {
        watch::watch_metadata(self, key, start_revision).await
    }

    pub(crate) fn metadata_key(&self, key: &str) -> String {
        self.namespaced_key(&format!("metadata/{}", encode_segment(key)))
    }

    pub(crate) fn namespaced_key(&self, suffix: &str) -> String {
        format!("{}/{}", self.prefix, suffix)
    }

    pub(crate) fn client(&self) -> Client {
        self.client.clone()
    }

    pub(crate) fn kv_client(&self) -> etcd_client::KvClient {
        self.client
            .kv_client()
            .max_decoding_message_size(MAX_ETCD_DECODING_MESSAGE_SIZE)
    }

    pub(crate) fn watch_client(&self) -> etcd_client::WatchClient {
        self.client
            .watch_client()
            .max_decoding_message_size(MAX_ETCD_DECODING_MESSAGE_SIZE)
    }

    pub(crate) fn controller_active_key(&self) -> String {
        self.namespaced_key("controller/election/active")
    }

    pub(crate) fn authoritative_input_marker_operation(&self) -> etcd_client::TxnOp {
        etcd_client::TxnOp::put(self.metadata_key(AUTHORITATIVE_INPUT_MARKER), "1", None)
    }

    pub(crate) fn controller_candidate_key(&self, controller_id: &str) -> String {
        self.namespaced_key(&format!(
            "controller/election/candidates/{}",
            encode_segment(controller_id)
        ))
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.prefix
    }

    pub(crate) async fn raw_snapshot(
        &self,
        key: Vec<u8>,
        options: Option<etcd_client::GetOptions>,
    ) -> Result<(Revision, Vec<etcd_client::KeyValue>), CoordinationError> {
        let response = self.kv_client().get(key, options).await?;
        let revision = response
            .header()
            .map(|header| header.revision())
            .ok_or(CoordinationError::MissingRevision)?;
        Ok((Revision::new(revision)?, response.kvs().to_vec()))
    }

    pub(crate) fn parse_relative_key<'a>(
        &self,
        key: &'a [u8],
    ) -> Result<&'a str, CoordinationError> {
        let key = std::str::from_utf8(key).map_err(|_| CoordinationError::InvalidKey)?;
        let prefix = format!("{}/", self.prefix);
        key.strip_prefix(&prefix)
            .ok_or(CoordinationError::OutsidePrefix)
    }
}

fn metadata_from_snapshot(
    backend: &EtcdCoordination,
    key_values: &[etcd_client::KeyValue],
) -> Result<BTreeMap<String, MetadataEntry>, CoordinationError> {
    let mut metadata = BTreeMap::new();
    for key_value in key_values {
        let relative_key = backend.parse_relative_key(key_value.key())?;
        let Some(encoded_key) = relative_key.strip_prefix("metadata/") else {
            continue;
        };
        let key = decode_segment(encoded_key)?;
        let value = String::from_utf8(key_value.value().to_vec())
            .map_err(|_| CoordinationError::InvalidValue)?;
        metadata.insert(
            key,
            MetadataEntry {
                value: Some(value),
                revision: Revision::new(key_value.mod_revision())?,
            },
        );
    }
    Ok(metadata)
}

fn controller_election_from_kvs(
    backend: &EtcdCoordination,
    key_values: &[etcd_client::KeyValue],
) -> Result<ControllerElectionSnapshot, CoordinationError> {
    let mut active = None;
    let mut candidates = BTreeMap::new();
    for key_value in key_values {
        let relative = backend.parse_relative_key(key_value.key())?;
        if relative == "controller/election/active" {
            let controller = String::from_utf8(key_value.value().to_vec())
                .map_err(|_| CoordinationError::InvalidValue)?;
            if controller.trim().is_empty() || key_value.lease() == 0 {
                return Err(CoordinationError::InvalidValue);
            }
            active = Some((controller, key_value.lease()));
        } else if let Some(encoded) = relative.strip_prefix("controller/election/candidates/") {
            let controller = decode_segment(encoded)?;
            if controller.trim().is_empty() || key_value.lease() == 0 {
                return Err(CoordinationError::InvalidValue);
            }
            candidates.insert(controller, key_value.lease());
        }
    }
    Ok(ControllerElectionSnapshot { active, candidates })
}

fn authoritative_revision(
    backend: &EtcdCoordination,
    snapshot_revision: Revision,
    key_values: &[etcd_client::KeyValue],
) -> Result<Revision, CoordinationError> {
    let mut revision = None;
    for key_value in key_values {
        let relative = backend.parse_relative_key(key_value.key())?;
        let metadata_key = relative
            .strip_prefix("metadata/")
            .and_then(|encoded| decode_segment(encoded).ok());
        if is_authoritative_key(relative, metadata_key.as_deref()) {
            let current = Revision::new(key_value.mod_revision())?;
            revision = Some(revision.map_or(current, |previous: Revision| previous.max(current)));
        }
    }
    Ok(revision.unwrap_or(snapshot_revision))
}

fn is_authoritative_key(relative: &str, metadata_key: Option<&str>) -> bool {
    let is_derived = metadata_key.is_some_and(|key| key.starts_with("controller/output/"));
    let is_participant_progress = metadata_key
        .is_some_and(|key| key.starts_with("participant/") && key.ends_with("/processed-revision"));
    let is_election_record = relative.starts_with("controller/election/");
    let is_session_sequence = relative == INTERNAL_SESSION_SEQUENCE;
    !is_derived && !is_participant_progress && !is_election_record && !is_session_sequence
}

fn metadata_entry_from_snapshot(
    snapshot_revision: Revision,
    key_values: &[etcd_client::KeyValue],
) -> Result<MetadataEntry, CoordinationError> {
    let Some(key_value) = key_values.first() else {
        return Ok(MetadataEntry::empty(snapshot_revision));
    };
    let value = String::from_utf8(key_value.value().to_vec())
        .map_err(|_| CoordinationError::InvalidValue)?;
    Ok(MetadataEntry {
        value: Some(value),
        revision: Revision::new(key_value.mod_revision())?,
    })
}

/// Errors returned by the etcd coordination backend.
#[derive(Debug)]
pub enum CoordinationError {
    Etcd(Box<etcd_client::Error>),
    InvalidConnectionOptions(String),
    InvalidPrefix,
    InvalidCluster,
    InvalidKey,
    InvalidValue,
    InvalidLeaseTtl,
    LeaseExpired,
    InvalidRevision(i64),
    RevisionExhausted,
    MissingRevision,
    OutsidePrefix,
    InvalidSession,
    UnknownSession(SessionId),
    RegistrationLost,
    Contention,
    StaleSession(InstanceId),
    DuplicateMessage(String),
    StaleController,
    StaleRevision,
    InstanceStillLive(InstanceId),
    ResourceDeletionDisabled,
}

impl CoordinationError {
    /// Whether retrying the operation may succeed after the backend returns.
    pub const fn is_transient(&self) -> bool {
        matches!(self, Self::Etcd(_))
    }
}

impl fmt::Display for CoordinationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Etcd(error) => error.fmt(formatter),
            Self::InvalidConnectionOptions(message) => formatter.write_str(message),
            Self::InvalidPrefix => formatter.write_str("coordination prefix must not be empty"),
            Self::InvalidCluster => formatter.write_str("coordination cluster must not be empty"),
            Self::InvalidKey => formatter.write_str("coordination key is invalid"),
            Self::InvalidValue => formatter.write_str("coordination value is invalid UTF-8"),
            Self::InvalidLeaseTtl => formatter.write_str("lease TTL must be positive"),
            Self::LeaseExpired => formatter.write_str("lease expired before keepalive completed"),
            Self::InvalidRevision(revision) => write!(formatter, "invalid revision {revision}"),
            Self::RevisionExhausted => formatter.write_str("revision space exhausted"),
            Self::MissingRevision => {
                formatter.write_str("etcd response did not include a revision")
            }
            Self::OutsidePrefix => formatter.write_str("key is outside the coordination prefix"),
            Self::InvalidSession => formatter.write_str("invalid persisted session identifier"),
            Self::UnknownSession(session) => write!(formatter, "unknown session {session:?}"),
            Self::RegistrationLost => formatter.write_str("participant registration lost the race"),
            Self::Contention => {
                formatter.write_str("coordination CAS contention exceeded retry bound")
            }
            Self::StaleSession(instance) => write!(formatter, "session is not live for {instance}"),
            Self::DuplicateMessage(message) => write!(formatter, "duplicate message: {message}"),
            Self::StaleController => {
                formatter.write_str("controller authority is no longer current")
            }
            Self::StaleRevision => formatter.write_str("controller input revision is stale"),
            Self::InstanceStillLive(instance) => {
                write!(formatter, "instance is still live: {instance}")
            }
            Self::ResourceDeletionDisabled => formatter.write_str("resource deletion is disabled"),
        }
    }
}

pub const PENDING_TRANSITIONS_KEY: &str = "controller/output/pending-transitions";
const MAX_QUEUE_CAS_RETRIES: usize = 256;

/// Result of the session-fenced message/state completion transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionResult {
    Applied(Revision),
    SessionLost,
    AlreadyCompleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransitionClaim {
    Claimed(Revision),
    SessionLost,
    Withdrawn,
}

fn parse_pending_messages(
    value: Option<&str>,
) -> Result<Vec<TransitionMessage>, CoordinationError> {
    let messages: Vec<TransitionMessage> = value
        .map(|value| serde_json::from_str(value).map_err(|_| CoordinationError::InvalidValue))
        .transpose()
        .map(|messages| messages.unwrap_or_default())?;
    if messages.iter().any(|message| message.message_id.is_empty()) {
        return Err(CoordinationError::InvalidValue);
    }
    Ok(messages)
}

impl std::error::Error for CoordinationError {}

impl From<etcd_client::Error> for CoordinationError {
    fn from(error: etcd_client::Error) -> Self {
        Self::Etcd(Box::new(error))
    }
}

fn normalize_prefix(prefix: String) -> Result<String, CoordinationError> {
    let prefix = prefix.trim_end_matches('/').to_owned();
    if prefix.is_empty() || prefix.as_bytes().contains(&0) {
        return Err(CoordinationError::InvalidPrefix);
    }
    Ok(prefix)
}

pub(crate) fn encode_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

pub(crate) fn decode_segment(value: &str) -> Result<String, CoordinationError> {
    if value.len() % 2 != 0 {
        return Err(CoordinationError::InvalidKey);
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks(2) {
        let text = std::str::from_utf8(pair).map_err(|_| CoordinationError::InvalidKey)?;
        bytes.push(u8::from_str_radix(text, 16).map_err(|_| CoordinationError::InvalidKey)?);
    }
    String::from_utf8(bytes).map_err(|_| CoordinationError::InvalidKey)
}

pub(crate) fn revision_from_header(
    header: Option<&etcd_client::ResponseHeader>,
) -> Result<Revision, CoordinationError> {
    let header = header.ok_or(CoordinationError::MissingRevision)?;
    Revision::new(header.revision())
}

pub(crate) fn live_instance(instance_id: InstanceId, session_id: SessionId) -> LiveInstance {
    LiveInstance::new(instance_id, session_id)
}

pub(crate) fn active_current_state(
    session_id: SessionId,
    resources: BTreeMap<crate::model::ResourceId, CurrentState>,
) -> ActiveCurrentState {
    ActiveCurrentState::new(session_id, resources)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_segment, encode_segment, is_authoritative_key, metadata_entry_from_snapshot,
        normalize_prefix, parse_pending_messages, revision_from_header, CoordinationError,
        CoordinationSnapshot, EtcdConnectionOptions, MetadataEntry, Revision,
    };
    use crate::model::{InstanceId, ParticipantSessionSnapshot, SessionId};
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[test]
    fn connection_options_redact_credentials_and_validate_tls_material() {
        let options = EtcdConnectionOptions::new()
            .with_credentials("admin", "super-secret")
            .with_ca_certificate(b"ca-pem".to_vec())
            .with_client_identity(b"client-cert".to_vec(), b"client-key".to_vec())
            .with_connect_timeout(Duration::from_secs(2))
            .with_request_timeout(Duration::from_secs(3))
            .with_keep_alive(Duration::from_secs(5), Duration::from_secs(6));
        assert!(options.validate().is_ok());
        let debug = format!("{options:?}");
        assert!(!debug.contains("super-secret"));
        assert!(!debug.contains("client-key"));
        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("<configured>"));

        assert!(EtcdConnectionOptions::new()
            .with_user("admin", "secret")
            .with_ca_certificate(Vec::<u8>::new())
            .validate()
            .is_err());
        assert!(EtcdConnectionOptions::new()
            .with_client_identity("certificate", Vec::<u8>::new())
            .validate()
            .is_err());
        assert!(EtcdConnectionOptions::new()
            .with_connect_timeout(Duration::ZERO)
            .validate()
            .is_err());
    }

    #[test]
    fn key_segments_round_trip_arbitrary_utf8() {
        let value = "resource/partition\0\u{2603}";
        assert_eq!(decode_segment(&encode_segment(value)).unwrap(), value);
    }

    #[test]
    fn prefixes_are_normalized_without_becoming_empty() {
        assert_eq!(
            normalize_prefix("/clustodian/".to_owned()).unwrap(),
            "/clustodian"
        );
        assert!(matches!(
            normalize_prefix("///".to_owned()),
            Err(CoordinationError::InvalidPrefix)
        ));
    }

    #[test]
    fn revisions_are_positive_and_incrementable() {
        let revision = Revision::new(7).unwrap();
        assert_eq!(revision.next().unwrap().value(), 8);
        assert!(matches!(
            Revision::new(0),
            Err(CoordinationError::InvalidRevision(0))
        ));
        assert!(matches!(
            Revision::new(i64::MAX).unwrap().next(),
            Err(CoordinationError::RevisionExhausted)
        ));
    }

    #[test]
    fn authoritative_revision_excludes_derived_and_session_sequence_keys() {
        assert!(!is_authoritative_key(
            "metadata/controller/output/external-view",
            Some("controller/output/external-view")
        ));
        assert!(!is_authoritative_key("internal/session-sequence", None));
        assert!(!is_authoritative_key(
            "metadata/participant/node-1/processed-revision",
            Some("participant/node-1/processed-revision")
        ));
        assert!(is_authoritative_key(
            "current-state/node/1/resource/partition",
            None
        ));
    }

    #[test]
    fn coordination_snapshots_expose_metadata_participants_and_election_views() {
        let revision = Revision::new(7).unwrap();
        let snapshot = CoordinationSnapshot::from_parts(
            revision,
            BTreeMap::from([(
                String::from("controller/cluster"),
                MetadataEntry {
                    value: Some(String::from("test")),
                    revision,
                },
            )]),
            ParticipantSessionSnapshot::default(),
        )
        .with_controller_election(
            Some((String::from("controller-a"), 11)),
            BTreeMap::from([
                (String::from("controller-a"), 11),
                (String::from("controller-b"), 12),
            ]),
        );

        assert_eq!(snapshot.revision(), revision);
        assert_eq!(snapshot.authoritative_revision(), revision);
        assert_eq!(
            snapshot.metadata()["controller/cluster"].value(),
            Some("test")
        );
        assert!(snapshot.participants().live_instances().is_empty());
        assert_eq!(
            snapshot.controller_election().active(),
            Some(("controller-a", 11))
        );
        assert_eq!(snapshot.controller_election().candidates().len(), 2);

        let updated = snapshot.with_authoritative_revision(Revision::new(8).unwrap());
        assert_eq!(updated.authoritative_revision().value(), 8);
    }

    #[test]
    fn coordination_errors_and_pending_queue_validation_are_descriptive() {
        let message = r#"{"message_id":"m1","resource":"r","partition":"p","instance":"i","target_session":1,"from":"A","to":"B","message_type":"STATE_TRANSITION"}"#;
        assert!(parse_pending_messages(None).unwrap().is_empty());
        assert_eq!(
            parse_pending_messages(Some(&format!("[{message}]")))
                .unwrap()
                .len(),
            1
        );
        assert!(parse_pending_messages(Some("not-json")).is_err());
        assert!(parse_pending_messages(Some(
            r#"[{"message_id":"","resource":"r","partition":"p","instance":"i","target_session":1,"from":"A","to":"B","message_type":"STATE_TRANSITION"}]"#
        ))
        .is_err());
        assert!(decode_segment("0").is_err());
        assert!(decode_segment("gg").is_err());
        assert!(normalize_prefix("bad\0prefix".to_owned()).is_err());

        let errors = [
            CoordinationError::InvalidPrefix,
            CoordinationError::InvalidCluster,
            CoordinationError::InvalidKey,
            CoordinationError::InvalidValue,
            CoordinationError::InvalidLeaseTtl,
            CoordinationError::LeaseExpired,
            CoordinationError::InvalidRevision(4),
            CoordinationError::RevisionExhausted,
            CoordinationError::MissingRevision,
            CoordinationError::OutsidePrefix,
            CoordinationError::InvalidSession,
            CoordinationError::UnknownSession(SessionId::from_wire_value(7)),
            CoordinationError::RegistrationLost,
            CoordinationError::Contention,
            CoordinationError::StaleSession(InstanceId::new("node-a").unwrap()),
            CoordinationError::DuplicateMessage(String::from("m1")),
            CoordinationError::StaleController,
            CoordinationError::StaleRevision,
            CoordinationError::InstanceStillLive(InstanceId::new("node-a").unwrap()),
            CoordinationError::ResourceDeletionDisabled,
        ];
        assert!(errors.iter().all(|error| !error.to_string().is_empty()));
    }

    #[test]
    fn snapshot_helpers_handle_empty_values_and_missing_headers() {
        let revision = Revision::new(9).unwrap();
        let empty = metadata_entry_from_snapshot(revision, &[]).unwrap();
        assert_eq!(empty.value(), None);
        assert_eq!(empty.revision(), revision);
        assert!(matches!(
            revision_from_header(None),
            Err(CoordinationError::MissingRevision)
        ));
    }
}
