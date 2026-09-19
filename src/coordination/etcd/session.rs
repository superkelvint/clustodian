use super::{
    active_current_state, decode_segment, encode_segment, live_instance, revision_from_header,
    CoordinationError, EtcdCoordination,
};
use crate::model::{
    CurrentState, InstanceId, ParticipantSessionSnapshot, PartitionId, ResourceId, SessionId, State,
};
use etcd_client::{Compare, CompareOp, GetOptions, PutOptions, Txn, TxnOp};
use std::collections::BTreeMap;
use std::time::Duration;

const MAX_CAS_RETRIES: usize = 32;
const CURRENT_STATE_DELETE_BATCH: usize = 100;

/// Registration settings for a participant incarnation.
#[derive(Clone, Debug)]
pub struct RegistrationOptions {
    lease_ttl: Duration,
    default_initial_state: State,
    resource_initial_states: BTreeMap<ResourceId, State>,
}

impl RegistrationOptions {
    /// Create registration settings with the given lease TTL.
    pub fn new(lease_ttl: Duration) -> Result<Self, CoordinationError> {
        if lease_ttl.is_zero() || lease_ttl.as_secs() == 0 || lease_ttl.as_secs() > i64::MAX as u64
        {
            return Err(CoordinationError::InvalidLeaseTtl);
        }
        Ok(Self {
            lease_ttl,
            default_initial_state: State::try_from("OFFLINE").expect("OFFLINE is a valid state"),
            resource_initial_states: BTreeMap::new(),
        })
    }

    /// Set the fallback state used when carrying records into a replacement session.
    pub fn with_default_initial_state(mut self, state: State) -> Self {
        self.default_initial_state = state;
        self
    }

    /// Set the state-model initial state for one resource.
    pub fn with_resource_initial_state(mut self, resource: ResourceId, state: State) -> Self {
        self.resource_initial_states.insert(resource, state);
        self
    }

    pub(crate) fn ttl_seconds(&self) -> i64 {
        self.lease_ttl.as_secs() as i64
    }

    pub(crate) fn initial_state(&self, resource: &ResourceId) -> &State {
        self.resource_initial_states
            .get(resource)
            .unwrap_or(&self.default_initial_state)
    }
}

/// A registered participant incarnation. The etcd LeaseId is private backend state.
#[derive(Clone, Debug)]
pub struct EtcdParticipantSession {
    backend: EtcdCoordination,
    instance_id: InstanceId,
    session_id: SessionId,
    lease_id: i64,
    registration_revision: super::Revision,
}

impl EtcdParticipantSession {
    /// Return the stable participant identity.
    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    /// Return the backend-independent participant incarnation identity.
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Return the etcd revision at which this LiveInstance was registered.
    pub const fn registration_revision(&self) -> super::Revision {
        self.registration_revision
    }

    /// Send a real etcd lease keepalive for this session.
    pub async fn keep_alive(&self) -> Result<(), CoordinationError> {
        let client = self.backend.client();
        let (mut keeper, mut responses) = client.lease_client().keep_alive(self.lease_id).await?;
        keeper.keep_alive().await?;
        responses
            .message()
            .await?
            .filter(|response| response.ttl() > 0)
            .map(|_| ())
            .ok_or(CoordinationError::LeaseExpired)
    }

    pub(crate) async fn is_current(&self) -> Result<bool, CoordinationError> {
        Ok(live_session(&self.backend, &self.instance_id).await? == Some(self.session_id))
    }

    /// Revoke the lease and remove the LiveInstance through etcd.
    pub async fn revoke(&self) -> Result<(), CoordinationError> {
        let client = self.backend.client();
        client.lease_client().revoke(self.lease_id).await?;
        Ok(())
    }

    /// Publish CurrentState under the session-owned keys, fenced by the live key.
    pub async fn publish_current_state(
        &self,
        resource: ResourceId,
        states: BTreeMap<PartitionId, State>,
    ) -> Result<super::Revision, CoordinationError> {
        publish_current_state(
            &self.backend,
            &self.instance_id,
            self.session_id,
            resource,
            states,
        )
        .await
    }
}

pub(crate) async fn publish_current_state(
    backend: &EtcdCoordination,
    instance_id: &InstanceId,
    session_id: SessionId,
    resource: ResourceId,
    states: BTreeMap<PartitionId, State>,
) -> Result<super::Revision, CoordinationError> {
    let client = backend.client();
    let live_key = backend.live_key(instance_id);
    let mut operations = Vec::with_capacity(states.len());
    for (partition, state) in states {
        operations.push(TxnOp::put(
            backend.current_state_key(instance_id, session_id, &resource, &partition),
            state.as_str(),
            None,
        ));
    }
    operations.push(backend.authoritative_input_marker_operation());
    let txn = Txn::new()
        .when([Compare::value(
            live_key,
            CompareOp::Equal,
            session_text(session_id),
        )])
        .and_then(operations);
    let response = client.kv_client().txn(txn).await?;
    if !response.succeeded() {
        return Err(CoordinationError::StaleSession(instance_id.clone()));
    }
    super::revision_from_header(response.header())
}

pub(crate) async fn register(
    backend: &EtcdCoordination,
    instance_id: InstanceId,
    options: RegistrationOptions,
) -> Result<EtcdParticipantSession, CoordinationError> {
    let previous_session = latest_session(backend, &instance_id).await?;
    let session_id = allocate_session(backend).await?;
    let carried = carried_state(
        backend,
        &instance_id,
        previous_session,
        session_id,
        &options,
    )
    .await?;

    let lease_client = backend.client();
    let lease = lease_client
        .lease_client()
        .grant(options.ttl_seconds(), None)
        .await?;
    let lease_id = lease.id();
    crate::failpoints::hard_abort("participant_after_lease_grant");

    let live_key = backend.live_key(&instance_id);
    let session_key = backend.session_key(&instance_id, session_id);
    let mut operations = Vec::with_capacity(2 + carried.len());
    operations.push(TxnOp::put(
        live_key.clone(),
        session_text(session_id),
        Some(PutOptions::new().with_lease(lease_id)),
    ));
    operations.push(TxnOp::put(session_key, "active", None));
    for (key, state) in carried {
        operations.push(TxnOp::put(key, state.as_str(), None));
    }
    operations.push(backend.authoritative_input_marker_operation());

    let mut kv_client = lease_client.kv_client();
    let transaction = Txn::new()
        .when([Compare::version(live_key, CompareOp::Equal, 0)])
        .and_then(operations);
    let response = kv_client.txn(transaction).await?;
    if !response.succeeded() {
        lease_client.lease_client().revoke(lease_id).await?;
        return Err(CoordinationError::RegistrationLost);
    }

    crate::failpoints::hard_abort("participant_after_live_registration");
    let registration_revision = super::revision_from_header(response.header())?;
    Ok(EtcdParticipantSession {
        backend: backend.clone(),
        instance_id,
        session_id,
        lease_id,
        registration_revision,
    })
}

pub(crate) async fn live_session(
    backend: &EtcdCoordination,
    instance_id: &InstanceId,
) -> Result<Option<SessionId>, CoordinationError> {
    let client = backend.client();
    let response = client
        .kv_client()
        .get(backend.live_key(instance_id), None)
        .await?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    parse_session(kv.value())
        .map(Some)
        .map_err(|_| CoordinationError::InvalidSession)
}

pub(crate) async fn revoke_live(
    backend: &EtcdCoordination,
    instance_id: &InstanceId,
    session_id: SessionId,
) -> Result<super::Revision, CoordinationError> {
    let client = backend.client();
    let live_key = backend.live_key(instance_id);
    let response = client.kv_client().get(live_key.clone(), None).await?;
    let Some(kv) = response.kvs().first() else {
        return Err(CoordinationError::StaleSession(instance_id.clone()));
    };
    if parse_session(kv.value())? != session_id {
        return Err(CoordinationError::StaleSession(instance_id.clone()));
    }
    let lease_id = kv.lease();
    let deleted = client
        .kv_client()
        .txn(
            Txn::new()
                .when([Compare::value(
                    live_key.clone(),
                    CompareOp::Equal,
                    session_text(session_id),
                )])
                .and_then([
                    TxnOp::delete(live_key, None),
                    backend.authoritative_input_marker_operation(),
                ]),
        )
        .await?;
    if !deleted.succeeded() {
        return Err(CoordinationError::StaleSession(instance_id.clone()));
    }
    if lease_id != 0 {
        client.lease_client().revoke(lease_id).await?;
    }
    super::revision_from_header(deleted.header())
}

pub(crate) async fn participant_snapshot(
    backend: &EtcdCoordination,
) -> Result<ParticipantSessionSnapshot, CoordinationError> {
    let (_, kvs) = backend
        .raw_snapshot(
            backend.namespaced_key("").as_bytes().to_vec(),
            Some(GetOptions::new().with_prefix()),
        )
        .await?;
    participant_snapshot_from_kvs(backend, &kvs)
}

pub(crate) fn participant_snapshot_from_kvs(
    backend: &EtcdCoordination,
    kvs: &[etcd_client::KeyValue],
) -> Result<ParticipantSessionSnapshot, CoordinationError> {
    let mut live = BTreeMap::new();
    let mut state_records: BTreeMap<
        (InstanceId, SessionId),
        BTreeMap<ResourceId, BTreeMap<PartitionId, State>>,
    > = BTreeMap::new();

    for kv in kvs {
        let relative = backend.parse_relative_key(kv.key())?;
        let parts = relative.split('/').collect::<Vec<_>>();
        match parts.as_slice() {
            ["live", encoded_instance] => {
                let instance = InstanceId::try_from(decode_segment(encoded_instance)?)
                    .map_err(|_| CoordinationError::InvalidKey)?;
                live.insert(instance, parse_session(kv.value())?);
            }
            ["current-state", encoded_instance, encoded_session, encoded_resource, encoded_partition] =>
            {
                let instance = InstanceId::try_from(decode_segment(encoded_instance)?)
                    .map_err(|_| CoordinationError::InvalidKey)?;
                let session = parse_session(encoded_session.as_bytes())?;
                let resource = ResourceId::try_from(decode_segment(encoded_resource)?)
                    .map_err(|_| CoordinationError::InvalidKey)?;
                let partition = PartitionId::try_from(decode_segment(encoded_partition)?)
                    .map_err(|_| CoordinationError::InvalidKey)?;
                let state = State::try_from(
                    std::str::from_utf8(kv.value()).map_err(|_| CoordinationError::InvalidValue)?,
                )
                .map_err(|_| CoordinationError::InvalidValue)?;
                state_records
                    .entry((instance, session))
                    .or_default()
                    .entry(resource)
                    .or_default()
                    .insert(partition, state);
            }
            _ => {}
        }
    }

    let mut live_instances = BTreeMap::new();
    let mut active = BTreeMap::new();
    for (instance, session) in live {
        let resources = state_records
            .remove(&(instance.clone(), session))
            .unwrap_or_default()
            .into_iter()
            .map(|(resource, partitions)| {
                let mut builder = CurrentState::builder();
                for (partition, state) in partitions {
                    builder
                        .set_state(partition, instance.clone(), state)
                        .map_err(|_| CoordinationError::InvalidKey)?;
                }
                Ok((resource, builder.build()))
            })
            .collect::<Result<BTreeMap<_, _>, CoordinationError>>()?;
        live_instances.insert(instance.clone(), live_instance(instance.clone(), session));
        active.insert(instance, active_current_state(session, resources));
    }
    Ok(ParticipantSessionSnapshot::from_parts(
        live_instances,
        active,
    ))
}

pub(crate) async fn inject_session_current_state(
    backend: &EtcdCoordination,
    instance_id: &InstanceId,
    session_id: SessionId,
    resource: ResourceId,
    states: BTreeMap<PartitionId, State>,
) -> Result<(), CoordinationError> {
    let client = backend.client();
    let marker = backend.session_key(instance_id, session_id);
    let marker_response = client.kv_client().get(marker, None).await?;
    if marker_response.kvs().is_empty() {
        return Err(CoordinationError::UnknownSession(session_id));
    }
    let operations = states
        .into_iter()
        .map(|(partition, state)| {
            TxnOp::put(
                backend.current_state_key(instance_id, session_id, &resource, &partition),
                state.as_str(),
                None,
            )
        })
        .collect::<Vec<_>>();
    let mut operations = operations;
    operations.push(backend.authoritative_input_marker_operation());
    client
        .kv_client()
        .txn(Txn::new().and_then(operations))
        .await?;
    Ok(())
}

pub(crate) async fn remove_current_states_for_resource(
    backend: &EtcdCoordination,
    resource: &ResourceId,
) -> Result<super::Revision, CoordinationError> {
    let (snapshot_revision, key_values) = backend
        .raw_snapshot(
            backend.namespaced_key("current-state/").into_bytes(),
            Some(GetOptions::new().with_prefix()),
        )
        .await?;
    let keys = key_values
        .into_iter()
        .filter_map(|key_value| {
            let relative = backend.parse_relative_key(key_value.key()).ok()?;
            let parts = relative.split('/').collect::<Vec<_>>();
            if parts.len() == 5 && parts[0] == "current-state" {
                let value = decode_segment(parts[3]).ok()?;
                if value == resource.as_str() {
                    return Some(String::from_utf8_lossy(key_value.key()).into_owned());
                }
            }
            None
        })
        .collect::<Vec<_>>();
    let client = backend.client();
    let mut revision = None;
    for chunk in keys.chunks(CURRENT_STATE_DELETE_BATCH) {
        let operations = chunk
            .iter()
            .map(|key| TxnOp::delete(key.clone(), None))
            .collect::<Vec<_>>();
        if operations.is_empty() {
            continue;
        }
        let mut operations = operations;
        operations.push(backend.authoritative_input_marker_operation());
        let response = client
            .kv_client()
            .txn(Txn::new().and_then(operations))
            .await?;
        revision = Some(revision_from_header(response.header())?);
    }
    Ok(revision.unwrap_or(snapshot_revision))
}

async fn allocate_session(backend: &EtcdCoordination) -> Result<SessionId, CoordinationError> {
    let key = backend.namespaced_key(super::INTERNAL_SESSION_SEQUENCE);
    let client = backend.client();
    for _ in 0..MAX_CAS_RETRIES {
        let response = client.kv_client().get(key.clone(), None).await?;
        let (old_value, old_revision) = match response.kvs().first() {
            Some(kv) => {
                let value = parse_sequence(kv.value())?;
                (value, Some(kv.mod_revision()))
            }
            None => (0, None),
        };
        let next = old_value
            .checked_add(1)
            .ok_or(CoordinationError::InvalidSession)?;
        let transaction = match old_revision {
            Some(revision) => Txn::new()
                .when([Compare::mod_revision(
                    key.clone(),
                    CompareOp::Equal,
                    revision,
                )])
                .and_then([TxnOp::put(key.clone(), next.to_string(), None)]),
            None => Txn::new()
                .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
                .and_then([TxnOp::put(key.clone(), next.to_string(), None)]),
        };
        if client.kv_client().txn(transaction).await?.succeeded() {
            return Ok(SessionId::from_sequence(next));
        }
    }
    Err(CoordinationError::Contention)
}

async fn latest_session(
    backend: &EtcdCoordination,
    instance: &InstanceId,
) -> Result<Option<SessionId>, CoordinationError> {
    let (_, kvs) = backend
        .raw_snapshot(
            backend.session_prefix(instance).into_bytes(),
            Some(GetOptions::new().with_prefix()),
        )
        .await?;
    kvs.into_iter()
        .filter_map(|kv| {
            let key = std::str::from_utf8(kv.key()).ok()?;
            let encoded = key.rsplit('/').next()?;
            parse_session(encoded.as_bytes()).ok()
        })
        .max()
        .map(Ok)
        .transpose()
}

async fn carried_state(
    backend: &EtcdCoordination,
    instance: &InstanceId,
    previous_session: Option<SessionId>,
    new_session: SessionId,
    options: &RegistrationOptions,
) -> Result<Vec<(String, State)>, CoordinationError> {
    let Some(previous_session) = previous_session else {
        return Ok(Vec::new());
    };
    let (_, kvs) = backend
        .raw_snapshot(
            backend
                .current_state_session_prefix(instance, previous_session)
                .into_bytes(),
            Some(GetOptions::new().with_prefix()),
        )
        .await?;
    kvs.into_iter()
        .map(|kv| {
            let relative = backend.parse_relative_key(kv.key())?;
            let mut parts = relative.split('/');
            let _ = parts.next();
            let _ = parts.next();
            let _ = parts.next();
            let resource = ResourceId::try_from(decode_segment(
                parts.next().ok_or(CoordinationError::InvalidKey)?,
            )?)
            .map_err(|_| CoordinationError::InvalidKey)?;
            let partition = PartitionId::try_from(decode_segment(
                parts.next().ok_or(CoordinationError::InvalidKey)?,
            )?)
            .map_err(|_| CoordinationError::InvalidKey)?;
            let state = options.initial_state(&resource).clone();
            Ok((
                backend.current_state_key(instance, new_session, &resource, &partition),
                state,
            ))
        })
        .collect()
}

fn parse_sequence(value: &[u8]) -> Result<u64, CoordinationError> {
    let sequence = std::str::from_utf8(value)
        .map_err(|_| CoordinationError::InvalidSession)?
        .parse()
        .map_err(|_| CoordinationError::InvalidSession)?;
    (sequence > 0)
        .then_some(sequence)
        .ok_or(CoordinationError::InvalidSession)
}

fn parse_session(value: &[u8]) -> Result<SessionId, CoordinationError> {
    parse_sequence(value).map(SessionId::from_sequence)
}

fn session_text(session: SessionId) -> String {
    session.sequence().to_string()
}

impl EtcdCoordination {
    pub(crate) fn live_key(&self, instance: &InstanceId) -> String {
        self.namespaced_key(&format!("live/{}", encode_segment(instance.as_str())))
    }

    fn session_key(&self, instance: &InstanceId, session: SessionId) -> String {
        self.namespaced_key(&format!(
            "sessions/{}/{}",
            encode_segment(instance.as_str()),
            session_text(session)
        ))
    }

    fn session_prefix(&self, instance: &InstanceId) -> String {
        self.namespaced_key(&format!("sessions/{}/", encode_segment(instance.as_str())))
    }

    pub(crate) fn current_state_key(
        &self,
        instance: &InstanceId,
        session: SessionId,
        resource: &ResourceId,
        partition: &PartitionId,
    ) -> String {
        self.namespaced_key(&format!(
            "current-state/{}/{}/{}/{}",
            encode_segment(instance.as_str()),
            session_text(session),
            encode_segment(resource.as_str()),
            encode_segment(partition.as_str())
        ))
    }

    pub(crate) fn current_state_session_prefix(
        &self,
        instance: &InstanceId,
        session: SessionId,
    ) -> String {
        self.namespaced_key(&format!(
            "current-state/{}/{}/",
            encode_segment(instance.as_str()),
            session_text(session)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_sequence, parse_session, session_text, CoordinationError, RegistrationOptions,
    };
    use crate::model::{ResourceId, State};
    use std::time::Duration;

    #[test]
    fn registration_requires_a_whole_second_etcd_ttl() {
        assert!(matches!(
            RegistrationOptions::new(Duration::from_millis(999)),
            Err(CoordinationError::InvalidLeaseTtl)
        ));
        assert!(RegistrationOptions::new(Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn registration_options_and_session_values_preserve_wire_contracts() {
        let resource = ResourceId::new("documents").unwrap();
        let options = RegistrationOptions::new(Duration::from_secs(5))
            .unwrap()
            .with_default_initial_state(State::try_from("ERROR").unwrap())
            .with_resource_initial_state(resource.clone(), State::try_from("OFFLINE").unwrap());
        assert_eq!(options.ttl_seconds(), 5);
        assert_eq!(options.initial_state(&resource).as_str(), "OFFLINE");
        assert_eq!(
            options
                .initial_state(&ResourceId::new("other").unwrap())
                .as_str(),
            "ERROR"
        );

        assert_eq!(parse_session(b"42").unwrap().wire_value(), 42);
        assert_eq!(session_text(parse_session(b"42").unwrap()), "42");
        for invalid in [b"".as_slice(), b"0", b"-1", b"not-a-session"] {
            assert!(parse_sequence(invalid).is_err());
        }
        assert!(parse_sequence(&[0xff]).is_err());
    }

    #[test]
    fn registration_rejects_zero_and_unrepresentable_ttls() {
        assert!(matches!(
            RegistrationOptions::new(Duration::ZERO),
            Err(CoordinationError::InvalidLeaseTtl)
        ));
        assert!(matches!(
            RegistrationOptions::new(Duration::from_secs(i64::MAX as u64 + 1)),
            Err(CoordinationError::InvalidLeaseTtl)
        ));
        assert_eq!(parse_sequence(b"18446744073709551615").unwrap(), u64::MAX);
        assert_eq!(parse_session(b"1").unwrap().wire_value(), 1);
        assert!(parse_sequence(b"18446744073709551616").is_err());
    }
}
