use super::{
    encode_segment, is_authoritative_metadata_key, revision_from_header, CoordinationError,
    EtcdCoordination, Revision, AUTHORITATIVE_INPUT_MARKER,
};
use crate::election::{ControllerAuthority, ControllerCommitFence};
use crate::model::InstanceId;
use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};

/// A persistent metadata value and its etcd modification revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataEntry {
    pub(crate) value: Option<String>,
    pub(crate) revision: Revision,
}

impl MetadataEntry {
    pub(crate) const fn empty(revision: Revision) -> Self {
        Self {
            value: None,
            revision,
        }
    }

    /// Return the stored value, if present.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Return the value's modification revision, or the snapshot revision if absent.
    pub const fn revision(&self) -> Revision {
        self.revision
    }
}

/// Result of a revision-guarded metadata update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CasResult {
    applied: bool,
    revision: Revision,
}

impl CasResult {
    /// Whether the compare condition succeeded and the value was updated.
    pub const fn applied(self) -> bool {
        self.applied
    }

    /// The etcd revision at which the transaction completed.
    pub const fn revision(self) -> Revision {
        self.revision
    }
}

pub(crate) async fn snapshot(
    backend: &EtcdCoordination,
    key: &str,
) -> Result<(Revision, Vec<etcd_client::KeyValue>), CoordinationError> {
    let encoded_key = backend
        .namespaced_key(&format!("metadata/{}", encode_segment(key)))
        .into_bytes();
    backend.raw_snapshot(encoded_key, None).await
}

pub(crate) async fn get(
    backend: &EtcdCoordination,
    key: &str,
    revision: Option<Revision>,
) -> Result<Option<MetadataEntry>, CoordinationError> {
    let encoded_key = backend
        .namespaced_key(&format!("metadata/{}", encode_segment(key)))
        .into_bytes();
    let options =
        revision.map(|revision| etcd_client::GetOptions::new().with_revision(revision.value()));
    let (_, kvs) = backend.raw_snapshot(encoded_key, options).await?;
    let Some(kv) = kvs.first() else {
        return Ok(None);
    };
    let value =
        String::from_utf8(kv.value().to_vec()).map_err(|_| CoordinationError::InvalidValue)?;
    Ok(Some(MetadataEntry {
        value: Some(value),
        revision: Revision::new(kv.mod_revision())?,
    }))
}

pub(crate) async fn put(
    backend: &EtcdCoordination,
    key: &str,
    value: &str,
    lease: Option<i64>,
) -> Result<Revision, CoordinationError> {
    let client = backend.client();
    let options = lease.map(|lease| PutOptions::new().with_lease(lease));
    if is_authoritative_metadata_key(key) {
        let metadata_key = backend.namespaced_key(&format!("metadata/{}", encode_segment(key)));
        let response = client
            .kv_client()
            .txn(Txn::new().and_then([
                TxnOp::put(metadata_key, value, options),
                backend.authoritative_input_marker_operation(),
            ]))
            .await?;
        return revision_from_header(response.header());
    }
    let response = client
        .kv_client()
        .put(
            backend.namespaced_key(&format!("metadata/{}", encode_segment(key))),
            value,
            options,
        )
        .await?;
    revision_from_header(response.header())
}

pub(crate) async fn compare_and_put(
    backend: &EtcdCoordination,
    key: &str,
    expected_revision: Revision,
    value: &str,
) -> Result<CasResult, CoordinationError> {
    let client = backend.client();
    let encoded_key = backend.namespaced_key(&format!("metadata/{}", encode_segment(key)));
    let mut operations = vec![TxnOp::put(encoded_key.clone(), value, None)];
    if is_authoritative_metadata_key(key) {
        operations.push(backend.authoritative_input_marker_operation());
    }
    let txn = Txn::new()
        .when([Compare::mod_revision(
            encoded_key,
            CompareOp::Equal,
            expected_revision.value(),
        )])
        .and_then(operations);
    let response = client.kv_client().txn(txn).await?;
    Ok(CasResult {
        applied: response.succeeded(),
        revision: revision_from_header(response.header())?,
    })
}

pub(crate) async fn compare_and_put_if_live_absent(
    backend: &EtcdCoordination,
    key: &str,
    expected_revision: Revision,
    instance: &InstanceId,
    value: &str,
) -> Result<CasResult, CoordinationError> {
    let client = backend.client();
    let encoded_key = backend.metadata_key(key);
    let live_key = backend.live_key(instance);
    let mut operations = vec![TxnOp::put(encoded_key.clone(), value, None)];
    if is_authoritative_metadata_key(key) {
        operations.push(backend.authoritative_input_marker_operation());
    }
    let response = client
        .kv_client()
        .txn(
            Txn::new()
                .when([
                    Compare::mod_revision(encoded_key, CompareOp::Equal, expected_revision.value()),
                    Compare::version(live_key, CompareOp::Equal, 0),
                ])
                .and_then(operations),
        )
        .await?;
    Ok(CasResult {
        applied: response.succeeded(),
        revision: revision_from_header(response.header())?,
    })
}

pub(crate) async fn compare_and_delete(
    backend: &EtcdCoordination,
    key: &str,
    expected_revision: Revision,
) -> Result<CasResult, CoordinationError> {
    let client = backend.client();
    let encoded_key = backend.metadata_key(key);
    let mut operations = vec![TxnOp::delete(encoded_key.clone(), None)];
    if is_authoritative_metadata_key(key) {
        operations.push(backend.authoritative_input_marker_operation());
    }
    let response = client
        .kv_client()
        .txn(
            Txn::new()
                .when([Compare::mod_revision(
                    encoded_key,
                    CompareOp::Equal,
                    expected_revision.value(),
                )])
                .and_then(operations),
        )
        .await?;
    Ok(CasResult {
        applied: response.succeeded(),
        revision: revision_from_header(response.header())?,
    })
}

pub(crate) async fn compare_and_put_absent(
    backend: &EtcdCoordination,
    key: &str,
    value: &str,
) -> Result<bool, CoordinationError> {
    let client = backend.client();
    let encoded_key = backend.metadata_key(key);
    let mut operations = vec![TxnOp::put(encoded_key.clone(), value, None)];
    if is_authoritative_metadata_key(key) {
        operations.push(backend.authoritative_input_marker_operation());
    }
    let response = client
        .kv_client()
        .txn(
            Txn::new()
                .when([Compare::version(encoded_key, CompareOp::Equal, 0)])
                .and_then(operations),
        )
        .await?;
    Ok(response.succeeded())
}

pub(crate) async fn initialize_authoritative_input_marker(
    backend: &EtcdCoordination,
) -> Result<(), CoordinationError> {
    let _ = compare_and_put_absent(backend, AUTHORITATIVE_INPUT_MARKER, "1").await?;
    Ok(())
}

pub(crate) async fn publish_controller_outputs(
    backend: &EtcdCoordination,
    fence: &ControllerCommitFence,
    external_view: &str,
    pending_transitions: &str,
    pending_revision: Option<Revision>,
) -> Result<Revision, CoordinationError> {
    let client = backend.client();
    let authority_key = backend.controller_active_key();
    if fence.namespace != backend.namespace() {
        return Err(CoordinationError::StaleController);
    }
    let operations = vec![
        TxnOp::put(
            backend.namespaced_key(&format!(
                "metadata/{}",
                encode_segment("controller/output/external-view")
            )),
            external_view,
            None,
        ),
        TxnOp::put(
            backend.namespaced_key(&format!(
                "metadata/{}",
                encode_segment("controller/output/pending-transitions")
            )),
            pending_transitions,
            None,
        ),
        TxnOp::put(
            backend.namespaced_key(&format!(
                "metadata/{}",
                encode_segment("controller/output/processed-revision")
            )),
            fence.observed_revision.value().to_string(),
            None,
        ),
    ];
    let pending_key = backend.metadata_key("controller/output/pending-transitions");
    for _ in 0..32 {
        let processed_key = backend.metadata_key("controller/output/processed-revision");
        let current = client.kv_client().get(processed_key.clone(), None).await?;
        let (revision_compare, current_revision) = match current.kvs().first() {
            Some(kv) => {
                let value = std::str::from_utf8(kv.value())
                    .map_err(|_| CoordinationError::InvalidValue)?
                    .parse::<i64>()
                    .map_err(|_| CoordinationError::InvalidValue)?;
                (
                    Compare::mod_revision(
                        processed_key.clone(),
                        CompareOp::Equal,
                        kv.mod_revision(),
                    ),
                    Some(Revision::new(value)?),
                )
            }
            None => (
                Compare::version(processed_key.clone(), CompareOp::Equal, 0),
                None,
            ),
        };
        if current_revision.is_some_and(|current| current > fence.observed_revision) {
            return Err(CoordinationError::StaleRevision);
        }
        let mut compares = vec![
            Compare::value(
                authority_key.clone(),
                CompareOp::Equal,
                fence.controller_id.clone(),
            ),
            Compare::lease(authority_key.clone(), CompareOp::Equal, fence.lease_id),
            revision_compare,
            pending_revision_compare(&pending_key, pending_revision),
            Compare::mod_revision(
                backend.metadata_key(AUTHORITATIVE_INPUT_MARKER),
                CompareOp::Equal,
                fence.observed_revision.value(),
            ),
        ];
        compares.extend(fence.live_sessions.iter().map(|(instance, session)| {
            Compare::value(
                backend.live_key(instance),
                CompareOp::Equal,
                session.wire_value().to_string(),
            )
        }));
        let response = client
            .kv_client()
            .txn(Txn::new().when(compares).and_then(operations.clone()))
            .await?;
        if response.succeeded() {
            return revision_from_header(response.header());
        }
        if !authority_matches(
            &client,
            &authority_key,
            &fence.controller_id,
            fence.lease_id,
        )
        .await?
        {
            return Err(CoordinationError::StaleController);
        }
        if pending_revision_changed(&client, &pending_key, pending_revision).await?
            || input_fence_changed(backend, fence).await?
        {
            return Err(CoordinationError::StaleRevision);
        }
        let latest = client.kv_client().get(processed_key, None).await?;
        if let Some(kv) = latest.kvs().first() {
            let value = std::str::from_utf8(kv.value())
                .map_err(|_| CoordinationError::InvalidValue)?
                .parse::<i64>()
                .map_err(|_| CoordinationError::InvalidValue)?;
            if Revision::new(value)? > fence.observed_revision {
                return Err(CoordinationError::StaleRevision);
            }
        }
    }
    Err(CoordinationError::Contention)
}

pub(crate) async fn record_lease_expiry(
    backend: &EtcdCoordination,
    authority: &ControllerAuthority,
    event_revision: Revision,
) -> Result<Revision, CoordinationError> {
    if authority.namespace != backend.namespace() {
        return Err(CoordinationError::StaleController);
    }
    let client = backend.client();
    let marker_key = backend.metadata_key(AUTHORITATIVE_INPUT_MARKER);
    let authority_key = backend.controller_active_key();

    for _ in 0..32 {
        let marker = client.kv_client().get(marker_key.clone(), None).await?;
        if let Some(current) = marker.kvs().first() {
            let current_revision = Revision::new(current.mod_revision())?;
            if current_revision >= event_revision {
                return Ok(current_revision);
            }
        }

        let current_marker_revision = marker
            .kvs()
            .first()
            .map(|value| value.mod_revision())
            .unwrap_or(0);
        let marker_compare = if current_marker_revision == 0 {
            Compare::version(marker_key.clone(), CompareOp::Equal, 0)
        } else {
            Compare::mod_revision(
                marker_key.clone(),
                CompareOp::Equal,
                current_marker_revision,
            )
        };
        let response = client
            .kv_client()
            .txn(
                Txn::new()
                    .when([
                        Compare::value(
                            authority_key.clone(),
                            CompareOp::Equal,
                            authority.controller_id.clone(),
                        ),
                        Compare::lease(authority_key.clone(), CompareOp::Equal, authority.lease_id),
                        marker_compare,
                    ])
                    .and_then([TxnOp::put(marker_key.clone(), "1", None)]),
            )
            .await?;
        if response.succeeded() {
            return revision_from_header(response.header());
        }
        if !authority_matches(
            &client,
            &authority_key,
            &authority.controller_id,
            authority.lease_id,
        )
        .await?
        {
            return Err(CoordinationError::StaleController);
        }
    }
    Err(CoordinationError::Contention)
}

async fn authority_matches(
    client: &etcd_client::Client,
    authority_key: &str,
    controller_id: &str,
    lease_id: i64,
) -> Result<bool, CoordinationError> {
    let current = client.kv_client().get(authority_key, None).await?;
    let Some(entry) = current.kvs().first() else {
        return Ok(false);
    };
    Ok(entry.value() == controller_id.as_bytes() && entry.lease() == lease_id)
}

async fn input_fence_changed(
    backend: &EtcdCoordination,
    fence: &ControllerCommitFence,
) -> Result<bool, CoordinationError> {
    let marker = backend
        .kv_client()
        .get(backend.metadata_key(AUTHORITATIVE_INPUT_MARKER), None)
        .await?;
    let Some(marker) = marker.kvs().first() else {
        return Ok(true);
    };
    if marker.mod_revision() != fence.observed_revision.value() {
        return Ok(true);
    }
    for (instance, session) in &fence.live_sessions {
        let live = backend
            .kv_client()
            .get(backend.live_key(instance), None)
            .await?;
        let Some(live) = live.kvs().first() else {
            return Ok(true);
        };
        if live.value() != session.wire_value().to_string().as_bytes() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn pending_revision_compare(key: &str, revision: Option<Revision>) -> Compare {
    match revision {
        Some(revision) => Compare::mod_revision(key.to_owned(), CompareOp::Equal, revision.value()),
        None => Compare::version(key.to_owned(), CompareOp::Equal, 0),
    }
}

async fn pending_revision_changed(
    client: &etcd_client::Client,
    key: &str,
    expected: Option<Revision>,
) -> Result<bool, CoordinationError> {
    let current = client.kv_client().get(key.to_owned(), None).await?;
    Ok(match (expected, current.kvs().first()) {
        (Some(expected), Some(current)) => current.mod_revision() != expected.value(),
        (None, None) => false,
        (None, Some(_)) | (Some(_), None) => true,
    })
}

#[cfg(test)]
mod tests {
    use super::{pending_revision_compare, CasResult, MetadataEntry};
    use crate::coordination::etcd::Revision;

    #[test]
    fn metadata_entries_and_cas_results_expose_their_revision_contract() {
        let revision = Revision::new(7).unwrap();
        let present = MetadataEntry {
            value: Some(String::from("value")),
            revision,
        };
        assert_eq!(present.value(), Some("value"));
        assert_eq!(present.revision(), revision);

        let absent = MetadataEntry::empty(revision);
        assert_eq!(absent.value(), None);
        assert_eq!(absent.revision(), revision);

        let applied = CasResult {
            applied: true,
            revision,
        };
        assert!(applied.applied());
        assert_eq!(applied.revision(), revision);

        let rejected = CasResult {
            applied: false,
            revision,
        };
        assert!(!rejected.applied());
        assert_eq!(rejected.revision(), revision);
    }

    #[test]
    fn pending_revision_comparison_distinguishes_present_and_absent_keys() {
        let revision = Revision::new(7).unwrap();
        let existing = pending_revision_compare("metadata/pending", Some(revision));
        let absent = pending_revision_compare("metadata/pending", None);
        assert_ne!(format!("{existing:?}"), format!("{absent:?}"));
    }
}
