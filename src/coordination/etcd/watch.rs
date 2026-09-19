use super::{
    decode_segment, encode_segment, CoordinationError, CoordinationSnapshot, EtcdCoordination,
    Revision,
};
use crate::coordination::etcd::MetadataEntry;
use etcd_client::{EventType, GetOptions, WatchOptions, WatchStream, Watcher};
use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

const WATCH_RESUME_BACKOFF: Duration = Duration::from_millis(250);

/// A point-in-time key/value snapshot used to seed a watch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyValueSnapshot {
    revision: Revision,
    entries: Vec<(String, String)>,
}

/// Authoritative state returned while rebuilding a compacted watch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchRecovery {
    /// The current value of a single metadata-key watch, if it exists.
    Metadata(Option<MetadataEntry>),
    /// A complete coordination snapshot for a namespace watch.
    Namespace(CoordinationSnapshot),
}

impl KeyValueSnapshot {
    /// Return the snapshot revision.
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Return the decoded relative keys and values in key order.
    pub fn entries(&self) -> &[(String, String)] {
        &self.entries
    }
}

/// The semantic kind of a watched mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchEventKind {
    Put,
    Delete,
}

/// One decoded etcd mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchEvent {
    revision: Revision,
    key: String,
    value: Option<String>,
    kind: WatchEventKind,
}

/// Local semantic cursor state for a resumable watch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WatchCursor {
    start_revision: Revision,
    last_event_revision: Option<Revision>,
    processed_keys: BTreeSet<String>,
}

impl WatchCursor {
    pub(crate) const fn new(start_revision: Revision) -> Self {
        Self {
            start_revision,
            last_event_revision: None,
            processed_keys: BTreeSet::new(),
        }
    }

    pub(crate) fn accept(&mut self, revision: Revision, key: &str) -> bool {
        if revision < self.start_revision
            || self.last_event_revision.is_some_and(|last| revision < last)
            || (self.last_event_revision == Some(revision) && self.processed_keys.contains(key))
        {
            return false;
        }
        if self.last_event_revision != Some(revision) {
            self.processed_keys.clear();
            self.last_event_revision = Some(revision);
        }
        self.processed_keys.insert(key.to_owned());
        true
    }

    pub(crate) const fn resume_revision(&self) -> Revision {
        match self.last_event_revision {
            Some(revision) => revision,
            None => self.start_revision,
        }
    }

    #[allow(dead_code)]
    pub(crate) const fn start_revision(&self) -> Revision {
        self.start_revision
    }

    pub(crate) fn reset_after_recovery(
        &mut self,
        snapshot_revision: Revision,
    ) -> Result<Revision, CoordinationError> {
        let start_revision = snapshot_revision.next()?;
        self.start_revision = start_revision;
        self.last_event_revision = None;
        self.processed_keys.clear();
        Ok(start_revision)
    }
}

impl WatchEvent {
    #[cfg(test)]
    pub(crate) fn from_parts(
        revision: Revision,
        key: String,
        value: Option<String>,
        kind: WatchEventKind,
    ) -> Self {
        Self {
            revision,
            key,
            value,
            kind,
        }
    }

    /// Return the event revision.
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Return the relative metadata key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return the new value for puts, or None for deletes.
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// Return whether the mutation was a put or delete.
    pub const fn kind(&self) -> WatchEventKind {
        self.kind
    }
}

pub(crate) fn is_lease_backed_delete(event: &WatchEvent) -> bool {
    event.kind == WatchEventKind::Delete
        && (event.key.starts_with("live/") || event.key.starts_with("current-state/"))
}

/// Watch errors, including the explicit compaction recovery boundary.
#[derive(Debug)]
pub enum WatchError {
    Coordination(CoordinationError),
    Disconnected,
    Compacted { revision: Revision },
}

impl From<CoordinationError> for WatchError {
    fn from(error: CoordinationError) -> Self {
        Self::Coordination(error)
    }
}

impl std::fmt::Display for WatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Coordination(error) => error.fmt(formatter),
            Self::Disconnected => formatter.write_str("watch stream disconnected"),
            Self::Compacted { revision } => {
                write!(
                    formatter,
                    "watch history compacted at revision {}",
                    revision.value()
                )
            }
        }
    }
}

impl std::error::Error for WatchError {}

/// A resumable watch cursor. It does not spawn a background task.
pub struct WatchSubscription {
    backend: EtcdCoordination,
    key: String,
    prefix: bool,
    cursor: WatchCursor,
    watcher: Option<Watcher>,
    stream: Option<WatchStream>,
    pending_events: VecDeque<WatchEvent>,
}

impl std::fmt::Debug for WatchSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WatchSubscription")
            .field("key", &self.key)
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

impl WatchSubscription {
    /// Read the next semantic event.
    pub async fn next(&mut self) -> Result<WatchEvent, WatchError> {
        loop {
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(event);
            }
            let stream = self.stream.as_mut().ok_or(WatchError::Disconnected)?;
            let response = stream
                .message()
                .await
                .map_err(CoordinationError::from)?
                .ok_or(WatchError::Disconnected)?;
            if response.compact_revision() > 0 {
                return Err(WatchError::Compacted {
                    revision: Revision::new(response.compact_revision())?,
                });
            }
            for event in response.events() {
                let Some(decoded_event) = self.decode_event(event)? else {
                    continue;
                };
                let revision = decoded_event.revision;
                let key = decoded_event.key.clone();
                if !self.cursor.accept(revision, &key) {
                    continue;
                }
                self.pending_events.push_back(decoded_event);
            }
            if let Some(event) = self.pending_events.pop_front() {
                return Ok(event);
            }
        }
    }

    fn decode_event(&self, event: &etcd_client::Event) -> Result<Option<WatchEvent>, WatchError> {
        let Some(key_value) = event.kv() else {
            return Ok(None);
        };
        let revision = Revision::new(key_value.mod_revision())?;
        let relative_key = self.backend.parse_relative_key(key_value.key())?;
        let key = if self.prefix {
            relative_key.to_owned()
        } else {
            relative_key
                .strip_prefix("metadata/")
                .ok_or(CoordinationError::InvalidKey)
                .and_then(decode_segment)
                .map_err(WatchError::Coordination)?
        };
        let kind = match event.event_type() {
            EventType::Put => WatchEventKind::Put,
            EventType::Delete => WatchEventKind::Delete,
        };
        let value = match kind {
            WatchEventKind::Put => Some(
                String::from_utf8(key_value.value().to_vec())
                    .map_err(|_| WatchError::Coordination(CoordinationError::InvalidValue))?,
            ),
            WatchEventKind::Delete => None,
        };
        Ok(Some(WatchEvent {
            revision,
            key,
            value,
            kind,
        }))
    }

    /// Drop the current stream to model a disconnected observer.
    pub fn disconnect(&mut self) {
        self.watcher = None;
        self.stream = None;
    }

    /// Resume from the last processed event without duplicating it.
    pub async fn resume(&mut self) -> Result<(), WatchError> {
        let revision = self.cursor.resume_revision();
        let (watcher, stream) = open_watch(&self.backend, &self.key, revision).await?;
        self.watcher = Some(watcher);
        self.stream = Some(stream);
        Ok(())
    }

    /// Resume a disconnected watch, retrying transient etcd transport errors.
    ///
    /// The operation is intentionally cancellation-safe: callers can place it
    /// in a `select!` with their shutdown future (or an enclosing timeout).
    /// Compaction, malformed data, and all other semantic errors are returned
    /// immediately to the caller.
    pub async fn resume_until_available(&mut self) -> Result<(), WatchError> {
        loop {
            match self.resume().await {
                Ok(()) => return Ok(()),
                Err(error) if is_transient(&error) => {
                    tokio::time::sleep(WATCH_RESUME_BACKOFF).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Rebuild the authoritative state after a compacted watch.
    pub async fn recover_compaction(&mut self) -> Result<WatchRecovery, WatchError> {
        if self.prefix {
            let snapshot = self.backend.controller_snapshot().await?;
            let start_revision = self.cursor.reset_after_recovery(snapshot.revision())?;
            self.pending_events.clear();
            let (watcher, stream) = open_namespace_watch(&self.backend, start_revision).await?;
            self.watcher = Some(watcher);
            self.stream = Some(stream);
            return Ok(WatchRecovery::Namespace(snapshot));
        }
        let encoded_key = self
            .backend
            .namespaced_key(&format!("metadata/{}", encode_segment(&self.key)))
            .into_bytes();
        let (revision, kvs) = self.backend.raw_snapshot(encoded_key, None).await?;
        let entry = if let Some(kv) = kvs.first() {
            Some(MetadataEntry {
                value: Some(
                    String::from_utf8(kv.value().to_vec())
                        .map_err(|_| CoordinationError::InvalidValue)?,
                ),
                revision: Revision::new(kv.mod_revision())?,
            })
        } else {
            None
        };
        let start_revision = self.cursor.reset_after_recovery(revision)?;
        self.pending_events.clear();
        let (watcher, stream) = open_watch(&self.backend, &self.key, start_revision).await?;
        self.watcher = Some(watcher);
        self.stream = Some(stream);
        Ok(WatchRecovery::Metadata(entry))
    }
}

fn is_transient(error: &WatchError) -> bool {
    matches!(
        error,
        WatchError::Disconnected | WatchError::Coordination(CoordinationError::Etcd(_))
    )
}

pub(crate) async fn watch_metadata(
    backend: &EtcdCoordination,
    key: &str,
    start_revision: Revision,
) -> Result<WatchSubscription, CoordinationError> {
    let (watcher, stream) = open_watch(backend, key, start_revision).await?;
    Ok(WatchSubscription {
        backend: backend.clone(),
        key: key.to_owned(),
        prefix: false,
        cursor: WatchCursor::new(start_revision),
        watcher: Some(watcher),
        stream: Some(stream),
        pending_events: VecDeque::new(),
    })
}

pub(crate) async fn watch_namespace(
    backend: &EtcdCoordination,
    start_revision: Revision,
) -> Result<WatchSubscription, CoordinationError> {
    let (watcher, stream) = open_namespace_watch(backend, start_revision).await?;
    Ok(WatchSubscription {
        backend: backend.clone(),
        key: String::new(),
        prefix: true,
        cursor: WatchCursor::new(start_revision),
        watcher: Some(watcher),
        stream: Some(stream),
        pending_events: VecDeque::new(),
    })
}

async fn open_watch(
    backend: &EtcdCoordination,
    key: &str,
    start_revision: Revision,
) -> Result<(Watcher, WatchStream), CoordinationError> {
    let encoded_key = backend.namespaced_key(&format!("metadata/{}", encode_segment(key)));
    let options = WatchOptions::new().with_start_revision(start_revision.value());
    Ok(backend
        .watch_client()
        .watch(encoded_key, Some(options))
        .await?)
}

async fn open_namespace_watch(
    backend: &EtcdCoordination,
    start_revision: Revision,
) -> Result<(Watcher, WatchStream), CoordinationError> {
    let prefix = backend.namespaced_key("");
    let options = WatchOptions::new()
        .with_prefix()
        .with_start_revision(start_revision.value());
    Ok(backend.watch_client().watch(prefix, Some(options)).await?)
}

#[allow(dead_code)]
pub(crate) async fn snapshot_prefix(
    backend: &EtcdCoordination,
    relative_prefix: &str,
) -> Result<KeyValueSnapshot, CoordinationError> {
    let key = backend
        .namespaced_key(&format!("metadata/{}", encode_segment(relative_prefix)))
        .into_bytes();
    let (revision, kvs) = backend
        .raw_snapshot(key, Some(GetOptions::new().with_prefix()))
        .await?;
    let mut entries = Vec::with_capacity(kvs.len());
    for kv in kvs {
        entries.push((
            decode_segment(
                backend
                    .parse_relative_key(kv.key())?
                    .trim_start_matches("metadata/"),
            )?,
            String::from_utf8(kv.value().to_vec()).map_err(|_| CoordinationError::InvalidValue)?,
        ));
    }
    Ok(KeyValueSnapshot { revision, entries })
}

#[cfg(test)]
mod tests {
    use super::{KeyValueSnapshot, WatchCursor, WatchError, WatchEvent, WatchEventKind};
    use crate::coordination::etcd::{CoordinationError, Revision};

    fn revision(value: i64) -> Revision {
        Revision::new(value).unwrap()
    }

    #[test]
    fn cursor_filters_old_and_duplicate_events_and_resets_after_recovery() {
        let mut cursor = WatchCursor::new(revision(5));
        assert_eq!(cursor.start_revision(), revision(5));
        assert!(!cursor.accept(revision(4), "old"));
        assert!(cursor.accept(revision(5), "first"));
        assert!(!cursor.accept(revision(5), "first"));
        assert!(cursor.accept(revision(5), "second"));
        assert!(!cursor.accept(revision(4), "older"));
        assert_eq!(cursor.resume_revision(), revision(5));
        assert!(cursor.accept(revision(6), "new"));
        assert_eq!(cursor.resume_revision(), revision(6));
        assert_eq!(
            cursor.reset_after_recovery(revision(9)).unwrap(),
            revision(10)
        );
        assert_eq!(cursor.start_revision(), revision(10));
        assert_eq!(cursor.resume_revision(), revision(10));
        assert!(matches!(
            cursor.reset_after_recovery(revision(i64::MAX)),
            Err(CoordinationError::RevisionExhausted)
        ));
    }

    #[test]
    fn watch_value_and_error_views_are_typed() {
        let event = WatchEvent {
            revision: revision(3),
            key: String::from("controller/cluster"),
            value: Some(String::from("test")),
            kind: WatchEventKind::Put,
        };
        assert_eq!(event.revision(), revision(3));
        assert_eq!(event.key(), "controller/cluster");
        assert_eq!(event.value(), Some("test"));
        assert_eq!(event.kind(), WatchEventKind::Put);
        let delete = WatchEvent {
            value: None,
            kind: WatchEventKind::Delete,
            ..event
        };
        assert_eq!(delete.value(), None);
        assert_eq!(delete.kind(), WatchEventKind::Delete);

        let snapshot = KeyValueSnapshot {
            revision: revision(7),
            entries: vec![(String::from("a"), String::from("b"))],
        };
        assert_eq!(snapshot.revision(), revision(7));
        assert_eq!(snapshot.entries(), [(String::from("a"), String::from("b"))]);
        assert_eq!(
            WatchError::Disconnected.to_string(),
            "watch stream disconnected"
        );
        assert_eq!(
            WatchError::Compacted {
                revision: revision(7)
            }
            .to_string(),
            "watch history compacted at revision 7"
        );
        assert!(matches!(
            WatchError::from(CoordinationError::InvalidKey),
            WatchError::Coordination(CoordinationError::InvalidKey)
        ));

        let constructed = WatchEvent::from_parts(
            revision(4),
            String::from("metadata/key"),
            Some(String::from("value")),
            WatchEventKind::Put,
        );
        assert_eq!(constructed.key(), "metadata/key");
        assert_eq!(constructed.value(), Some("value"));
    }
}
