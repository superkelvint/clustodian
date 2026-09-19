use super::{CurrentState, InstanceId, PartitionId, ResourceId, SessionId, State};
use std::collections::BTreeMap;
use std::fmt;

/// The live incarnation of one participant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveInstance {
    instance_id: InstanceId,
    session_id: SessionId,
}

impl LiveInstance {
    pub(crate) fn new(instance_id: InstanceId, session_id: SessionId) -> Self {
        Self {
            instance_id,
            session_id,
        }
    }

    /// Return the stable participant identity.
    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    /// Return the current participant incarnation.
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }
}

/// CurrentState records belonging to one live participant incarnation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveCurrentState {
    session_id: SessionId,
    resources: BTreeMap<ResourceId, CurrentState>,
}

impl ActiveCurrentState {
    pub(crate) fn new(
        session_id: SessionId,
        resources: BTreeMap<ResourceId, CurrentState>,
    ) -> Self {
        Self {
            session_id,
            resources,
        }
    }

    /// Return the session that owns these active records.
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Return the resource-scoped CurrentState records.
    pub fn resources(&self) -> &BTreeMap<ResourceId, CurrentState> {
        &self.resources
    }
}

/// Immutable semantic view of participant liveness and active CurrentState.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParticipantSessionSnapshot {
    live_instances: BTreeMap<InstanceId, LiveInstance>,
    active_current_state: BTreeMap<InstanceId, ActiveCurrentState>,
}

impl ParticipantSessionSnapshot {
    pub(crate) fn from_parts(
        live_instances: BTreeMap<InstanceId, LiveInstance>,
        active_current_state: BTreeMap<InstanceId, ActiveCurrentState>,
    ) -> Self {
        Self {
            live_instances,
            active_current_state,
        }
    }

    /// Return the currently live participant incarnations.
    pub fn live_instances(&self) -> &BTreeMap<InstanceId, LiveInstance> {
        &self.live_instances
    }

    /// Return CurrentState selected for the currently live session of each instance.
    pub fn active_current_state(&self) -> &BTreeMap<InstanceId, ActiveCurrentState> {
        &self.active_current_state
    }
}

/// Participant/session model with backend-independent session fencing.
#[derive(Clone, Debug)]
pub struct ParticipantSessionState {
    next_session_sequence: u64,
    known_sessions: BTreeMap<SessionId, InstanceId>,
    live_instances: BTreeMap<InstanceId, LiveInstance>,
    current_states: BTreeMap<InstanceId, BTreeMap<SessionId, BTreeMap<ResourceId, CurrentState>>>,
    default_initial_state: State,
    resource_initial_states: BTreeMap<ResourceId, State>,
}

impl ParticipantSessionState {
    /// Create an empty participant/session model.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure the state-model initial state for a resource.
    ///
    /// Helix carries partition membership into a replacement participant
    /// session and resets each carried partition to its state model's initial
    /// state. M8 scenarios use `LeaderStandby` (`OFFLINE`), while this hook
    /// keeps the participant model independent of a particular state model.
    pub fn set_resource_initial_state(&mut self, resource_id: ResourceId, initial_state: State) {
        self.resource_initial_states
            .insert(resource_id, initial_state);
    }

    /// Start a new participant incarnation.
    pub fn connect(&mut self, instance_id: InstanceId) -> Result<SessionId, SessionError> {
        if self.live_instances.contains_key(&instance_id) {
            return Err(SessionError::AlreadyLive(instance_id));
        }

        let carried_states = self.carried_current_states(&instance_id, None)?;
        let session_id = self.allocate_session(&instance_id)?;
        self.live_instances.insert(
            instance_id.clone(),
            LiveInstance::new(instance_id.clone(), session_id),
        );
        self.current_states
            .entry(instance_id)
            .or_default()
            .insert(session_id, carried_states);
        Ok(session_id)
    }

    /// Gracefully remove a participant's LiveInstance while retaining its metadata.
    pub fn disconnect(
        &mut self,
        instance_id: &InstanceId,
        session_id: SessionId,
    ) -> Result<(), SessionError> {
        self.require_live_session(instance_id, session_id)?;
        self.live_instances.remove(instance_id);
        Ok(())
    }

    /// Replace the current incarnation after a real or modeled session expiry.
    pub fn expire_and_reconnect(
        &mut self,
        instance_id: &InstanceId,
        old_session_id: SessionId,
    ) -> Result<SessionId, SessionError> {
        self.require_live_session(instance_id, old_session_id)?;
        let carried_states = self.carried_current_states(instance_id, Some(old_session_id))?;
        let new_session_id = self.allocate_session(instance_id)?;
        self.live_instances.insert(
            instance_id.clone(),
            LiveInstance::new(instance_id.clone(), new_session_id),
        );
        self.current_states
            .entry(instance_id.clone())
            .or_default()
            .insert(new_session_id, carried_states);
        Ok(new_session_id)
    }

    /// Publish CurrentState for the currently live session of an instance.
    pub fn publish_current_state(
        &mut self,
        instance_id: &InstanceId,
        session_id: SessionId,
        resource_id: ResourceId,
        states: BTreeMap<PartitionId, State>,
    ) -> Result<(), SessionError> {
        self.require_live_session(instance_id, session_id)?;
        self.store_current_state(instance_id, session_id, resource_id, states)
    }

    /// Inject persisted CurrentState under any session previously created here.
    ///
    /// This is intentionally broader than [`Self::publish_current_state`] so
    /// tests can retain stale records and prove they remain fenced.
    pub fn inject_session_current_state(
        &mut self,
        instance_id: &InstanceId,
        session_id: SessionId,
        resource_id: ResourceId,
        states: BTreeMap<PartitionId, State>,
    ) -> Result<(), SessionError> {
        match self.known_sessions.get(&session_id) {
            None => return Err(SessionError::UnknownSession(session_id)),
            Some(owner) if owner != instance_id => {
                return Err(SessionError::SessionOwnershipMismatch {
                    instance_id: instance_id.clone(),
                    session_id,
                    owner: owner.clone(),
                });
            }
            Some(_) => {}
        }
        self.store_current_state(instance_id, session_id, resource_id, states)
    }

    /// Derive the active session-aware view without exposing stale metadata.
    pub fn snapshot(&self) -> ParticipantSessionSnapshot {
        let mut active_current_state = BTreeMap::new();
        for (instance_id, live_instance) in &self.live_instances {
            let resources = self
                .current_states
                .get(instance_id)
                .and_then(|sessions| sessions.get(&live_instance.session_id))
                .cloned()
                .unwrap_or_default();
            active_current_state.insert(
                instance_id.clone(),
                ActiveCurrentState {
                    session_id: live_instance.session_id,
                    resources,
                },
            );
        }

        ParticipantSessionSnapshot {
            live_instances: self.live_instances.clone(),
            active_current_state,
        }
    }

    fn allocate_session(&mut self, instance_id: &InstanceId) -> Result<SessionId, SessionError> {
        self.next_session_sequence = self
            .next_session_sequence
            .checked_add(1)
            .ok_or(SessionError::SessionIdExhausted)?;
        let session_id = SessionId::from_sequence(self.next_session_sequence);
        self.known_sessions.insert(session_id, instance_id.clone());
        Ok(session_id)
    }

    fn require_live_session(
        &self,
        instance_id: &InstanceId,
        session_id: SessionId,
    ) -> Result<(), SessionError> {
        match self.live_instances.get(instance_id) {
            Some(live_instance) if live_instance.session_id == session_id => Ok(()),
            Some(live_instance) => Err(SessionError::SessionMismatch {
                instance_id: instance_id.clone(),
                expected: live_instance.session_id,
                actual: session_id,
            }),
            None => Err(SessionError::NotLive(instance_id.clone())),
        }
    }

    fn store_current_state(
        &mut self,
        instance_id: &InstanceId,
        session_id: SessionId,
        resource_id: ResourceId,
        states: BTreeMap<PartitionId, State>,
    ) -> Result<(), SessionError> {
        let existing = self
            .current_states
            .get(instance_id)
            .and_then(|sessions| sessions.get(&session_id))
            .and_then(|resources| resources.get(&resource_id));
        let mut merged_states = BTreeMap::new();
        if let Some(existing) = existing {
            for (partition_id, instances) in existing.entries() {
                for (existing_instance, state) in instances {
                    merged_states.insert(
                        (partition_id.clone(), existing_instance.clone()),
                        state.clone(),
                    );
                }
            }
        }
        for (partition_id, state) in states {
            merged_states.insert((partition_id, instance_id.clone()), state);
        }

        let mut builder = CurrentState::builder();
        for ((partition_id, stored_instance), state) in merged_states {
            builder
                .set_state(partition_id, stored_instance, state)
                .map_err(SessionError::CurrentState)?;
        }
        let current_state = builder.build();
        self.current_states
            .entry(instance_id.clone())
            .or_default()
            .entry(session_id)
            .or_default()
            .insert(resource_id, current_state);
        Ok(())
    }

    fn carried_current_states(
        &self,
        instance_id: &InstanceId,
        previous_session_id: Option<SessionId>,
    ) -> Result<BTreeMap<ResourceId, CurrentState>, SessionError> {
        let previous_resources =
            self.current_states
                .get(instance_id)
                .and_then(|sessions| match previous_session_id {
                    Some(session_id) => sessions.get(&session_id),
                    None => sessions.iter().next_back().map(|(_, resources)| resources),
                });
        let Some(previous_resources) = previous_resources else {
            return Ok(BTreeMap::new());
        };

        let mut carried = BTreeMap::new();
        for (resource_id, current_state) in previous_resources {
            let initial_state = self
                .resource_initial_states
                .get(resource_id)
                .unwrap_or(&self.default_initial_state);
            let mut builder = CurrentState::builder();
            for (partition_id, instances) in current_state.entries() {
                for (stored_instance, state) in instances {
                    let carried_state = if stored_instance == instance_id {
                        initial_state.clone()
                    } else {
                        state.clone()
                    };
                    builder
                        .set_state(partition_id.clone(), stored_instance.clone(), carried_state)
                        .map_err(SessionError::CurrentState)?;
                }
            }
            carried.insert(resource_id.clone(), builder.build());
        }
        Ok(carried)
    }
}

impl Default for ParticipantSessionState {
    fn default() -> Self {
        Self {
            next_session_sequence: 0,
            known_sessions: BTreeMap::new(),
            live_instances: BTreeMap::new(),
            current_states: BTreeMap::new(),
            default_initial_state: State::try_from("OFFLINE")
                .expect("the built-in reconnect initial state is valid"),
            resource_initial_states: BTreeMap::new(),
        }
    }
}

/// Errors raised by participant/session lifecycle or state publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionError {
    AlreadyLive(InstanceId),
    NotLive(InstanceId),
    SessionIdExhausted,
    SessionMismatch {
        instance_id: InstanceId,
        expected: SessionId,
        actual: SessionId,
    },
    SessionOwnershipMismatch {
        instance_id: InstanceId,
        session_id: SessionId,
        owner: InstanceId,
    },
    UnknownSession(SessionId),
    CurrentState(super::ReplicaStateError),
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyLive(instance) => write!(formatter, "instance {instance} is already live"),
            Self::NotLive(instance) => write!(formatter, "instance {instance} is not live"),
            Self::SessionIdExhausted => formatter.write_str("session identifier space exhausted"),
            Self::SessionMismatch { instance_id, .. } => {
                write!(
                    formatter,
                    "session does not own live instance {instance_id}"
                )
            }
            Self::SessionOwnershipMismatch {
                instance_id,
                session_id,
                owner,
            } => write!(
                formatter,
                "session {session_id:?} belongs to {owner}, not {instance_id}"
            ),
            Self::UnknownSession(_) => formatter.write_str("session is unknown to this model"),
            Self::CurrentState(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for SessionError {}

#[cfg(test)]
mod tests {
    use super::{ParticipantSessionState, SessionError};
    use crate::model::{InstanceId, PartitionId, ResourceId, State};
    use std::collections::BTreeMap;

    fn id<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: std::fmt::Debug,
    {
        value.to_owned().try_into().unwrap()
    }

    fn states(entries: &[(&str, &str)]) -> BTreeMap<PartitionId, State> {
        entries
            .iter()
            .map(|(partition, state)| (id(partition), id(state)))
            .collect()
    }

    #[test]
    fn reconnect_fences_old_state_and_retains_stale_metadata() {
        let instance = id::<InstanceId>("node-a");
        let resource = id::<ResourceId>("documents");
        let mut model = ParticipantSessionState::new();
        let first = model.connect(instance.clone()).unwrap();
        model
            .publish_current_state(
                &instance,
                first,
                resource.clone(),
                states(&[("documents_0", "LEADER")]),
            )
            .unwrap();

        let second = model.expire_and_reconnect(&instance, first).unwrap();
        let snapshot = model.snapshot();
        assert_ne!(first, second);
        assert_eq!(snapshot.live_instances()[&instance].session_id(), second);
        assert_eq!(
            snapshot.active_current_state()[&instance].resources()[&resource]
                .state(&id("documents_0"), &instance)
                .expect("carried partition state")
                .as_str(),
            "OFFLINE"
        );

        model
            .inject_session_current_state(
                &instance,
                first,
                resource.clone(),
                states(&[("documents_0", "LEADER")]),
            )
            .unwrap();
        assert_eq!(
            model.snapshot().active_current_state()[&instance].resources()[&resource]
                .state(&id("documents_0"), &instance)
                .expect("carried partition state")
                .as_str(),
            "OFFLINE"
        );
    }

    #[test]
    fn disconnect_removes_live_state_but_does_not_delete_metadata() {
        let instance = id::<InstanceId>("node-a");
        let mut model = ParticipantSessionState::new();
        let session = model.connect(instance.clone()).unwrap();
        model
            .publish_current_state(
                &instance,
                session,
                id("documents"),
                states(&[("documents_0", "STANDBY")]),
            )
            .unwrap();
        model.disconnect(&instance, session).unwrap();
        let snapshot = model.snapshot();
        assert!(snapshot.live_instances().is_empty());
        assert!(snapshot.active_current_state().is_empty());
    }

    #[test]
    fn participants_receive_independent_sessions() {
        let mut model = ParticipantSessionState::new();
        let first = model.connect(id("node-a")).unwrap();
        let second = model.connect(id("node-b")).unwrap();
        assert_ne!(first, second);
        let snapshot = model.snapshot();
        assert_eq!(
            snapshot.live_instances()[&id("node-a")].instance_id(),
            &id("node-a")
        );
        assert_eq!(snapshot.live_instances()[&id("node-a")].session_id(), first);
        assert_eq!(
            snapshot.active_current_state()[&id("node-a")].session_id(),
            first
        );
        assert!(snapshot.active_current_state()[&id("node-a")]
            .resources()
            .is_empty());
    }

    #[test]
    fn session_errors_have_stable_operator_messages() {
        let node = id::<InstanceId>("node-a");
        let owner = id::<InstanceId>("node-b");
        let session = super::SessionId::from_wire_value(3);
        let errors = [
            SessionError::AlreadyLive(node.clone()),
            SessionError::NotLive(node.clone()),
            SessionError::SessionIdExhausted,
            SessionError::SessionMismatch {
                instance_id: node.clone(),
                expected: session,
                actual: super::SessionId::from_wire_value(4),
            },
            SessionError::SessionOwnershipMismatch {
                instance_id: node,
                session_id: session,
                owner,
            },
            SessionError::UnknownSession(session),
        ];
        assert!(errors.iter().all(|error| !error.to_string().is_empty()));
    }

    #[test]
    fn stale_state_injection_requires_session_ownership() {
        let node_a = id::<InstanceId>("node-a");
        let node_b = id::<InstanceId>("node-b");
        let mut model = ParticipantSessionState::new();
        let session_a = model.connect(node_a.clone()).unwrap();
        model.connect(node_b.clone()).unwrap();

        assert!(matches!(
            model.inject_session_current_state(
                &node_b,
                session_a,
                id("documents"),
                states(&[("documents_0", "LEADER")]),
            ),
            Err(SessionError::SessionOwnershipMismatch {
                instance_id,
                session_id,
                owner,
            }) if instance_id == node_b && session_id == session_a && owner == node_a
        ));
        assert!(model
            .snapshot()
            .active_current_state()
            .contains_key(&node_b));
        assert!(model.snapshot().active_current_state()[&node_b]
            .resources()
            .is_empty());
    }

    #[test]
    fn publication_requires_the_live_session() {
        let instance = id::<InstanceId>("node-a");
        let mut model = ParticipantSessionState::new();
        let first = model.connect(instance.clone()).unwrap();
        let second = model.expire_and_reconnect(&instance, first).unwrap();
        assert!(matches!(
            model.publish_current_state(
                &instance,
                first,
                id("documents"),
                states(&[("documents_0", "LEADER")]),
            ),
            Err(SessionError::SessionMismatch { .. })
        ));
        assert!(model
            .publish_current_state(
                &instance,
                second,
                id("documents"),
                states(&[("documents_0", "STANDBY")]),
            )
            .is_ok());
    }

    #[test]
    fn reconnect_uses_configured_initial_state_and_merges_partial_publication() {
        let instance = id::<InstanceId>("node-a");
        let resource = id::<ResourceId>("documents");
        let mut model = ParticipantSessionState::new();
        model.set_resource_initial_state(resource.clone(), id("INITIAL"));
        let first = model.connect(instance.clone()).unwrap();
        model
            .publish_current_state(
                &instance,
                first,
                resource.clone(),
                states(&[("documents_0", "LEADER"), ("documents_1", "STANDBY")]),
            )
            .unwrap();

        let second = model.expire_and_reconnect(&instance, first).unwrap();
        let snapshot = model.snapshot();
        let current_state = &snapshot.active_current_state()[&instance].resources()[&resource];
        assert_eq!(
            current_state
                .state(&id("documents_0"), &instance)
                .unwrap()
                .as_str(),
            "INITIAL"
        );
        assert_eq!(
            current_state
                .state(&id("documents_1"), &instance)
                .unwrap()
                .as_str(),
            "INITIAL"
        );

        model
            .publish_current_state(
                &instance,
                second,
                resource.clone(),
                states(&[("documents_0", "STANDBY")]),
            )
            .unwrap();
        let snapshot = model.snapshot();
        let current_state = &snapshot.active_current_state()[&instance].resources()[&resource];
        assert_eq!(
            current_state
                .state(&id("documents_0"), &instance)
                .unwrap()
                .as_str(),
            "STANDBY"
        );
        assert_eq!(
            current_state
                .state(&id("documents_1"), &instance)
                .unwrap()
                .as_str(),
            "INITIAL"
        );
    }
}
