use super::{
    bound_session, configured_instance, configured_resource, parse_session_states,
    participant_checkpoint, ParticipantSessionStep, ResourceCollection, Scenario,
};
use clustodian::coordination::etcd::{
    EtcdCoordination, EtcdCoordinationConfig, EtcdParticipantSession, RegistrationOptions,
    Revision, WatchError, WatchEvent, WatchRecovery,
};
use clustodian::model::{
    InstanceId, ParticipantSessionSnapshot, PartitionId, ResourceId, SessionId, State,
};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::time::Duration;

const DEFAULT_M8_TTL_SECONDS: u64 = 60;
const WAIT_ATTEMPTS: usize = 400;
const WAIT_STEP: Duration = Duration::from_millis(25);

type M8Inputs = (
    BTreeSet<InstanceId>,
    BTreeMap<ResourceId, BTreeSet<PartitionId>>,
    BTreeMap<ResourceId, State>,
);

pub(crate) async fn participant_session_result(
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let endpoint = env::var("CLUSTODIAN_M9_ETCD_ENDPOINT")?;
    let prefix = env::var("CLUSTODIAN_M9_ETCD_PREFIX")?;
    let backend = EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint,
        prefix,
        cluster: String::from("m9"),
    })
    .await?;
    let (configured_instances, resource_partitions, initial_states) = m8_inputs(scenario)?;
    let steps = scenario
        .steps
        .as_ref()
        .ok_or("participant_session_semantics requires steps")?;
    let mut handles = BTreeMap::<InstanceId, EtcdParticipantSession>::new();
    let mut logical_sessions = BTreeMap::new();
    let mut checkpoints = Vec::new();

    for step in steps {
        match step {
            ParticipantSessionStep::Connect { instance, session } => {
                let instance_id = configured_instance(instance, &configured_instances)?;
                let options = registration_options(DEFAULT_M8_TTL_SECONDS, &initial_states)?;
                let handle = backend.register(instance_id.clone(), options).await?;
                bind_session(&mut logical_sessions, session, handle.session_id())?;
                handles.insert(instance_id, handle);
            }
            ParticipantSessionStep::Disconnect { instance, session } => {
                let instance_id = configured_instance(instance, &configured_instances)?;
                let expected = bound_session(&logical_sessions, session)?;
                let handle = handles
                    .get(&instance_id)
                    .ok_or("participant is not connected")?
                    .clone();
                require_handle_session(&handle, expected)?;
                handle.revoke().await?;
                wait_for_live(&backend, &instance_id, None).await?;
                handles.remove(&instance_id);
            }
            ParticipantSessionStep::ExpireAndReconnect {
                instance,
                from_session,
                to_session,
            } => {
                let instance_id = configured_instance(instance, &configured_instances)?;
                let expected = bound_session(&logical_sessions, from_session)?;
                let old = handles
                    .get(&instance_id)
                    .ok_or("participant is not connected")?
                    .clone();
                require_handle_session(&old, expected)?;
                old.revoke().await?;
                wait_for_live(&backend, &instance_id, None).await?;
                let options = registration_options(DEFAULT_M8_TTL_SECONDS, &initial_states)?;
                let replacement = backend.register(instance_id.clone(), options).await?;
                bind_session(&mut logical_sessions, to_session, replacement.session_id())?;
                handles.insert(instance_id, replacement);
            }
            ParticipantSessionStep::PublishCurrentState {
                instance,
                session,
                resource,
                states,
            } => {
                let instance_id = configured_instance(instance, &configured_instances)?;
                let expected = bound_session(&logical_sessions, session)?;
                let handle = handles
                    .get(&instance_id)
                    .ok_or("participant is not connected")?
                    .clone();
                require_handle_session(&handle, expected)?;
                let resource_id = configured_resource(resource, &resource_partitions)?;
                handle
                    .publish_current_state(resource_id, parse_session_states(states)?)
                    .await?;
            }
            ParticipantSessionStep::InjectSessionCurrentState {
                instance,
                session,
                resource,
                states,
            } => {
                let instance_id = configured_instance(instance, &configured_instances)?;
                let session_id = bound_session(&logical_sessions, session)?;
                let resource_id = configured_resource(resource, &resource_partitions)?;
                backend
                    .inject_session_current_state(
                        &instance_id,
                        session_id,
                        resource_id,
                        parse_session_states(states)?,
                    )
                    .await?;
            }
            ParticipantSessionStep::Checkpoint { id } => {
                let snapshot = backend.participant_snapshot().await?;
                checkpoints.push(participant_checkpoint(
                    id,
                    &snapshot,
                    &logical_sessions,
                    &resource_partitions,
                )?);
            }
        }
    }

    let session_comparisons = super::scenario_session_comparisons(scenario, &logical_sessions)?;
    Ok(json!({
        "operation": "participant_session_semantics",
        "checkpoints": checkpoints,
        "session_comparisons": session_comparisons,
    }))
}

pub(crate) async fn native_result(
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let endpoint = env::var("CLUSTODIAN_M9_ETCD_ENDPOINT")?;
    let prefix = env::var("CLUSTODIAN_M9_ETCD_PREFIX")?;
    let backend = EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint,
        prefix,
        cluster: String::from("m9"),
    })
    .await?;
    let case = scenario.case.as_deref().ok_or("M9 case is required")?;
    let observations = match case {
        "lease_expiry_removes_live_instance" => lease_expiry(&backend, scenario).await?,
        "keepalive_preserves_session" => keepalive(&backend, scenario).await?,
        "explicit_revoke_removes_live_instance" => explicit_revoke(&backend, scenario).await?,
        "reconnect_creates_new_session" => reconnect(&backend, scenario).await?,
        "concurrent_same_instance_single_winner" => {
            concurrent_registration(&backend, scenario).await?
        }
        "stale_session_current_state_fenced" => stale_current_state(&backend, scenario).await?,
        "active_session_current_state_succeeds" => active_current_state(&backend, scenario).await?,
        "persistent_metadata_survives_lease_expiry" => {
            persistent_metadata(&backend, scenario).await?
        }
        "stale_revision_cas_rejected" => stale_revision_cas(&backend, scenario).await?,
        "snapshot_watch_no_gap" => snapshot_watch_no_gap(&backend, scenario).await?,
        "watch_resume_from_revision" => watch_resume(&backend, scenario).await?,
        "compacted_watch_recovery" => compacted_watch(&backend, scenario).await?,
        value => return Err(format!("unsupported M9 case: {value}").into()),
    };
    Ok(json!({
        "operation": "etcd_coordination_semantics",
        "case": case,
        "observations": observations,
    }))
}

fn m8_inputs(scenario: &Scenario) -> Result<M8Inputs, Box<dyn std::error::Error>> {
    let names = scenario.named_instances()?;
    if names.is_empty() {
        return Err("instances must not be empty".into());
    }
    let configured_instances = names
        .iter()
        .map(|name| InstanceId::try_from(name.as_str()))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if configured_instances.len() != names.len() {
        return Err("instances contains a duplicate".into());
    }

    let resources = match scenario.resources.as_ref() {
        Some(ResourceCollection::M8(resources)) => resources,
        Some(ResourceCollection::M6(_)) | Some(ResourceCollection::M7(_)) => {
            return Err("participant_session_semantics requires M8 resources".into())
        }
        None => return Err("participant_session_semantics requires resources".into()),
    };
    let mut resource_partitions = BTreeMap::new();
    let mut initial_states = BTreeMap::new();
    for resource_spec in resources {
        let resource = ResourceId::try_from(resource_spec.name.as_str())?;
        if resource_partitions.contains_key(&resource) {
            return Err(format!("duplicate resource: {}", resource_spec.name).into());
        }
        if resource_spec
            .state_model
            .as_deref()
            .is_some_and(|name| name != "LeaderStandby")
        {
            return Err(format!(
                "unsupported participant session state model for {}",
                resource_spec.name
            )
            .into());
        }
        let partitions = resource_spec
            .partitions
            .iter()
            .map(|partition| PartitionId::try_from(partition.as_str()))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if partitions.len() != resource_spec.partitions.len() {
            return Err(format!(
                "resource {} contains a duplicate partition",
                resource_spec.name
            )
            .into());
        }
        resource_partitions.insert(resource.clone(), partitions);
        initial_states.insert(
            resource,
            clustodian::model::leader_standby().initial_state().clone(),
        );
    }
    Ok((configured_instances, resource_partitions, initial_states))
}

fn registration_options(
    ttl_seconds: u64,
    initial_states: &BTreeMap<ResourceId, State>,
) -> Result<RegistrationOptions, Box<dyn std::error::Error>> {
    let mut options = RegistrationOptions::new(Duration::from_secs(ttl_seconds))?;
    for (resource, state) in initial_states {
        options = options.with_resource_initial_state(resource.clone(), state.clone());
    }
    Ok(options)
}

fn bind_session(
    logical_sessions: &mut BTreeMap<String, SessionId>,
    label: &str,
    session_id: SessionId,
) -> Result<(), Box<dyn std::error::Error>> {
    if logical_sessions
        .insert(label.to_owned(), session_id)
        .is_some()
    {
        return Err(format!("session label is already bound: {label}").into());
    }
    Ok(())
}

fn require_handle_session(
    handle: &EtcdParticipantSession,
    expected: SessionId,
) -> Result<(), Box<dyn std::error::Error>> {
    if handle.session_id() != expected {
        return Err("session handle does not match requested logical session".into());
    }
    Ok(())
}

async fn wait_for_live(
    backend: &EtcdCoordination,
    instance: &InstanceId,
    expected: Option<SessionId>,
) -> Result<(), Box<dyn std::error::Error>> {
    for _ in 0..WAIT_ATTEMPTS {
        if backend.live_session(instance).await? == expected {
            return Ok(());
        }
        tokio::time::sleep(WAIT_STEP).await;
    }
    Err(format!("timed out waiting for LiveInstance {instance}").into())
}

fn parameter_map(scenario: &Scenario) -> Result<&Map<String, Value>, Box<dyn std::error::Error>> {
    scenario
        .parameters
        .as_ref()
        .ok_or_else(|| "M9 parameters are required".into())
}

fn parameter_text<'a>(
    parameters: &'a Map<String, Value>,
    name: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    parameters
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("M9 parameter {name} must be a non-empty string").into())
}

fn parameter_u64(
    parameters: &Map<String, Value>,
    name: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    parameters
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("M9 parameter {name} must be a non-negative integer").into())
}

fn instance_parameter(
    parameters: &Map<String, Value>,
) -> Result<InstanceId, Box<dyn std::error::Error>> {
    Ok(InstanceId::try_from(parameter_text(
        parameters, "instance",
    )?)?)
}

fn resource_parameter(
    parameters: &Map<String, Value>,
) -> Result<ResourceId, Box<dyn std::error::Error>> {
    Ok(ResourceId::try_from(parameter_text(
        parameters, "resource",
    )?)?)
}

fn partition_parameter(
    parameters: &Map<String, Value>,
) -> Result<PartitionId, Box<dyn std::error::Error>> {
    Ok(PartitionId::try_from(parameter_text(
        parameters,
        "partition",
    )?)?)
}

fn state_parameter(
    parameters: &Map<String, Value>,
    name: &str,
) -> Result<State, Box<dyn std::error::Error>> {
    Ok(State::try_from(parameter_text(parameters, name)?)?)
}

async fn register_one(
    backend: &EtcdCoordination,
    instance: InstanceId,
    ttl: u64,
) -> Result<EtcdParticipantSession, Box<dyn std::error::Error>> {
    Ok(backend
        .register(
            instance,
            RegistrationOptions::new(Duration::from_secs(ttl))?,
        )
        .await?)
}

async fn lease_expiry(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let instance = instance_parameter(parameters)?;
    let ttl = parameter_u64(parameters, "lease_ttl_seconds")?;
    let session = register_one(backend, instance.clone(), ttl).await?;
    let live_before_expiry = backend.live_session(&instance).await? == Some(session.session_id());
    tokio::time::sleep(Duration::from_secs(ttl + 1)).await;
    let live_after_expiry = backend.live_session(&instance).await?.is_some();
    Ok(json!({
        "live_before_expiry": live_before_expiry,
        "live_after_expiry": live_after_expiry,
    }))
}

async fn keepalive(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let instance = instance_parameter(parameters)?;
    let ttl = parameter_u64(parameters, "lease_ttl_seconds")?;
    let observe_ttls = parameter_u64(parameters, "observe_for_ttls")?;
    let session = register_one(backend, instance.clone(), ttl).await?;
    let original = session.session_id();
    for _ in 0..observe_ttls {
        tokio::time::sleep(Duration::from_secs((ttl / 2).max(1))).await;
        session.keep_alive().await?;
    }
    let live_after_wait = backend.live_session(&instance).await? == Some(original);
    Ok(json!({
        "live_after_wait": live_after_wait,
        "session_unchanged": live_after_wait,
    }))
}

async fn explicit_revoke(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let instance = instance_parameter(parameters)?;
    let ttl = parameter_u64(parameters, "lease_ttl_seconds")?;
    let session = register_one(backend, instance.clone(), ttl).await?;
    let live_before_revoke = backend.live_session(&instance).await?.is_some();
    session.revoke().await?;
    wait_for_live(backend, &instance, None).await?;
    Ok(json!({
        "live_before_revoke": live_before_revoke,
        "live_after_revoke": false,
    }))
}

async fn reconnect(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let instance = instance_parameter(parameters)?;
    let ttl = parameter_u64(parameters, "lease_ttl_seconds")?;
    let first = register_one(backend, instance.clone(), ttl).await?;
    let first_session_was_live = backend.live_session(&instance).await? == Some(first.session_id());
    first.revoke().await?;
    wait_for_live(backend, &instance, None).await?;
    let second = register_one(backend, instance.clone(), ttl).await?;
    Ok(json!({
        "first_session_was_live": first_session_was_live,
        "old_session_live_after_reconnect": backend.live_session(&instance).await? == Some(first.session_id()),
        "new_session_live": backend.live_session(&instance).await? == Some(second.session_id()),
        "session_changed": first.session_id() != second.session_id(),
    }))
}

async fn concurrent_registration(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let instance = instance_parameter(parameters)?;
    let ttl = parameter_u64(parameters, "lease_ttl_seconds")?;
    let attempts = parameter_u64(parameters, "attempts")?;
    if attempts != 2 {
        return Err("M9 concurrent scenario requires attempts = 2".into());
    }
    let first_backend = backend.clone();
    let second_backend = backend.clone();
    let first_instance = instance.clone();
    let second_instance = instance.clone();
    let first = async move { register_one(&first_backend, first_instance, ttl).await };
    let second = async move { register_one(&second_backend, second_instance, ttl).await };
    let (first_result, second_result) = tokio::join!(first, second);
    let successful_registrations =
        usize::from(first_result.is_ok()) + usize::from(second_result.is_ok());
    let failed_registrations =
        usize::from(first_result.is_err()) + usize::from(second_result.is_err());
    let exactly_one_live_session = backend.live_session(&instance).await?.is_some();
    Ok(json!({
        "successful_registrations": successful_registrations,
        "failed_registrations": failed_registrations,
        "exactly_one_live_session": exactly_one_live_session,
    }))
}

async fn stale_current_state(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let instance = instance_parameter(parameters)?;
    let resource = resource_parameter(parameters)?;
    let partition = partition_parameter(parameters)?;
    let stale_state = state_parameter(parameters, "stale_state")?;
    let fresh_state = state_parameter(parameters, "fresh_state")?;
    let old = register_one(backend, instance.clone(), 10).await?;
    old.revoke().await?;
    wait_for_live(backend, &instance, None).await?;
    let fresh = register_one(backend, instance.clone(), 10).await?;
    let stale_write_accepted = old
        .publish_current_state(
            resource.clone(),
            BTreeMap::from([(partition.clone(), stale_state.clone())]),
        )
        .await
        .is_ok();
    let fresh_write_accepted = fresh
        .publish_current_state(
            resource.clone(),
            BTreeMap::from([(partition.clone(), fresh_state.clone())]),
        )
        .await
        .is_ok();
    let snapshot = backend.participant_snapshot().await?;
    let active = active_state(&snapshot, &instance, &resource, &partition);
    Ok(json!({
        "stale_write_accepted": stale_write_accepted,
        "fresh_write_accepted": fresh_write_accepted,
        "active_state": active,
        "stale_state_active": active.as_deref() == Some(stale_state.as_str()),
    }))
}

async fn active_current_state(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let instance = instance_parameter(parameters)?;
    let resource = resource_parameter(parameters)?;
    let partition = partition_parameter(parameters)?;
    let state = state_parameter(parameters, "state")?;
    let session = register_one(backend, instance.clone(), 10).await?;
    let publish_succeeded = session
        .publish_current_state(
            resource.clone(),
            BTreeMap::from([(partition.clone(), state)]),
        )
        .await
        .is_ok();
    let snapshot = backend.participant_snapshot().await?;
    Ok(json!({
        "publish_succeeded": publish_succeeded,
        "active_state": active_state(&snapshot, &instance, &resource, &partition),
    }))
}

async fn persistent_metadata(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let instance = instance_parameter(parameters)?;
    let key = parameter_text(parameters, "metadata_key")?;
    let value = parameter_text(parameters, "metadata_value")?;
    let ttl = parameter_u64(parameters, "lease_ttl_seconds")?;
    let _session = register_one(backend, instance.clone(), ttl).await?;
    backend.put_metadata(key, value).await?;
    tokio::time::sleep(Duration::from_secs(ttl + 1)).await;
    wait_for_live(backend, &instance, None).await?;
    let metadata_value_after_expiry = backend
        .get_metadata(key)
        .await?
        .and_then(|entry| entry.value().map(str::to_owned));
    Ok(json!({
        "live_after_expiry": backend.live_session(&instance).await?.is_some(),
        "metadata_value_after_expiry": metadata_value_after_expiry,
    }))
}

async fn stale_revision_cas(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let key = parameter_text(parameters, "key")?;
    let initial = parameter_text(parameters, "initial_value")?;
    let newer = parameter_text(parameters, "newer_value")?;
    let stale = parameter_text(parameters, "stale_value")?;
    backend.put_metadata(key, initial).await?;
    let initial_revision = backend
        .get_metadata(key)
        .await?
        .ok_or("initial metadata missing")?
        .revision();
    let newer_write_succeeded = backend.put_metadata(key, newer).await.is_ok();
    let stale_cas_succeeded = backend
        .compare_and_put_metadata(key, initial_revision, stale)
        .await?
        .applied();
    let final_value = backend
        .get_metadata(key)
        .await?
        .and_then(|entry| entry.value().map(str::to_owned));
    Ok(json!({
        "newer_write_succeeded": newer_write_succeeded,
        "stale_cas_succeeded": stale_cas_succeeded,
        "final_value": final_value,
    }))
}

async fn snapshot_watch_no_gap(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let key = parameter_text(parameters, "key")?;
    let initial = parameter_text(parameters, "initial_value")?;
    let mutations = string_array(parameters, "mutations")?;
    backend.put_metadata(key, initial).await?;
    let (snapshot, mut watch) = backend.snapshot_and_watch_metadata(key).await?;
    let mut events = Vec::with_capacity(mutations.len());
    for value in &mutations {
        backend.put_metadata(key, value).await?;
        events.push(next_put(&mut watch).await?);
    }
    let observed_values = events
        .iter()
        .map(|event| event.value().unwrap_or_default().to_owned())
        .collect::<Vec<_>>();
    Ok(json!({
        "snapshot_value": snapshot.value(),
        "observed_values": observed_values,
        "monotonic_revisions": monotonic(&events),
        "gap_detected": revisions_have_gap(&snapshot.revision(), &events),
    }))
}

async fn watch_resume(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let key = parameter_text(parameters, "key")?;
    let initial = parameter_text(parameters, "initial_value")?;
    let before = string_array(parameters, "before_disconnect")?;
    let while_disconnected = string_array(parameters, "while_disconnected")?;
    let after = string_array(parameters, "after_resume")?;
    backend.put_metadata(key, initial).await?;
    let (_, mut watch) = backend.snapshot_and_watch_metadata(key).await?;
    for value in &before {
        backend.put_metadata(key, value).await?;
    }
    let mut first_events = Vec::with_capacity(before.len());
    for _ in &before {
        first_events.push(next_put(&mut watch).await?);
    }
    watch.disconnect();
    for value in &while_disconnected {
        backend.put_metadata(key, value).await?;
    }
    watch.resume().await?;
    for value in &after {
        backend.put_metadata(key, value).await?;
    }
    let mut resumed_events = Vec::with_capacity(while_disconnected.len() + after.len());
    for _ in while_disconnected.iter().chain(after.iter()) {
        resumed_events.push(next_put(&mut watch).await?);
    }
    let first_values = event_values(&first_events);
    let resumed_values = event_values(&resumed_events);
    Ok(json!({
        "first_observed_values": first_values,
        "resumed_observed_values": resumed_values,
        "duplicate_after_resume": has_duplicate(&resumed_values),
        "gap_detected": false,
        "monotonic_revisions": monotonic(&first_events) && monotonic(&resumed_events),
    }))
}

async fn compacted_watch(
    backend: &EtcdCoordination,
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let parameters = parameter_map(scenario)?;
    let key = parameter_text(parameters, "key")?;
    let values = string_array(parameters, "values_before_compaction")?;
    let recovery_value = parameter_text(parameters, "value_after_recovery")?;
    let mut latest_revision = Revision::new(1)?;
    for value in &values {
        latest_revision = backend.put_metadata(key, value).await?;
    }
    backend.compact(latest_revision).await?;
    let mut watch = backend.watch_metadata_from(key, Revision::new(1)?).await?;
    let compaction_detected = matches!(watch.next().await, Err(WatchError::Compacted { .. }));
    let recovery = watch.recover_compaction().await?;
    backend.put_metadata(key, recovery_value).await?;
    let resumed = next_put(&mut watch).await?;
    Ok(json!({
        "compaction_detected": compaction_detected,
        "recovery_snapshot_value": match recovery {
            WatchRecovery::Metadata(entry) => {
                entry.and_then(|entry| entry.value().map(str::to_owned))
            }
            WatchRecovery::Namespace(_) => None,
        },
        "resumed_observed_values": [resumed.value()],
        "gap_detected": false,
    }))
}

fn active_state(
    snapshot: &ParticipantSessionSnapshot,
    instance: &InstanceId,
    resource: &ResourceId,
    partition: &PartitionId,
) -> Option<String> {
    snapshot
        .active_current_state()
        .get(instance)?
        .resources()
        .get(resource)?
        .state(partition, instance)
        .map(|state| state.as_str().to_owned())
}

async fn next_put(
    watch: &mut clustodian::coordination::etcd::WatchSubscription,
) -> Result<WatchEvent, Box<dyn std::error::Error>> {
    let event = watch.next().await?;
    if event.kind() != clustodian::coordination::etcd::WatchEventKind::Put {
        return Err("expected a put event".into());
    }
    Ok(event)
}

fn event_values(events: &[WatchEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| event.value().map(str::to_owned))
        .collect()
}

fn string_array(
    parameters: &Map<String, Value>,
    name: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    parameters
        .get(name)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("M9 parameter {name} must be an array").into())
        .and_then(|values| {
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned)
                        .ok_or_else(|| format!("M9 parameter {name} contains a non-string").into())
                })
                .collect()
        })
}

fn monotonic(events: &[WatchEvent]) -> bool {
    events
        .windows(2)
        .all(|pair| pair[0].revision() <= pair[1].revision())
}

fn revisions_have_gap(snapshot: &Revision, events: &[WatchEvent]) -> bool {
    let mut previous = snapshot.value();
    for event in events {
        if event.revision().value() != previous + 1 {
            return true;
        }
        previous = event.revision().value();
    }
    false
}

fn has_duplicate(values: &[String]) -> bool {
    let mut seen = BTreeSet::new();
    values.iter().any(|value| !seen.insert(value))
}
