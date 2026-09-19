use clustodian::controller::PublishedTransition;
use clustodian::coordination::etcd::{
    EtcdCoordination, EtcdCoordinationConfig, RegistrationOptions,
};
use clustodian::model::{InstanceId, PartitionId, ResourceId, SessionId, State};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

const INSTANCE_CONFIGS_KEY: &str = "controller/instance-configs";
const THROTTLES_KEY: &str = "controller/throttles";

#[derive(Debug, Deserialize)]
struct Scenario {
    operation: String,
    #[serde(default)]
    instance_configs: Vec<InstanceConfigInput>,
    #[serde(default)]
    setup: Vec<Operation>,
    #[serde(default)]
    steps: Vec<Operation>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct InstanceConfigInput {
    name: String,
    zone: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op")]
enum Operation {
    #[serde(rename = "put_resource")]
    PutResource { resource: Value },
    #[serde(rename = "connect")]
    Connect { instance: String, session: String },
    #[serde(rename = "disconnect")]
    Disconnect { instance: String, session: String },
    #[serde(rename = "expire_and_reconnect")]
    ExpireAndReconnect {
        instance: String,
        from_session: String,
        to_session: String,
    },
    #[serde(rename = "publish_current_state")]
    PublishCurrentState {
        instance: String,
        session: String,
        resource: String,
        states: BTreeMap<String, String>,
    },
    #[serde(rename = "set_transition_throttle")]
    SetTransitionThrottle {
        scope: String,
        rebalance_type: String,
        max_in_flight: usize,
    },
    #[serde(rename = "clear_transition_throttles")]
    ClearTransitionThrottles,
    #[serde(rename = "checkpoint")]
    Checkpoint { id: String },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SessionBinding {
    instance: String,
    session_id: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct StateFile {
    sessions: BTreeMap<String, SessionBinding>,
    boundary_revision: i64,
}

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(mode) = args.next() else {
        eprintln!("usage: controller-scenario <prepare|run> <scenario> <state-file>");
        return ExitCode::from(2);
    };
    let Some(scenario_path) = args.next() else {
        eprintln!("missing scenario path");
        return ExitCode::from(2);
    };
    let Some(state_path) = args.next() else {
        eprintln!("missing state path");
        return ExitCode::from(2);
    };
    if args.next().is_some() || !matches!(mode.as_str(), "prepare" | "run") {
        eprintln!("usage: controller-scenario <prepare|run> <scenario> <state-file>");
        return ExitCode::from(2);
    }

    let endpoint = match env::var("CLUSTODIAN_M10_ETCD_ENDPOINT") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("CLUSTODIAN_M10_ETCD_ENDPOINT is required");
            return ExitCode::from(2);
        }
    };
    let prefix = match env::var("CLUSTODIAN_M10_ETCD_PREFIX") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("CLUSTODIAN_M10_ETCD_PREFIX is required");
            return ExitCode::from(2);
        }
    };
    let scenario_text = match fs::read_to_string(&scenario_path) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("failed to read scenario: {error}");
            return ExitCode::from(1);
        }
    };
    let scenario: Scenario = match serde_json::from_str(&scenario_text) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("failed to parse scenario: {error}");
            return ExitCode::from(1);
        }
    };
    if scenario.operation != "controller_runtime_semantics" {
        eprintln!("unsupported scenario operation");
        return ExitCode::from(1);
    }

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to create runtime: {error}");
            return ExitCode::from(1);
        }
    };
    let result = runtime.block_on(async move {
        let backend = EtcdCoordination::connect(EtcdCoordinationConfig {
            endpoint,
            prefix,
            cluster: String::from("m10"),
        })
        .await?;
        match mode.as_str() {
            "prepare" => prepare(&backend, &scenario, Path::new(&state_path)).await,
            "run" => run(&backend, &scenario, Path::new(&state_path)).await,
            _ => unreachable!(),
        }
    });
    match result {
        Ok(Some(value)) => {
            println!(
                "{}",
                serde_json::to_string(&value).expect("result serializes")
            );
            ExitCode::SUCCESS
        }
        Ok(None) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("controller scenario failed: {error}");
            ExitCode::from(1)
        }
    }
}

async fn prepare(
    backend: &EtcdCoordination,
    scenario: &Scenario,
    state_path: &Path,
) -> Result<Option<Value>, Box<dyn std::error::Error>> {
    backend
        .put_metadata(
            INSTANCE_CONFIGS_KEY,
            &serde_json::to_string(&scenario.instance_configs)?,
        )
        .await?;
    let mut state = StateFile {
        sessions: BTreeMap::new(),
        boundary_revision: 0,
    };
    for operation in &scenario.setup {
        let _ = apply_operation(backend, &mut state, operation).await?;
    }
    state.boundary_revision = backend.controller_snapshot().await?.revision().value();
    write_state(state_path, &state)?;
    Ok(None)
}

async fn run(
    backend: &EtcdCoordination,
    scenario: &Scenario,
    state_path: &Path,
) -> Result<Option<Value>, Box<dyn std::error::Error>> {
    let mut state = read_state(state_path)?;
    let mut checkpoints = Vec::new();
    for operation in &scenario.steps {
        match operation {
            Operation::Checkpoint { id } => {
                wait_for_processed(backend, state.boundary_revision).await?;
                checkpoints.push(capture_checkpoint(backend, &state, id).await?);
            }
            _ => {
                state.boundary_revision = apply_operation(backend, &mut state, operation)
                    .await?
                    .value();
                write_state(state_path, &state)?;
            }
        }
    }
    write_state(state_path, &state)?;
    Ok(Some(json!({
        "operation": "controller_runtime_semantics",
        "checkpoints": checkpoints,
    })))
}

async fn apply_operation(
    backend: &EtcdCoordination,
    state: &mut StateFile,
    operation: &Operation,
) -> Result<clustodian::coordination::etcd::Revision, Box<dyn std::error::Error>> {
    match operation {
        Operation::PutResource { resource } => {
            let name = resource
                .get("name")
                .and_then(Value::as_str)
                .ok_or("resource.name must be a string")?;
            Ok(backend
                .put_metadata(
                    &format!("controller/resources/{name}"),
                    &serde_json::to_string(resource)?,
                )
                .await?)
        }
        Operation::Connect { instance, session } => {
            let participant = backend
                .register(
                    InstanceId::new(instance.clone())?,
                    RegistrationOptions::new(Duration::from_secs(600))?,
                )
                .await?;
            let session_id = participant.session_id();
            state.sessions.insert(
                session.clone(),
                SessionBinding {
                    instance: instance.clone(),
                    session_id: session_id.wire_value(),
                },
            );
            Ok(participant.registration_revision())
        }
        Operation::Disconnect { instance, session } => {
            let binding = binding(state, session, instance)?;
            Ok(backend
                .revoke_live(
                    &InstanceId::new(instance.clone())?,
                    SessionId::from_wire_value(binding.session_id),
                )
                .await?)
        }
        Operation::ExpireAndReconnect {
            instance,
            from_session,
            to_session,
        } => {
            let binding = binding(state, from_session, instance)?;
            let instance_id = InstanceId::new(instance.clone())?;
            let _ = backend
                .revoke_live(&instance_id, SessionId::from_wire_value(binding.session_id))
                .await?;
            let participant = backend
                .register(
                    instance_id,
                    RegistrationOptions::new(Duration::from_secs(600))?,
                )
                .await?;
            let session_id = participant.session_id();
            state.sessions.insert(
                to_session.clone(),
                SessionBinding {
                    instance: instance.clone(),
                    session_id: session_id.wire_value(),
                },
            );
            Ok(participant.registration_revision())
        }
        Operation::PublishCurrentState {
            instance,
            session,
            resource,
            states,
        } => {
            let binding = binding(state, session, instance)?;
            let typed_states = states
                .iter()
                .map(|(partition, value)| {
                    Ok((
                        PartitionId::new(partition.clone())?,
                        State::new(value.clone())?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, Box<dyn std::error::Error>>>()?;
            Ok(backend
                .publish_current_state_for_session(
                    &InstanceId::new(instance.clone())?,
                    SessionId::from_wire_value(binding.session_id),
                    ResourceId::new(resource.clone())?,
                    typed_states,
                )
                .await?)
        }
        Operation::SetTransitionThrottle {
            scope,
            rebalance_type,
            max_in_flight,
        } => {
            let value = json!([{
                "scope": scope,
                "rebalance_type": rebalance_type,
                "max_in_flight": max_in_flight,
            }]);
            Ok(backend
                .put_metadata(THROTTLES_KEY, &serde_json::to_string(&value)?)
                .await?)
        }
        Operation::ClearTransitionThrottles => {
            Ok(backend.put_metadata(THROTTLES_KEY, "[]").await?)
        }
        Operation::Checkpoint { .. } => Err("checkpoint is handled by the run loop".into()),
    }
}

fn binding<'a>(
    state: &'a StateFile,
    logical_session: &str,
    instance: &str,
) -> Result<&'a SessionBinding, Box<dyn std::error::Error>> {
    let binding = state
        .sessions
        .get(logical_session)
        .ok_or_else(|| format!("unknown logical session {logical_session}"))?;
    if binding.instance != instance {
        return Err(format!("session {logical_session} belongs to {}", binding.instance).into());
    }
    Ok(binding)
}

async fn wait_for_processed(
    backend: &EtcdCoordination,
    boundary: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(entry) = backend
            .get_metadata("controller/output/processed-revision")
            .await?
        {
            if entry
                .value()
                .ok_or("processed revision has no value")?
                .parse::<i64>()?
                >= boundary
            {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("controller did not process revision {boundary}").into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn capture_checkpoint(
    backend: &EtcdCoordination,
    state: &StateFile,
    id: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let snapshot = backend.controller_snapshot().await?;
    let mut live_instances = Map::new();
    let mut active_current_state = Map::new();
    for (instance, live) in snapshot.participants().live_instances() {
        let label = logical_session(state, live.session_id())?;
        live_instances.insert(instance.to_string(), Value::String(label.clone()));
        let resources = snapshot
            .participants()
            .active_current_state()
            .get(instance)
            .map(|active| {
                active
                    .resources()
                    .iter()
                    .map(|(resource, current)| {
                        let partitions = current
                            .entries()
                            .iter()
                            .flat_map(|(partition, replicas)| {
                                replicas.iter().filter_map(|(replica, value)| {
                                    (replica == instance)
                                        .then_some((partition.to_string(), value.to_string()))
                                })
                            })
                            .collect::<BTreeMap<_, _>>();
                        (
                            resource.to_string(),
                            Value::Object(
                                partitions
                                    .into_iter()
                                    .map(|(partition, value)| (partition, Value::String(value)))
                                    .collect(),
                            ),
                        )
                    })
                    .collect::<Map<_, _>>()
            })
            .unwrap_or_default();
        active_current_state.insert(
            instance.to_string(),
            json!({"session": label, "resources": resources}),
        );
    }

    let external_view: Value = backend
        .get_metadata("controller/output/external-view")
        .await?
        .and_then(|entry| entry.value().map(str::to_owned))
        .map(|value| serde_json::from_str(&value))
        .transpose()?
        .unwrap_or_else(|| json!({}));
    let stored_pending: Vec<PublishedTransition> = backend
        .get_metadata("controller/output/pending-transitions")
        .await?
        .and_then(|entry| entry.value().map(str::to_owned))
        .map(|value| serde_json::from_str(&value))
        .transpose()?
        .unwrap_or_default();
    let pending = stored_pending
        .into_iter()
        .map(|message| {
            let label = logical_session_by_value(state, message.target_session)
                .ok_or_else(|| format!("unknown target session {}", message.target_session))?;
            Ok(json!({
                "resource": message.resource,
                "partition": message.partition,
                "instance": message.instance,
                "target_session": label,
                "from": message.from,
                "to": message.to,
                "message_type": message.message_type,
            }))
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

    Ok(json!({
        "id": id,
        "live_instances": live_instances,
        "active_current_state": active_current_state,
        "external_view": external_view,
        "pending_transitions": pending,
    }))
}

fn logical_session(
    state: &StateFile,
    session: SessionId,
) -> Result<String, Box<dyn std::error::Error>> {
    logical_session_by_value(state, session.wire_value())
        .ok_or_else(|| format!("unknown session {}", session.wire_value()).into())
}

fn logical_session_by_value(state: &StateFile, value: u64) -> Option<String> {
    state
        .sessions
        .iter()
        .find_map(|(label, binding)| (binding.session_id == value).then(|| label.clone()))
}

fn write_state(path: &Path, state: &StateFile) -> Result<(), Box<dyn std::error::Error>> {
    fs::write(path, serde_json::to_vec_pretty(state)?)?;
    Ok(())
}

fn read_state(path: &Path) -> Result<StateFile, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
