use clustodian::coordination::etcd::{
    EtcdCoordination, EtcdCoordinationConfig, PENDING_TRANSITIONS_KEY,
};
use clustodian::model::InstanceId;
use clustodian::participant::{
    processed_revision_key, ParticipantRuntime, TransitionExecution, TransitionHandler,
    TransitionHandlerError,
};
use clustodian::transition::TransitionMessage;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

const WAIT_STEP: Duration = Duration::from_millis(25);
const WAIT_ATTEMPTS: usize = 4_800;

#[derive(Debug, Deserialize)]
pub(crate) struct Scenario {
    pub participant: ParticipantSpec,
    #[serde(default)]
    pub handler_behaviors: BTreeMap<String, HandlerBehavior>,
    pub steps: Vec<Step>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ParticipantSpec {
    pub instance: String,
    pub initial_session: String,
    pub state_model: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HandlerBehavior {
    pub kind: String,
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op")]
pub(crate) enum Step {
    #[serde(rename = "send_transition")]
    SendTransition {
        message_id: String,
        resource: String,
        partition: String,
        target_session: String,
        from: String,
        to: String,
    },
    #[serde(rename = "checkpoint")]
    Checkpoint { id: String },
    #[serde(rename = "wait_handler_entered")]
    WaitHandlerEntered { message_id: String, token: String },
    #[serde(rename = "release_handler")]
    ReleaseHandler { message_id: String, token: String },
    #[serde(rename = "expire_and_reconnect")]
    ExpireAndReconnect {
        from_session: String,
        to_session: String,
    },
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct DriverState {
    sessions: BTreeMap<String, u64>,
}

pub(crate) fn read_scenario(path: &Path) -> Result<Scenario, Box<dyn std::error::Error>> {
    let scenario = serde_json::from_str(&fs::read_to_string(path)?)?;
    Ok(scenario)
}

pub(crate) async fn run_runtime(scenario_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let scenario = read_scenario(scenario_path)?;
    let endpoint = env::var("CLUSTODIAN_M11_ETCD_ENDPOINT")?;
    let prefix = env::var("CLUSTODIAN_M11_ETCD_PREFIX")?;
    let ready = PathBuf::from(env::var("CLUSTODIAN_M11_READY_FILE")?);
    let backend = EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint,
        prefix,
        cluster: String::from("m11"),
    })
    .await?;
    let instance = InstanceId::try_from(scenario.participant.instance.as_str())?;
    if scenario.participant.state_model != "LeaderStandby" {
        return Err("M11 supports only LeaderStandby".into());
    }
    let handler = RecordingHandler::new(scenario.handler_behaviors);
    let runtime = ParticipantRuntime::new(
        backend,
        instance,
        clustodian::model::leader_standby(),
        handler,
    );
    runtime
        .run(|| {
            fs::write(&ready, b"ready\n")?;
            Ok(())
        })
        .await?;
    Ok(())
}

pub(crate) async fn prepare(
    scenario_path: &Path,
    state_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let scenario = read_scenario(scenario_path)?;
    if scenario.participant.state_model != "LeaderStandby" {
        return Err("M11 supports only LeaderStandby".into());
    }
    let state = DriverState {
        sessions: BTreeMap::new(),
    };
    fs::write(state_path, serde_json::to_vec_pretty(&state)?)?;
    Ok(())
}

pub(crate) async fn run_driver(
    scenario_path: &Path,
    state_path: &Path,
) -> Result<Value, Box<dyn std::error::Error>> {
    let scenario = read_scenario(scenario_path)?;
    let endpoint = env::var("CLUSTODIAN_M11_ETCD_ENDPOINT")?;
    let prefix = env::var("CLUSTODIAN_M11_ETCD_PREFIX")?;
    let backend = EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint,
        prefix,
        cluster: String::from("m11"),
    })
    .await?;
    let instance = InstanceId::try_from(scenario.participant.instance.as_str())?;
    let mut state: DriverState = serde_json::from_slice(&fs::read(state_path)?)?;
    let initial = wait_for_live(&backend, &instance, None).await?;
    state.sessions.insert(
        scenario.participant.initial_session.clone(),
        initial.wire_value(),
    );
    let mut boundary = None;
    let mut checkpoints = Vec::new();
    for step in &scenario.steps {
        match step {
            Step::SendTransition {
                message_id,
                resource,
                partition,
                target_session,
                from,
                to,
            } => {
                let target = state.sessions.get(target_session).copied().unwrap_or(0);
                let message = TransitionMessage {
                    message_id: message_id.clone(),
                    resource: resource.clone(),
                    partition: partition.clone(),
                    instance: scenario.participant.instance.clone(),
                    target_session: target,
                    from: from.clone(),
                    to: to.clone(),
                    message_type: String::from("STATE_TRANSITION"),
                };
                boundary = Some(backend.inject_pending_transition(&message).await?.value());
            }
            Step::WaitHandlerEntered { message_id, token } => {
                wait_for_control("entered", message_id, token).await?;
            }
            Step::ReleaseHandler { message_id, token } => {
                let path = control_path("release", message_id, token)?;
                fs::write(path, b"release\n")?;
            }
            Step::ExpireAndReconnect {
                from_session,
                to_session,
            } => {
                let old = *state
                    .sessions
                    .get(from_session)
                    .ok_or("unknown source session label")?;
                backend
                    .revoke_live(
                        &instance,
                        clustodian::model::SessionId::from_wire_value(old),
                    )
                    .await?;
                wait_for_live(&backend, &instance, None).await?;
                let replacement = wait_for_live(&backend, &instance, Some(old)).await?;
                state
                    .sessions
                    .insert(to_session.clone(), replacement.wire_value());
            }
            Step::Checkpoint { id } => {
                if let Some(revision) = boundary {
                    wait_for_progress(&backend, &instance, revision).await?;
                }
                checkpoints.push(checkpoint(&backend, &instance, &state, id).await?);
            }
        }
    }
    fs::write(state_path, serde_json::to_vec_pretty(&state)?)?;
    Ok(json!({"operation":"participant_runtime_semantics", "checkpoints":checkpoints}))
}

struct RecordingHandler {
    behaviors: BTreeMap<String, HandlerBehavior>,
    event_file: PathBuf,
    control_dir: PathBuf,
    file_lock: Arc<std::sync::Mutex<()>>,
}

impl RecordingHandler {
    fn new(behaviors: BTreeMap<String, HandlerBehavior>) -> Self {
        Self {
            behaviors,
            event_file: PathBuf::from(env::var("CLUSTODIAN_M11_EVENT_FILE").expect("event file")),
            control_dir: PathBuf::from(
                env::var("CLUSTODIAN_M11_CONTROL_DIR").expect("control dir"),
            ),
            file_lock: Arc::new(std::sync::Mutex::new(())),
        }
    }

    fn record(&self, execution: &TransitionExecution, outcome: &str) {
        let _guard = self.file_lock.lock().expect("event file lock");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.event_file)
            .expect("open event file");
        let event = json!({
            "message_id": execution.transition_id(),
            "resource": execution.resource().as_str(),
            "partition": execution.partition().as_str(),
            "from": execution.source_state().as_str(),
            "to": execution.target_state().as_str(),
            "outcome": outcome,
        });
        writeln!(file, "{event}").expect("write handler event");
    }
}

impl TransitionHandler for RecordingHandler {
    fn handle(&self, execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        let behavior = self.behaviors.get(execution.transition_id());
        let kind = behavior.map_or("success", |value| value.kind.as_str());
        if kind == "block_then_success" {
            let token = behavior
                .and_then(|value| value.token.as_deref())
                .ok_or_else(|| TransitionHandlerError::new("blocking handler has no token"))?;
            fs::write(
                control_path_in(
                    &self.control_dir,
                    "entered",
                    execution.transition_id(),
                    token,
                ),
                b"entered\n",
            )
            .map_err(|error| TransitionHandlerError::new(error.to_string()))?;
            let release = control_path_in(
                &self.control_dir,
                "release",
                execution.transition_id(),
                token,
            );
            for _ in 0..WAIT_ATTEMPTS {
                if release.exists() {
                    self.record(execution, "success");
                    return Ok(());
                }
                std::thread::sleep(WAIT_STEP);
            }
            return Err(TransitionHandlerError::new(
                "timed out waiting for handler release",
            ));
        }
        if kind == "error" {
            self.record(execution, "error");
            return Err(TransitionHandlerError::new("application transition failed"));
        }
        self.record(execution, "success");
        Ok(())
    }
}

async fn checkpoint(
    backend: &EtcdCoordination,
    instance: &InstanceId,
    state: &DriverState,
    id: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let snapshot = backend.participant_snapshot().await?;
    let labels = state
        .sessions
        .iter()
        .map(|(label, session)| (*session, label.as_str()))
        .collect::<BTreeMap<_, _>>();
    let live_instances = snapshot
        .live_instances()
        .iter()
        .map(|(name, live)| {
            (
                name.to_string(),
                labels
                    .get(&live.session_id().wire_value())
                    .copied()
                    .unwrap_or("unknown")
                    .to_owned(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut active_current_state = BTreeMap::new();
    if let Some(active) = snapshot.active_current_state().get(instance) {
        let resources = active
            .resources()
            .iter()
            .map(|(resource, current)| {
                let partitions = current
                    .entries()
                    .iter()
                    .filter_map(|(partition, replicas)| {
                        replicas
                            .get(instance)
                            .map(|value| (partition.to_string(), value.to_string()))
                    })
                    .collect::<BTreeMap<_, _>>();
                (resource.to_string(), partitions)
            })
            .filter(|(_, partitions)| !partitions.is_empty())
            .collect::<BTreeMap<_, _>>();
        active_current_state.insert(
            instance.to_string(),
            json!({
                "session": labels.get(&active.session_id().wire_value()).copied().unwrap_or("unknown"),
                "resources": resources,
            }),
        );
    }
    let pending = backend
        .get_metadata(PENDING_TRANSITIONS_KEY)
        .await?
        .map(|entry| serde_json::from_str::<Vec<TransitionMessage>>(entry.value().unwrap_or("[]")))
        .transpose()?
        .unwrap_or_default()
        .into_iter()
        .map(|message| {
            json!({
                "message_id": message.message_id,
                "resource": message.resource,
                "partition": message.partition,
                "target_session": labels.get(&message.target_session).copied().unwrap_or("unknown"),
                "from": message.from,
                "to": message.to,
                "message_type": message.message_type,
            })
        })
        .collect::<Vec<_>>();
    let events = read_events()?;
    Ok(json!({
        "id": id,
        "live_instances": live_instances,
        "active_current_state": active_current_state,
        "pending_messages": pending,
        "handler_events": events,
    }))
}

fn read_events() -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let path = PathBuf::from(env::var("CLUSTODIAN_M11_EVENT_FILE")?);
    if !path.exists() {
        return Ok(Vec::new());
    }
    fs::read_to_string(path)
        .map(|text| {
            text.lines()
                .filter(|line| !line.is_empty())
                .map(serde_json::from_str)
                .collect::<Result<Vec<Value>, _>>()
        })
        .map_err(Into::into)
        .and_then(|result| result.map_err(Into::into))
}

async fn wait_for_live(
    backend: &EtcdCoordination,
    instance: &InstanceId,
    not_session: Option<u64>,
) -> Result<clustodian::model::SessionId, Box<dyn std::error::Error>> {
    for _ in 0..WAIT_ATTEMPTS {
        if let Some(session) = backend.live_session(instance).await? {
            if not_session != Some(session.wire_value()) {
                return Ok(session);
            }
        }
        tokio::time::sleep(WAIT_STEP).await;
    }
    Err("timed out waiting for participant session".into())
}

async fn wait_for_progress(
    backend: &EtcdCoordination,
    instance: &InstanceId,
    revision: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = processed_revision_key(instance);
    for _ in 0..WAIT_ATTEMPTS {
        if let Some(entry) = backend.get_metadata(&key).await? {
            if entry.value().unwrap_or("0").parse::<i64>()? >= revision {
                return Ok(());
            }
        }
        tokio::time::sleep(WAIT_STEP).await;
    }
    Err(format!("timed out waiting for participant progress {revision}").into())
}

async fn wait_for_control(
    kind: &str,
    message_id: &str,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = control_path(kind, message_id, token)?;
    for _ in 0..WAIT_ATTEMPTS {
        if path.exists() {
            return Ok(());
        }
        tokio::time::sleep(WAIT_STEP).await;
    }
    Err(format!("timed out waiting for {kind} control").into())
}

fn control_path(
    kind: &str,
    message_id: &str,
    token: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(control_path_in(
        &PathBuf::from(env::var("CLUSTODIAN_M11_CONTROL_DIR")?),
        kind,
        message_id,
        token,
    ))
}

fn control_path_in(dir: &Path, kind: &str, message_id: &str, token: &str) -> PathBuf {
    dir.join(format!("{kind}-{message_id}-{token}"))
}
