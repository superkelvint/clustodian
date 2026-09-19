use clustodian::coordination::etcd::EtcdCoordination;
use clustodian::model::{leader_standby, InstanceId};
use clustodian::observe::ClusterObserver;
use clustodian::participant::{
    ParticipantRuntime, TransitionExecution, TransitionHandler, TransitionHandlerError,
};
use clustodian::runtime::{ControllerRuntime, ControllerRuntimeConfig};
use clustodian_chaos::{
    check_derived_state, check_safety, DerivedCheck, InvariantClass, InvariantViolation,
};
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    spawn_failpoint_watcher();
    match env::var("CLUSTODIAN_CHAOS_NODE_MODE")?.as_str() {
        "controller" => run_controller().await,
        "participant" => run_participant().await,
        "observer" => run_observer().await,
        mode => Err(format!("unsupported chaos node mode: {mode}").into()),
    }
}

fn spawn_failpoint_watcher() {
    let Some(path) = env::var_os("CLUSTODIAN_CHAOS_FAILPOINTS_FILE") else {
        return;
    };
    let path = PathBuf::from(path);
    if let Ok(value) = std::fs::read_to_string(&path) {
        configure_failpoints(&value);
    }
    tokio::spawn(async move {
        let mut previous: Option<String> = None;
        loop {
            if let Ok(value) = tokio::fs::read_to_string(&path).await {
                if previous.as_deref() != Some(value.as_str()) {
                    if let Some(previous) = previous.as_deref() {
                        for entry in previous.split(';').filter(|entry| !entry.is_empty()) {
                            if let Some((name, _)) = entry.split_once('=') {
                                let _ = fail::cfg(name, "off");
                            }
                        }
                    }
                    configure_failpoints(&value);
                    previous = Some(value);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

fn configure_failpoints(specification: &str) {
    for entry in specification.split(';').filter(|entry| !entry.is_empty()) {
        let Some((name, actions)) = entry.split_once('=') else {
            continue;
        };
        let _ = fail::cfg(name, actions);
    }
}

async fn run_controller() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = required("CLUSTODIAN_CHAOS_CLUSTER")?;
    let controller_id = required("CLUSTODIAN_CHAOS_CONTROLLER_ID")?;
    let lease_ttl_ms = env::var("CLUSTODIAN_CHAOS_CONTROLLER_LEASE_TTL_MS")
        .unwrap_or_else(|_| String::from("1500"))
        .parse()?;
    let backend = connect_backend(&cluster, required("CLUSTODIAN_CHAOS_PREFIX")?).await?;
    ControllerRuntime::new(
        backend,
        ControllerRuntimeConfig {
            cluster,
            controller_id,
            lease_ttl_ms,
        },
    )
    .await?
    .run()
    .await
    .map_err(Into::into)
}

async fn run_participant() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = required("CLUSTODIAN_CHAOS_CLUSTER")?;
    let instance_id = required("CLUSTODIAN_CHAOS_INSTANCE_ID")?;
    let _zone = required("CLUSTODIAN_CHAOS_ZONE")?;
    let callback_file = PathBuf::from(required("CLUSTODIAN_CHAOS_CALLBACK_FILE")?);
    let hit_file = env::var_os("CLUSTODIAN_CHAOS_HITS_FILE").map(PathBuf::from);
    let backend = connect_backend(&cluster, required("CLUSTODIAN_CHAOS_PREFIX")?).await?;
    let instance = InstanceId::new(&instance_id)?;
    let shutdown_backend = backend.clone();
    let shutdown_instance = instance.clone();
    let lease_ttl_ms = env::var("CLUSTODIAN_CHAOS_PARTICIPANT_LEASE_TTL_MS")
        .unwrap_or_else(|_| String::from("2000"))
        .parse()?;
    let runtime = ParticipantRuntime::new(
        backend,
        instance,
        leader_standby(),
        FileCallback {
            callback_file,
            instance_id,
            hit_file,
            repeated_error_state: Mutex::new((0, 0)),
        },
    )
    .with_lease_ttl(Duration::from_millis(lease_ttl_ms))?;
    let mut task = tokio::spawn(runtime.run(|| Ok(())));
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = &mut task => {
            let result = result?;
            result.map_err(Into::into)
        }
        _ = terminate.recv() => {
            task.abort();
            let _ = task.await;
            if let Some(session) = shutdown_backend.live_session(&shutdown_instance).await? {
                shutdown_backend
                    .revoke_live(&shutdown_instance, session)
                    .await?;
            }
            Ok(())
        }
    }
}

async fn run_observer() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = required("CLUSTODIAN_CHAOS_CLUSTER")?;
    let output = PathBuf::from(required("CLUSTODIAN_CHAOS_OBSERVER_EVENTS")?);
    let backend = connect_backend(&cluster, required("CLUSTODIAN_CHAOS_PREFIX")?).await?;
    let observer = ClusterObserver::new(backend);
    let (snapshot, mut watch) = observer.snapshot_and_watch_namespace().await?;
    append_observer_snapshot(&output, &snapshot)?;
    let mut last_revision = snapshot.observer_revision.value();
    loop {
        match watch.next().await {
            Ok(event) => {
                let revision = event.revision().value();
                if revision <= last_revision {
                    continue;
                }
                let snapshot = observer.snapshot_at_revision(event.revision()).await?;
                append_observer_snapshot(&output, &snapshot)?;
                last_revision = revision;
            }
            Err(clustodian::coordination::etcd::WatchError::Compacted { .. }) => {
                let recovery = watch.recover_compaction().await?;
                if matches!(
                    recovery,
                    clustodian::coordination::etcd::WatchRecovery::Namespace(_)
                ) {
                    let snapshot = observer.snapshot().await?;
                    last_revision = snapshot.observer_revision.value();
                    append_observer_snapshot(&output, &snapshot)?;
                }
            }
            Err(error) => {
                let event = serde_json::json!({
                "kind": "observer_blindness",
                "error": error.to_string(),
                });
                append_json(&output, &event)?;
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = watch.resume().await;
            }
        }
    }
}

fn append_observer_snapshot(
    output: &PathBuf,
    snapshot: &clustodian::observe::ClusterSnapshot,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut violations = check_safety(snapshot);
    if let DerivedCheck::Invalid(detail) = check_derived_state(snapshot) {
        violations.push(InvariantViolation {
            class: InvariantClass::DerivedState,
            name: String::from("external_view_matches_current_state"),
            detail,
            observer_revision: snapshot.observer_revision.value(),
        });
    }
    append_json(
        output,
        &serde_json::json!({
            "kind": "observation",
            "observer_revision": snapshot.observer_revision.value(),
            "processed_revision": snapshot.processed_revision.map(|revision| revision.value()),
            "violations": violations,
            "snapshot": snapshot,
        }),
    )?;
    Ok(())
}

struct FileCallback {
    callback_file: PathBuf,
    instance_id: String,
    hit_file: Option<PathBuf>,
    repeated_error_state: Mutex<(u64, u32)>,
}

impl FileCallback {
    fn behavior(&self) -> Result<CallbackSpec, std::io::Error> {
        if !self.callback_file.exists() {
            return Ok(CallbackSpec::SucceedImmediately);
        }
        let bytes = std::fs::read(&self.callback_file)?;
        serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }
}

#[derive(serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CallbackSpec {
    SucceedImmediately,
    SucceedSlowly {
        milliseconds: u64,
    },
    Block {
        token: String,
    },
    Error,
    RepeatedError {
        attempts: u32,
        #[serde(default)]
        generation: u64,
    },
    Panic,
}

impl TransitionHandler for FileCallback {
    fn handle(&self, _execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        record_callback_hit(&self.hit_file, &self.instance_id)
            .map_err(|error| TransitionHandlerError::new(error.to_string()))?;
        match self
            .behavior()
            .map_err(|error| TransitionHandlerError::new(error.to_string()))?
        {
            CallbackSpec::SucceedImmediately => Ok(()),
            CallbackSpec::SucceedSlowly { milliseconds } => {
                std::thread::sleep(Duration::from_millis(milliseconds));
                Ok(())
            }
            CallbackSpec::Block { token } => {
                let release = self
                    .callback_file
                    .with_file_name(format!("release-{token}"));
                while !release.exists() {
                    std::thread::sleep(Duration::from_millis(20));
                }
                std::fs::remove_file(release)
                    .map_err(|error| TransitionHandlerError::new(error.to_string()))?;
                Ok(())
            }
            CallbackSpec::Error => Err(TransitionHandlerError::new("injected application error")),
            CallbackSpec::RepeatedError {
                attempts,
                generation,
            } => {
                let mut state = self
                    .repeated_error_state
                    .lock()
                    .map_err(|_| TransitionHandlerError::new("callback state poisoned"))?;
                if state.0 != generation {
                    *state = (generation, 0);
                }
                if state.1 < attempts {
                    state.1 += 1;
                    Err(TransitionHandlerError::new(
                        "injected repeated application error",
                    ))
                } else {
                    Ok(())
                }
            }
            CallbackSpec::Panic => panic!("injected application callback panic"),
        }
    }
}

fn record_callback_hit(path: &Option<PathBuf>, instance_id: &str) -> Result<(), std::io::Error> {
    let Some(path) = path else {
        return Ok(());
    };
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "callback/{instance_id}")
}

async fn connect_backend(
    cluster: &str,
    prefix: String,
) -> Result<EtcdCoordination, Box<dyn std::error::Error>> {
    let endpoints = required("CLUSTODIAN_CHAOS_ETCD_ENDPOINTS")?
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    Ok(EtcdCoordination::connect_endpoints(endpoints, prefix, cluster.to_owned()).await?)
}

fn required(name: &str) -> Result<String, std::io::Error> {
    env::var(name).map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, name))
}

fn append_json(path: &PathBuf, value: &serde_json::Value) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{value}")
}
