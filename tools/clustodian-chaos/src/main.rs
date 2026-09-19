use clustodian::admin::{ClusterAdmin, InstanceSpec, PlacementSpec, ResourceSpec, ThrottleSpec};
use clustodian::coordination::etcd::{EtcdCoordination, Revision};
use clustodian::observe::ClusterObserver;
use clustodian_chaos::minimizer::{
    reduce_cluster_and_resources, reduce_fault, remove_action_range, remove_fault_range,
};
use clustodian_chaos::{
    check_convergence, check_safety, monotonic_millis, read_json, write_json, Action,
    CallbackBehavior, ChaosError, ClusterConfig, FailureArtifact, Fault, FaultActivation,
    ParticipantConfig, PlacementConfig, ProcessSupervisor, ReplayStats, ResourceConfig,
    RuntimeMetricSample, SeededGenerator, Trace,
};
use std::env;
use std::path::{Path, PathBuf};

#[tokio::main]
async fn main() -> Result<(), ChaosError> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("run") => run(args.collect()).await,
        Some("replay") => replay(args.collect()).await,
        Some("minimize") => minimize(args.collect()).await,
        Some("crash-matrix") => crash_matrix(args.collect()).await,
        Some("quorum-loss") => quorum_loss(args.collect()).await,
        Some("--help") | Some("-h") | None => {
            print_help();
            Ok(())
        }
        Some(command) => Err(ChaosError::InvalidArguments(format!(
            "unknown command: {command}"
        ))),
    }
}

async fn run(arguments: Vec<String>) -> Result<(), ChaosError> {
    let seed = required_u64(&arguments, "--seed")?;
    let steps = optional_u64(&arguments, "--steps")?.unwrap_or(100) as usize;
    let profile = optional_string(&arguments, "--profile")?.unwrap_or_else(|| String::from("pr"));
    let output = optional_string(&arguments, "--trace-out")?
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("target/clustodian-chaos/trace-{seed}.json")));
    let trace = SeededGenerator::new(seed).generate(&profile, steps);
    if let Some(parent) = output.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(ChaosError::Io)?;
    }
    write_json(&output, &trace).await?;
    if env::var_os("CLUSTODIAN_CHAOS_ETCD_ENDPOINTS").is_some() {
        if let Err(error) = execute_trace(&trace, None, None).await {
            capture_failure_evidence(
                env::var("CLUSTODIAN_CHAOS_WORK_DIR")
                    .unwrap_or_else(|_| String::from("target/clustodian-chaos")),
            );
            let replay = replay_trace(&trace, invariant_of(&error), None).await;
            let artifact = failure_artifact(&trace, &error, replay);
            let failure_path = output.with_file_name(format!("failure-{seed}.json"));
            write_json(&failure_path, &artifact).await?;
            return Err(error);
        }
    }
    println!(
        "generated seed={} profile={} steps={} trace={}",
        seed,
        profile,
        trace.actions.len(),
        output.display()
    );
    Ok(())
}

async fn execute_trace(
    trace: &Trace,
    prefix_override: Option<&str>,
    work_override: Option<&Path>,
) -> Result<(), ChaosError> {
    let endpoints = required_env("CLUSTODIAN_CHAOS_ETCD_ENDPOINTS")?;
    let controller_endpoints =
        env::var("CLUSTODIAN_CHAOS_CONTROLLER_ENDPOINTS").unwrap_or_else(|_| endpoints.clone());
    let participant_endpoints =
        env::var("CLUSTODIAN_CHAOS_PARTICIPANT_ENDPOINTS").unwrap_or_else(|_| endpoints.clone());
    let observer_endpoints = required_env("CLUSTODIAN_CHAOS_OBSERVER_ENDPOINTS")?;
    let use_process_specific_endpoints = toxiproxy_enabled();
    let configured_prefix = required_env("CLUSTODIAN_CHAOS_PREFIX")?;
    let prefix = prefix_override
        .map(str::to_owned)
        .unwrap_or(configured_prefix);
    let cluster = env::var("CLUSTODIAN_CHAOS_CLUSTER").unwrap_or_else(|_| String::from("chaos"));
    let work_directory = work_override
        .map(Path::to_path_buf)
        .or_else(|| {
            env::var("CLUSTODIAN_CHAOS_WORK_DIR")
                .ok()
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| PathBuf::from(format!("target/clustodian-chaos/run-{}", trace.seed)));
    let node_binary = env::var("CLUSTODIAN_CHAOS_NODE_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/debug/clustodian-chaos-node"));
    let backend = connect(&endpoints, &prefix, &cluster).await?;
    let admin = ClusterAdmin::new(backend.clone());
    // Keep invariant reads on a dedicated etcd client and endpoint path. The
    // admin client is part of the workload driver and must not become the
    // observer's coordination session.
    let observer_backend = connect(&observer_endpoints, &prefix, &cluster).await?;
    let observer = ClusterObserver::new(observer_backend);
    admin
        .ensure_cluster(&cluster)
        .await
        .map_err(|error| ChaosError::InvalidArguments(format!("ensure cluster: {error}")))?;
    for participant in &trace.initial_config.participants {
        put_instance(&admin, participant).await.map_err(|error| {
            ChaosError::InvalidArguments(format!("put instance {}: {error}", participant.id))
        })?;
    }
    // Resources are applied by their ordered CreateResource actions. In
    // particular, CRUSH placement needs participant leases established by the
    // startup actions before its first reconciliation.
    let mut supervisor = ProcessSupervisor::new(
        node_binary,
        &work_directory,
        cluster.clone(),
        prefix,
        controller_endpoints,
        participant_endpoints,
        observer_endpoints,
        use_process_specific_endpoints,
    )?;
    supervisor.start_observer()?;
    let mut budget = default_budget(&trace.profile);
    let mut fault_index = 0;
    let mut current_config = trace.initial_config.clone();
    let mut active_faults = Vec::new();
    let mut previous_observer_revision = None;
    for (action_index, action) in trace.actions.iter().enumerate() {
        while trace
            .faults
            .get(fault_index)
            .is_some_and(|fault| fault.action_index == action_index)
        {
            let scheduled = &trace.faults[fault_index];
            if !matches!(
                budget.admit(&scheduled.fault),
                clustodian_chaos::FaultAdmission::Admitted
            ) {
                fault_index += 1;
                continue;
            }
            let scope = activate_fault(&mut supervisor, &observer, scheduled.fault.clone()).await?;
            if let Some(scope) = scope {
                active_faults.push(scope);
            }
            fault_index += 1;
        }
        if !admit_action(&mut budget, action) {
            while let Some(scope) = active_faults.pop() {
                deactivate_fault(&mut supervisor, &scope).await?;
                assert_fault_hit(&supervisor, &scope)?;
            }
            continue;
        }
        let action_started = std::time::Instant::now();
        let action_result = apply_action(
            &mut supervisor,
            &admin,
            &mut current_config,
            action,
            &observer,
        )
        .await;
        let mut cleanup_error = None;
        while let Some(scope) = active_faults.pop() {
            if let Err(error) = deactivate_fault(&mut supervisor, &scope).await {
                if cleanup_error.is_none() {
                    cleanup_error = Some(error);
                }
            }
            if cleanup_error.is_none() {
                if let Err(error) = assert_fault_hit(&supervisor, &scope) {
                    cleanup_error = Some(error);
                }
            }
        }
        action_result?;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        if let Some(violation) = supervisor.observer_violations().into_iter().next() {
            return Err(ChaosError::Invariant(violation));
        }
        let snapshot = observer.snapshot().await.map_err(|error| {
            ChaosError::InvalidArguments(format!("observer lost connectivity: {error}"))
        })?;
        supervisor.record_observation(&snapshot)?;
        let observer_revision = snapshot.observer_revision.value();
        let semantic_revision_delta = previous_observer_revision
            .map_or(0, |previous| observer_revision.saturating_sub(previous));
        previous_observer_revision = Some(observer_revision);
        let external_view_bytes = serde_json::to_vec(&snapshot.external_view)
            .map_err(ChaosError::Serialization)?
            .len();
        let process_metrics = supervisor.process_metrics();
        supervisor.record_metrics(&RuntimeMetricSample {
            action_index,
            action: format!("{action:?}"),
            action_latency_millis: action_started.elapsed().as_millis(),
            observer_revision,
            semantic_revision_delta,
            pending_transition_count: snapshot.pending_transitions.len(),
            external_view_bytes,
            process_count: process_metrics.process_count,
            task_count: process_metrics.task_count,
            rss_bytes: process_metrics.rss_bytes,
            open_fd_count: process_metrics.open_fd_count,
            watch_event_count: supervisor.watch_event_count(),
        })?;
        if let Some(violation) = check_safety(&snapshot).into_iter().next() {
            return Err(ChaosError::Invariant(violation));
        }
        if matches!(action, Action::WaitForConvergence { .. }) {
            if let Some(violation) = check_convergence(&snapshot, &current_config)
                .into_iter()
                .next()
            {
                return Err(ChaosError::Invariant(violation));
            }
        }
    }
    Ok(())
}

fn admit_action(budget: &mut clustodian_chaos::FaultBudget, action: &Action) -> bool {
    match action {
        Action::GracefulStopController { .. } | Action::CrashController { .. } => matches!(
            budget.admit_controller_failure(),
            clustodian_chaos::FaultAdmission::Admitted
        ),
        Action::GracefulStopParticipant { .. } | Action::CrashParticipant { .. } => matches!(
            budget.admit_participant_failure(),
            clustodian_chaos::FaultAdmission::Admitted
        ),
        Action::StartController { .. }
        | Action::RestartController { .. }
        | Action::StartParticipant { .. }
        | Action::RestartParticipant { .. }
        | Action::AddParticipant { .. }
        | Action::RemoveParticipant { .. }
        | Action::CreateResource { .. }
        | Action::ModifyPreferenceList { .. }
        | Action::ChangeThrottle { .. }
        | Action::Wait { .. }
        | Action::RequestQuiescence
        | Action::WaitForConvergence { .. }
        | Action::AssertIdle { .. } => true,
    }
}

async fn apply_action(
    supervisor: &mut ProcessSupervisor,
    admin: &ClusterAdmin,
    config: &mut ClusterConfig,
    action: &Action,
    observer: &ClusterObserver,
) -> Result<(), ChaosError> {
    match action {
        Action::StartController { id } | Action::RestartController { id } => {
            if matches!(action, Action::RestartController { .. }) {
                supervisor.stop_controller(id)?;
            }
            supervisor.start_controller(id)?;
        }
        Action::GracefulStopController { id } => {
            supervisor.stop_controller(id)?;
        }
        Action::CrashController { id } => {
            supervisor.crash_controller(id)?;
        }
        Action::StartParticipant { id } | Action::RestartParticipant { id } => {
            if matches!(action, Action::RestartParticipant { .. }) {
                supervisor.stop_participant(id)?;
            }
            let participant = config
                .participants
                .iter()
                .find(|participant| participant.id == *id)
                .ok_or_else(|| ChaosError::InvalidArguments(format!("unknown participant {id}")))?;
            supervisor.start_participant(participant)?;
        }
        Action::GracefulStopParticipant { id } => {
            supervisor.stop_participant(id)?;
        }
        Action::CrashParticipant { id } => {
            supervisor.crash_participant(id)?;
        }
        Action::AddParticipant { participant } => {
            put_instance(admin, participant).await?;
            supervisor.start_participant(participant)?;
            if !config
                .participants
                .iter()
                .any(|candidate| candidate.id == participant.id)
            {
                config.participants.push(participant.clone());
                config
                    .participants
                    .sort_by(|left, right| left.id.cmp(&right.id));
            }
        }
        Action::RemoveParticipant { id } => {
            // Remove the live session before deleting its topology entry. A
            // controller must never observe a live instance without a
            // corresponding configured topology node.
            supervisor.stop_participant(id)?;
            admin
                .remove_instance(id)
                .await
                .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;
            config
                .participants
                .retain(|participant| participant.id != *id);
        }
        Action::CreateResource { resource } => put_resource(admin, resource).await?,
        Action::ModifyPreferenceList {
            resource,
            partition,
            instances,
        } => {
            let mut updated = config
                .resources
                .iter()
                .find(|candidate| candidate.name == *resource)
                .cloned()
                .ok_or_else(|| {
                    ChaosError::InvalidArguments(format!("unknown resource {resource}"))
                })?;
            let PlacementConfig::SemiAuto { preference_lists } = &mut updated.placement else {
                return Err(ChaosError::InvalidArguments(String::from(
                    "preference lists require SEMI_AUTO placement",
                )));
            };
            preference_lists.insert(partition.clone(), instances.clone());
            put_resource(admin, &updated).await?;
            if let Some(resource_config) = config
                .resources
                .iter_mut()
                .find(|candidate| candidate.name == *resource)
            {
                if let PlacementConfig::SemiAuto { preference_lists } =
                    &mut resource_config.placement
                {
                    preference_lists.insert(partition.clone(), instances.clone());
                }
            }
        }
        Action::ChangeThrottle { throttle } => {
            admin
                .put_throttles(vec![ThrottleSpec {
                    scope: throttle.scope.clone(),
                    rebalance_type: throttle.rebalance_type.clone(),
                    max_in_flight: throttle.max_in_flight,
                }])
                .await
                .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;
        }
        Action::Wait { milliseconds } => {
            tokio::time::sleep(std::time::Duration::from_millis(*milliseconds)).await;
        }
        Action::RequestQuiescence => wait_for_quiescence(supervisor, observer).await?,
        Action::WaitForConvergence {
            deadline_milliseconds,
        } => {
            wait_for_quiescence(supervisor, observer).await?;
            wait_for_convergence(observer, *deadline_milliseconds, config).await?;
        }
        Action::AssertIdle { seconds } => assert_idle(supervisor, observer, *seconds).await?,
    }
    Ok(())
}

enum FaultScope {
    Network {
        script: String,
        proxies: Vec<String>,
        name: String,
        toxic: String,
        value: u64,
        jitter: u64,
        stream: String,
        duration: std::time::Duration,
        started: std::time::Instant,
    },
    Callback {
        participant: String,
        behavior: CallbackBehavior,
        release_delay: std::time::Duration,
        hit_key: String,
        hit_before: u64,
    },
    Clock {
        duration: std::time::Duration,
        started: std::time::Instant,
    },
    Failpoint {
        point: String,
        behavior: clustodian_chaos::FailpointBehavior,
        hit_key: String,
        hit_before: u64,
    },
}

async fn activate_fault(
    supervisor: &mut ProcessSupervisor,
    observer: &ClusterObserver,
    fault: Fault,
) -> Result<Option<FaultScope>, ChaosError> {
    match fault {
        Fault::Network {
            target,
            toxic,
            duration_milliseconds,
        } => {
            let script = env::var("CLUSTODIAN_CHAOS_TOXIPROXY_SCRIPT")
                .unwrap_or_else(|_| String::from("tools/clustodian-chaos/scripts/toxiproxy.py"));
            let proxies = match target {
                clustodian_chaos::NetworkTarget::Controller { id } => {
                    ProcessSupervisor::process_proxy_names(
                        clustodian_chaos::ProcessRole::Controller,
                        &id,
                    )
                }
                clustodian_chaos::NetworkTarget::Participant { id } => {
                    ProcessSupervisor::process_proxy_names(
                        clustodian_chaos::ProcessRole::Participant,
                        &id,
                    )
                }
                clustodian_chaos::NetworkTarget::ControllersAndParticipants => {
                    supervisor.all_process_proxy_names()
                }
            };
            let (toxic_name, toxic_type, value, jitter, stream) = match toxic {
                clustodian_chaos::NetworkToxic::Disconnect
                | clustodian_chaos::NetworkToxic::ConnectionReset => (
                    String::from("chaos-disconnect"),
                    String::from("reset_peer"),
                    1_u64,
                    0_u64,
                    String::from("downstream"),
                ),
                clustodian_chaos::NetworkToxic::DirectionalDisconnect { direction } => (
                    String::from("chaos-directional-disconnect"),
                    String::from("reset_peer"),
                    1,
                    0,
                    match direction {
                        clustodian_chaos::Direction::Upstream => String::from("upstream"),
                        clustodian_chaos::Direction::Downstream => String::from("downstream"),
                    },
                ),
                clustodian_chaos::NetworkToxic::Latency {
                    milliseconds,
                    jitter_milliseconds,
                } => (
                    String::from("chaos-latency"),
                    String::from("latency"),
                    milliseconds,
                    jitter_milliseconds,
                    String::from("downstream"),
                ),
                clustodian_chaos::NetworkToxic::Timeout { milliseconds } => (
                    String::from("chaos-timeout"),
                    String::from("timeout"),
                    milliseconds,
                    0,
                    String::from("downstream"),
                ),
                clustodian_chaos::NetworkToxic::Bandwidth {
                    kilobytes_per_second,
                } => (
                    String::from("chaos-bandwidth"),
                    String::from("bandwidth"),
                    kilobytes_per_second,
                    0,
                    String::from("downstream"),
                ),
                clustodian_chaos::NetworkToxic::SlowClose { milliseconds } => (
                    String::from("chaos-slow-close"),
                    String::from("slow_close"),
                    milliseconds,
                    0,
                    String::from("downstream"),
                ),
            };
            let mut activated: Vec<String> = Vec::new();
            for proxy in &proxies {
                let result = run_toxic(ToxicRequest {
                    script: &script,
                    proxy,
                    name: &toxic_name,
                    toxic: &toxic_type,
                    value,
                    jitter,
                    stream: &stream,
                    add: true,
                });
                if let Err(error) = result {
                    for activated_proxy in activated {
                        let _ = run_toxic(ToxicRequest {
                            script: &script,
                            proxy: &activated_proxy,
                            name: &toxic_name,
                            toxic: &toxic_type,
                            value,
                            jitter,
                            stream: &stream,
                            add: false,
                        });
                        supervisor.mark_toxic(&activated_proxy, &toxic_name, false);
                    }
                    return Err(error);
                }
                supervisor.mark_toxic(proxy, &toxic_name, true);
                activated.push(proxy.clone());
            }
            Ok(Some(FaultScope::Network {
                script,
                proxies,
                name: toxic_name,
                toxic: toxic_type,
                value,
                jitter,
                stream,
                duration: std::time::Duration::from_millis(duration_milliseconds),
                started: std::time::Instant::now(),
            }))
        }
        Fault::Callback {
            participant,
            behavior,
        } => {
            let value = match &behavior {
                CallbackBehavior::SucceedImmediately => {
                    serde_json::json!({"kind":"succeed_immediately"})
                }
                CallbackBehavior::SucceedSlowly { milliseconds } => {
                    serde_json::json!({"kind":"succeed_slowly", "milliseconds":milliseconds})
                }
                CallbackBehavior::Block { token } => {
                    serde_json::json!({"kind":"block", "token":token})
                }
                CallbackBehavior::Error => {
                    serde_json::json!({"kind":"error"})
                }
                CallbackBehavior::RepeatedError { attempts } => serde_json::json!({
                    "kind":"repeated_error",
                    "attempts":attempts,
                    "generation":monotonic_millis(),
                }),
                CallbackBehavior::Panic => serde_json::json!({"kind":"panic"}),
            };
            write_json(supervisor.callback_file(&participant), &value).await?;
            let hit_key = format!("callback/{participant}");
            let hit_before = supervisor.fault_hit_count(&hit_key);
            supervisor.record_fault_activation(&FaultActivation {
                key: hit_key.clone(),
                configured: true,
                hit_count: hit_before,
            })?;
            let release_delay = env::var("CLUSTODIAN_CHAOS_CALLBACK_DURATION_MILLISECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(250);
            Ok(Some(FaultScope::Callback {
                participant,
                behavior,
                release_delay: std::time::Duration::from_millis(release_delay),
                hit_key,
                hit_before,
            }))
        }
        Fault::EtcdCompaction => {
            let snapshot = observer
                .snapshot()
                .await
                .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;
            let endpoints = required_env("CLUSTODIAN_CHAOS_ETCD_ENDPOINTS")?;
            let prefix = required_env("CLUSTODIAN_CHAOS_PREFIX")?;
            let cluster =
                env::var("CLUSTODIAN_CHAOS_CLUSTER").unwrap_or_else(|_| String::from("chaos"));
            let revision = Revision::new(snapshot.observer_revision.value())
                .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;
            connect(&endpoints, &prefix, &cluster)
                .await?
                .compact(revision)
                .await
                .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;
            Ok(None)
        }
        Fault::EtcdMemberRestart { member } => {
            control_etcd_member(&member, EtcdMemberOperation::Restart)?;
            Ok(None)
        }
        Fault::Clock {
            participant,
            perturbation:
                clustodian_chaos::ClockPerturbation::Suspend { milliseconds }
                | clustodian_chaos::ClockPerturbation::DelayedKeepalive { milliseconds },
        } => {
            supervisor.suspend_participant(&participant)?;
            Ok(Some(FaultScope::Clock {
                duration: std::time::Duration::from_millis(milliseconds),
                started: std::time::Instant::now(),
            }))
        }
        Fault::Clock { perturbation, .. } => Err(ChaosError::InvalidArguments(format!(
            "clock perturbation requires an isolated time namespace: {perturbation:?}"
        ))),
        Fault::Failpoint { point, behavior } => {
            let action = match behavior {
                clustodian_chaos::FailpointBehavior::Pause { milliseconds } => {
                    format!("sleep({milliseconds})")
                }
                clustodian_chaos::FailpointBehavior::ReturnError => {
                    String::from("return(injected chaos error)")
                }
                clustodian_chaos::FailpointBehavior::Panic => {
                    String::from("panic(injected chaos failpoint)")
                }
                clustodian_chaos::FailpointBehavior::HardAbort => String::from("return(abort)"),
            };
            supervisor.set_failpoint(&point, &action)?;
            let hit_key = format!("failpoint/{point}");
            let hit_before = supervisor.fault_hit_count(&hit_key);
            supervisor.record_fault_activation(&FaultActivation {
                key: hit_key.clone(),
                configured: true,
                hit_count: hit_before,
            })?;
            Ok(Some(FaultScope::Failpoint {
                point,
                behavior,
                hit_key,
                hit_before,
            }))
        }
    }
}

async fn deactivate_fault(
    supervisor: &mut ProcessSupervisor,
    scope: &FaultScope,
) -> Result<(), ChaosError> {
    match scope {
        FaultScope::Network {
            script,
            proxies,
            name,
            toxic,
            value,
            jitter,
            stream,
            duration,
            started,
        } => {
            let elapsed = started.elapsed();
            if elapsed < *duration {
                tokio::time::sleep(*duration - elapsed).await;
            }
            for proxy in proxies {
                let result = run_toxic(ToxicRequest {
                    script,
                    proxy,
                    name,
                    toxic,
                    value: *value,
                    jitter: *jitter,
                    stream,
                    add: false,
                });
                supervisor.mark_toxic(proxy, name, false);
                result?;
            }
        }
        FaultScope::Callback {
            participant,
            behavior,
            release_delay,
            ..
        } => {
            tokio::time::sleep(*release_delay).await;
            if let CallbackBehavior::Block { token } = behavior {
                tokio::fs::write(
                    supervisor
                        .callback_file(participant)
                        .with_file_name(format!("release-{token}")),
                    [],
                )
                .await
                .map_err(ChaosError::Io)?;
            }
            write_json(
                supervisor.callback_file(participant),
                &serde_json::json!({"kind":"succeed_immediately"}),
            )
            .await?;
        }
        FaultScope::Clock { duration, started } => {
            let elapsed = started.elapsed();
            if elapsed < *duration {
                tokio::time::sleep(*duration - elapsed).await;
            }
            supervisor.resume_all()?;
        }
        FaultScope::Failpoint {
            point, behavior, ..
        } => {
            let duration = match behavior {
                clustodian_chaos::FailpointBehavior::Pause { milliseconds } => {
                    std::time::Duration::from_millis(*milliseconds)
                }
                clustodian_chaos::FailpointBehavior::ReturnError
                | clustodian_chaos::FailpointBehavior::Panic
                | clustodian_chaos::FailpointBehavior::HardAbort => {
                    std::time::Duration::from_millis(100)
                }
            };
            tokio::time::sleep(duration).await;
            supervisor.set_failpoint(point, "off")?;
        }
    }
    Ok(())
}

fn assert_fault_hit(supervisor: &ProcessSupervisor, scope: &FaultScope) -> Result<(), ChaosError> {
    let (key, hit_before) = match scope {
        FaultScope::Callback {
            hit_key,
            hit_before,
            ..
        }
        | FaultScope::Failpoint {
            hit_key,
            hit_before,
            ..
        } => (hit_key, hit_before),
        FaultScope::Network { .. } | FaultScope::Clock { .. } => return Ok(()),
    };
    let hit_count = supervisor.fault_hit_count(key);
    supervisor.record_fault_activation(&FaultActivation {
        key: key.clone(),
        configured: true,
        hit_count,
    })?;
    if hit_count <= *hit_before {
        return Err(ChaosError::Invariant(
            clustodian_chaos::InvariantViolation {
                class: clustodian_chaos::InvariantClass::Convergence,
                name: String::from("fault_boundary_reached"),
                detail: format!("configured fault {key} was never reached"),
                observer_revision: 0,
            },
        ));
    }
    Ok(())
}

struct ToxicRequest<'a> {
    script: &'a str,
    proxy: &'a str,
    name: &'a str,
    toxic: &'a str,
    value: u64,
    jitter: u64,
    stream: &'a str,
    add: bool,
}

fn run_toxic(request: ToxicRequest<'_>) -> Result<(), ChaosError> {
    let toxiproxy_url = env::var("CLUSTODIAN_CHAOS_TOXIPROXY_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
        .ok_or_else(|| {
            ChaosError::InvalidArguments(String::from(
                "network faults require a Toxiproxy runtime; use Docker mode",
            ))
        })?;
    let command = if request.add { "add" } else { "remove" };
    let args = vec![
        request.script.to_owned(),
        String::from("--url"),
        toxiproxy_url,
        String::from("--proxy"),
        request.proxy.to_owned(),
        String::from("--name"),
        request.name.to_owned(),
        String::from("--stream"),
        request.stream.to_owned(),
        String::from("--toxic"),
        request.toxic.to_owned(),
        String::from("--value"),
        request.value.to_string(),
        String::from("--jitter"),
        request.jitter.to_string(),
        command.to_owned(),
    ];
    let status = std::process::Command::new("python3")
        .args(args)
        .status()
        .map_err(ChaosError::Io)?;
    if status.success() {
        Ok(())
    } else {
        Err(ChaosError::InvalidArguments(format!(
            "Toxiproxy command failed for {}",
            request.proxy
        )))
    }
}

#[derive(Clone, Copy)]
enum EtcdMemberOperation {
    Stop,
    Start,
    Restart,
}

fn control_etcd_member(member: &str, operation: EtcdMemberOperation) -> Result<(), ChaosError> {
    let service = match member {
        "etcd1" | "etcd2" | "etcd3" => member,
        _ => {
            return Err(ChaosError::InvalidArguments(format!(
                "unknown etcd member {member}"
            )))
        }
    };
    if env::var("CLUSTODIAN_M13_RUNTIME").as_deref() == Ok("local") {
        if matches!(operation, EtcdMemberOperation::Stop) {
            let pid_file = required_env("CLUSTODIAN_M13_LOCAL_PID_FILE")?;
            let index = ["etcd1", "etcd2", "etcd3"]
                .iter()
                .position(|candidate| *candidate == service)
                .expect("validated etcd member");
            if let Some(pid) = std::fs::read_to_string(pid_file)
                .map_err(ChaosError::Io)?
                .lines()
                .nth(index)
                .and_then(|pid| pid.parse::<i32>().ok())
            {
                let _ = std::process::Command::new("kill")
                    .args(["-TERM", &pid.to_string()])
                    .status()
                    .map_err(ChaosError::Io)?;
            }
            return Ok(());
        }
        let script = env::var("CLUSTODIAN_M13_LOCAL_ETCD_RESTART_SCRIPT").unwrap_or_else(|_| {
            String::from("tools/clustodian-chaos/scripts/restart-local-m13-member.sh")
        });
        let status = std::process::Command::new(script)
            .arg(service)
            .status()
            .map_err(ChaosError::Io)?;
        if !status.success() {
            return Err(ChaosError::InvalidArguments(format!(
                "failed to start etcd member {service}"
            )));
        }
        return Ok(());
    }
    let compose = env::var("CLUSTODIAN_CHAOS_COMPOSE_FILE")
        .unwrap_or_else(|_| String::from("tools/clustodian-chaos/docker-compose.yml"));
    let command = match operation {
        EtcdMemberOperation::Stop => "stop",
        EtcdMemberOperation::Start => "start",
        EtcdMemberOperation::Restart => "restart",
    };
    let mut args = vec![
        String::from("compose"),
        String::from("-p"),
        env::var("CLUSTODIAN_M13_COMPOSE_PROJECT")
            .unwrap_or_else(|_| String::from("clustodian-m13")),
        String::from("-f"),
        compose,
        String::from(command),
        String::from(service),
    ];
    let status = std::process::Command::new("docker")
        .args(std::mem::take(&mut args))
        .status()
        .map_err(ChaosError::Io)?;
    if status.success() {
        Ok(())
    } else {
        Err(ChaosError::InvalidArguments(format!(
            "failed to {command} etcd member {service}"
        )))
    }
}

async fn quorum_loss(arguments: Vec<String>) -> Result<(), ChaosError> {
    let endpoints = required_env("CLUSTODIAN_CHAOS_ETCD_ENDPOINTS")?;
    let controller_endpoints =
        env::var("CLUSTODIAN_CHAOS_CONTROLLER_ENDPOINTS").unwrap_or_else(|_| endpoints.clone());
    let participant_endpoints =
        env::var("CLUSTODIAN_CHAOS_PARTICIPANT_ENDPOINTS").unwrap_or_else(|_| endpoints.clone());
    let observer_endpoints = required_env("CLUSTODIAN_CHAOS_OBSERVER_ENDPOINTS")?;
    let prefix = optional_string(&arguments, "--prefix")?
        .or_else(|| env::var("CLUSTODIAN_CHAOS_PREFIX").ok())
        .unwrap_or_else(|| format!("/clustodian/m13/quorum-loss-{}", std::process::id()));
    let work_directory = optional_string(&arguments, "--work-dir")?
        .map(PathBuf::from)
        .or_else(|| {
            env::var("CLUSTODIAN_CHAOS_WORK_DIR")
                .ok()
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| PathBuf::from("target/clustodian-chaos/quorum-loss"));
    let cluster = env::var("CLUSTODIAN_CHAOS_CLUSTER").unwrap_or_else(|_| String::from("chaos"));
    let node_binary = env::var("CLUSTODIAN_CHAOS_NODE_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/debug/clustodian-chaos-node"));
    let config = SeededGenerator::new(0).generate("pr", 0).initial_config;
    let admin = ClusterAdmin::new(connect(&endpoints, &prefix, &cluster).await?);
    let observer = ClusterObserver::new(connect(&observer_endpoints, &prefix, &cluster).await?);
    admin
        .ensure_cluster(&cluster)
        .await
        .map_err(|error| ChaosError::InvalidArguments(format!("ensure cluster: {error}")))?;
    for participant in &config.participants {
        put_instance(&admin, participant).await?;
    }
    let mut supervisor = ProcessSupervisor::new(
        node_binary,
        &work_directory,
        cluster.clone(),
        prefix,
        controller_endpoints,
        participant_endpoints,
        observer_endpoints,
        toxiproxy_enabled(),
    )?;
    supervisor.start_observer()?;
    for participant in &config.participants {
        supervisor.start_participant(participant)?;
    }
    for controller in &config.controllers {
        supervisor.start_controller(controller)?;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    for resource in &config.resources {
        put_resource(&admin, resource).await?;
    }
    wait_for_convergence(&observer, 30_000, &config).await?;
    let before = observer
        .snapshot()
        .await
        .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;

    control_etcd_member("etcd2", EtcdMemberOperation::Stop)?;
    control_etcd_member("etcd3", EtcdMemberOperation::Stop)?;
    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
    let unavailable_error = if let Ok(Ok(snapshot)) =
        tokio::time::timeout(std::time::Duration::from_secs(3), observer.snapshot()).await
    {
        if snapshot.observer_revision.value() > before.observer_revision.value() {
            Some(ChaosError::Invariant(
                clustodian_chaos::InvariantViolation {
                    class: clustodian_chaos::InvariantClass::Safety,
                    name: String::from("quorum_loss_no_progress"),
                    detail: String::from("etcd advanced while quorum was unavailable"),
                    observer_revision: snapshot.observer_revision.value(),
                },
            ))
        } else {
            check_safety(&snapshot)
                .into_iter()
                .next()
                .map(ChaosError::Invariant)
        }
    } else {
        None
    };

    control_etcd_member("etcd2", EtcdMemberOperation::Start)?;
    wait_for_observer_recovery(&observer, before.observer_revision.value()).await?;
    control_etcd_member("etcd3", EtcdMemberOperation::Start)?;
    wait_for_observer_recovery(&observer, before.observer_revision.value()).await?;
    if let Some(error) = unavailable_error {
        Err(error)
    } else {
        wait_for_convergence(&observer, 60_000, &config).await
    }
}

async fn wait_for_observer_recovery(
    observer: &ClusterObserver,
    minimum_revision: i64,
) -> Result<(), ChaosError> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Ok(snapshot) = observer.snapshot().await {
            if snapshot.observer_revision.value() >= minimum_revision {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ChaosError::InvalidArguments(String::from(
                "etcd quorum did not recover",
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn wait_for_quiescence(
    supervisor: &mut ProcessSupervisor,
    observer: &ClusterObserver,
) -> Result<(), ChaosError> {
    let stability = std::time::Duration::from_millis(
        env::var("CLUSTODIAN_CHAOS_STABILITY_MILLISECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2_000),
    );
    let timeout = std::time::Duration::from_millis(
        env::var("CLUSTODIAN_CHAOS_QUIESCENCE_TIMEOUT_MILLISECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(60_000),
    );
    let deadline = tokio::time::Instant::now() + timeout;
    let mut stable_since = None;
    while tokio::time::Instant::now() < deadline {
        let snapshot = observer
            .snapshot()
            .await
            .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;
        let observer_connected = true;
        // The successful authoritative observer snapshot is already the
        // liveness/quorum probe. A second full namespace read here would
        // amplify load precisely while a large reconciliation is settling.
        let etcd_available = observer_connected;
        let callback_hanging = supervisor.callback_is_hanging("node-1").unwrap_or(true);
        let inputs = clustodian_chaos::QuiescenceInputs {
            no_action_executing: true,
            no_fault_awaiting_completion: true,
            stopped_processes_resumed: supervisor.all_processes_resumed(),
            toxics_removed: !supervisor.has_active_toxics(),
            network_paths_restored: !supervisor.has_active_toxics(),
            callback_barriers_released: true,
            no_hanging_callbacks: !callback_hanging,
            etcd_quorum_available: etcd_available,
            etcd_endpoints_reachable: etcd_available,
            eligible_controller_running: snapshot.controllers.active.len() == 1
                && supervisor.expected_controller_membership_healthy(&snapshot),
            minimum_participants_healthy: supervisor.expected_processes_healthy(&snapshot),
            clock_perturbations_removed: true,
            observer_connected,
            observer_caught_up: snapshot.processed_revision.is_some_and(|revision| {
                revision.value() >= snapshot.authoritative_revision.value()
            }),
        };
        supervisor.record_quiescence(&clustodian_chaos::QuiescenceRecord {
            monotonic_millis: monotonic_millis(),
            observer_revision: snapshot.observer_revision.value(),
            authoritative_revision: snapshot.authoritative_revision.value(),
            processed_revision: snapshot.processed_revision.map(|revision| revision.value()),
            inputs: inputs.clone(),
            stable: inputs.is_quiescent(),
        })?;
        if inputs.is_quiescent() {
            let since = stable_since.get_or_insert_with(tokio::time::Instant::now);
            if since.elapsed() >= stability {
                return Ok(());
            }
        } else {
            stable_since = None;
            if !supervisor.all_processes_resumed() {
                supervisor.resume_all()?;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err(ChaosError::InvalidArguments(String::from(
        "quiescence stability interval was not reached",
    )))
}

async fn wait_for_convergence(
    observer: &ClusterObserver,
    deadline_milliseconds: u64,
    config: &ClusterConfig,
) -> Result<(), ChaosError> {
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_millis(deadline_milliseconds);
    loop {
        let snapshot = observer
            .snapshot()
            .await
            .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;
        if let Some(violation) = check_safety(&snapshot).into_iter().next() {
            return Err(ChaosError::Invariant(violation));
        }
        if check_convergence(&snapshot, config).is_empty() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ChaosError::InvalidArguments(String::from(
                "convergence deadline expired",
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn assert_idle(
    supervisor: &ProcessSupervisor,
    observer: &ClusterObserver,
    seconds: u64,
) -> Result<(), ChaosError> {
    let before = observer
        .snapshot()
        .await
        .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;
    let process_before = supervisor.process_metrics();
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    let after = observer
        .snapshot()
        .await
        .map_err(|error| ChaosError::InvalidArguments(error.to_string()))?;
    let process_after = supervisor.process_metrics();
    let revision_delta = after.observer_revision.value() - before.observer_revision.value();
    let max_revision_delta = env::var("CLUSTODIAN_CHAOS_IDLE_MAX_REVISION_DELTA")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let max_cpu_jiffies = env::var("CLUSTODIAN_CHAOS_IDLE_MAX_CPU_JIFFIES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(500);
    let cpu_jiffies_delta = process_after
        .cpu_jiffies
        .saturating_sub(process_before.cpu_jiffies);
    if cpu_jiffies_delta > max_cpu_jiffies {
        return Err(ChaosError::Invariant(
            clustodian_chaos::InvariantViolation {
                class: clustodian_chaos::InvariantClass::Convergence,
                name: String::from("idle_cpu"),
                detail: format!(
                "idle window consumed {cpu_jiffies_delta} CPU jiffies, maximum is {max_cpu_jiffies}"
            ),
                observer_revision: after.observer_revision.value(),
            },
        ));
    }
    if revision_delta > max_revision_delta {
        return Err(ChaosError::Invariant(clustodian_chaos::InvariantViolation {
            class: clustodian_chaos::InvariantClass::Convergence,
            name: String::from("idle_semantic_writes"),
            detail: format!(
                "idle window advanced etcd revision by {revision_delta}, maximum is {max_revision_delta}"
            ),
            observer_revision: after.observer_revision.value(),
        }));
    }
    if before.external_view != after.external_view
        || before.pending_transitions != after.pending_transitions
        || before.controllers != after.controllers
    {
        return Err(ChaosError::Invariant(
            clustodian_chaos::InvariantViolation {
                class: clustodian_chaos::InvariantClass::Convergence,
                name: String::from("idle_semantic_state"),
                detail: String::from("semantic state changed during idle window"),
                observer_revision: after.observer_revision.value(),
            },
        ));
    }
    supervisor.record_idle_window(seconds, revision_delta, process_before, process_after)?;
    Ok(())
}

async fn put_instance(
    admin: &ClusterAdmin,
    participant: &ParticipantConfig,
) -> Result<(), ChaosError> {
    admin
        .put_instance(InstanceSpec {
            instance_id: participant.id.clone(),
            zone: participant.zone.clone(),
        })
        .await
        .map_err(|error| ChaosError::InvalidArguments(error.to_string()))
}

async fn put_resource(admin: &ClusterAdmin, resource: &ResourceConfig) -> Result<(), ChaosError> {
    let placement = match &resource.placement {
        PlacementConfig::Crush => PlacementSpec::Crush,
        PlacementConfig::SemiAuto { preference_lists } => PlacementSpec::SemiAuto {
            preference_lists: preference_lists.clone(),
        },
    };
    admin
        .put_resource(ResourceSpec {
            name: resource.name.clone(),
            partitions: resource.partitions.len(),
            replicas: resource.replicas,
            state_model: String::from("LeaderStandby"),
            placement,
        })
        .await
        .map_err(|error| ChaosError::InvalidArguments(error.to_string()))
}

async fn connect(
    endpoints: &str,
    prefix: &str,
    cluster: &str,
) -> Result<EtcdCoordination, ChaosError> {
    let endpoints = endpoints
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    EtcdCoordination::connect_endpoints(endpoints, prefix.to_owned(), cluster.to_owned())
        .await
        .map_err(|error| ChaosError::InvalidArguments(error.to_string()))
}

fn default_budget(profile: &str) -> clustodian_chaos::FaultBudget {
    if profile == "pr" {
        clustodian_chaos::FaultBudget {
            etcd_failures_remaining: 0,
            controller_failures_remaining: 3,
            participant_failures_remaining: 3,
            network_partitions_remaining: 2,
            clock_faults_remaining: 2,
        }
    } else {
        clustodian_chaos::FaultBudget {
            etcd_failures_remaining: 3,
            controller_failures_remaining: 8,
            participant_failures_remaining: 8,
            network_partitions_remaining: 8,
            clock_faults_remaining: 2,
        }
    }
}

fn toxiproxy_enabled() -> bool {
    env::var("CLUSTODIAN_CHAOS_TOXIPROXY_URL")
        .ok()
        .is_some_and(|value| !value.trim().is_empty())
}

fn required_env(name: &str) -> Result<String, ChaosError> {
    env::var(name).map_err(|_| ChaosError::InvalidArguments(format!("{name} is required")))
}

fn failure_artifact(trace: &Trace, error: &ChaosError, replay: ReplayStats) -> FailureArtifact {
    let work = env::var("CLUSTODIAN_CHAOS_WORK_DIR")
        .unwrap_or_else(|_| String::from("target/clustodian-chaos"));
    let work_path = Path::new(&work);
    let invariant_snapshots = read_json_lines(&work_path.join("observations.jsonl"));
    let observer_revisions = invariant_snapshots
        .iter()
        .filter_map(|snapshot| {
            snapshot
                .get("observer_revision")
                .and_then(serde_json::Value::as_object)
                .and_then(|revision| revision.get("value"))
                .and_then(serde_json::Value::as_i64)
                .or_else(|| {
                    snapshot
                        .get("observer_revision")
                        .and_then(serde_json::Value::as_i64)
                })
        })
        .collect();
    FailureArtifact {
        artifact_schema_version: clustodian_chaos::ARTIFACT_SCHEMA_VERSION,
        seed: trace.seed,
        source_revision: git_revision(),
        build_identity: env::var("CLUSTODIAN_CHAOS_BUILD_ID")
            .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_owned()),
        trace: trace.clone(),
        events: vec![clustodian_chaos::RecordedEvent {
            sequence: 0,
            monotonic_millis: monotonic_millis(),
            kind: String::from("failure"),
            detail: error.to_string(),
        }],
        invariant_failure: match error {
            ChaosError::Invariant(violation) => Some(violation.clone()),
            _ => None,
        },
        quiescence: read_json_lines(&work_path.join("quiescence.jsonl"))
            .into_iter()
            .filter_map(|value| serde_json::from_value(value).ok())
            .collect(),
        evidence_directory: work.clone(),
        replay,
        process_ids: read_json_or_default(&work_path.join("processes.json")),
        observer_revisions,
        invariant_snapshots,
        callback_state: read_callback_state(work_path),
        failpoints_active: std::fs::read_to_string(work_path.join("failpoints.txt"))
            .ok()
            .into_iter()
            .flat_map(|value| value.split(';').map(str::to_owned).collect::<Vec<_>>())
            .collect(),
        clock_perturbations: trace
            .faults
            .iter()
            .filter_map(|fault| match &fault.fault {
                Fault::Clock { perturbation, .. } => Some(perturbation.clone()),
                Fault::Network { .. }
                | Fault::EtcdMemberRestart { .. }
                | Fault::EtcdCompaction
                | Fault::Callback { .. }
                | Fault::Failpoint { .. } => None,
            })
            .collect(),
        toxiproxy_url: env::var("CLUSTODIAN_CHAOS_TOXIPROXY_URL").ok(),
        fault_activations: read_json_lines(&work_path.join("fault-activations.jsonl"))
            .into_iter()
            .filter_map(|value| serde_json::from_value(value).ok())
            .collect(),
    }
}

fn capture_failure_evidence(work: String) {
    let path = Path::new(&work);
    if env::var("CLUSTODIAN_M13_RUNTIME").as_deref() == Ok("local") {
        if let Ok(root) = env::var("CLUSTODIAN_M13_LOCAL_ROOT") {
            let mut logs = Vec::new();
            for member in ["etcd1", "etcd2", "etcd3"] {
                let log = Path::new(&root).join(member).join("etcd.log");
                if let Ok(bytes) = std::fs::read(log) {
                    logs.extend_from_slice(format!("== {member} ==\n").as_bytes());
                    logs.extend_from_slice(&bytes);
                }
            }
            let _ = std::fs::write(path.join("etcd.log"), logs);
        }
        let _ = std::fs::write(path.join("toxiproxy.json"), b"{}\n");
        return;
    }
    let compose = env::var("CLUSTODIAN_CHAOS_COMPOSE_FILE")
        .unwrap_or_else(|_| String::from("tools/clustodian-chaos/docker-compose.yml"));
    let _ = std::fs::write(
        path.join("etcd.log"),
        std::process::Command::new("docker")
            .args([
                "compose",
                "-f",
                &compose,
                "logs",
                "--no-color",
                "etcd1",
                "etcd2",
                "etcd3",
            ])
            .output()
            .map(|output| output.stdout)
            .unwrap_or_default(),
    );
    if let Some(toxiproxy_url) = env::var("CLUSTODIAN_CHAOS_TOXIPROXY_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    {
        let _ = std::fs::write(
            path.join("toxiproxy.json"),
            std::process::Command::new("curl")
                .args([
                    "--silent",
                    "--show-error",
                    &format!("{toxiproxy_url}/proxies"),
                ])
                .output()
                .map(|output| output.stdout)
                .unwrap_or_default(),
        );
    } else {
        let _ = std::fs::write(path.join("toxiproxy.json"), b"{}\n");
    }
}

fn read_json_lines(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .ok()
        .into_iter()
        .flat_map(|contents| {
            contents
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn read_json_or_default<T: for<'de> serde::Deserialize<'de> + Default>(path: &Path) -> T {
    std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn read_callback_state(path: &Path) -> std::collections::BTreeMap<String, serde_json::Value> {
    let mut state = std::collections::BTreeMap::new();
    let Ok(entries) = std::fs::read_dir(path) else {
        return state;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("callback-") && name.ends_with(".json") {
            if let Ok(bytes) = std::fs::read(entry.path()) {
                if let Ok(value) = serde_json::from_slice(&bytes) {
                    state.insert(name.into_owned(), value);
                }
            }
        }
    }
    state
}

fn git_revision() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| String::from("unknown"))
}

async fn replay(arguments: Vec<String>) -> Result<(), ChaosError> {
    let path = arguments
        .first()
        .ok_or_else(|| ChaosError::InvalidArguments(String::from("replay requires an artifact")))?;
    let attempts = optional_u64(&arguments, "--attempts")?.unwrap_or(10) as u32;
    if attempts == 0 {
        return Err(ChaosError::InvalidArguments(String::from(
            "--attempts must be positive",
        )));
    }
    let bytes = tokio::fs::read(path).await.map_err(ChaosError::Io)?;
    if let Ok(artifact) = serde_json::from_slice::<FailureArtifact>(&bytes) {
        let stats = if env::var_os("CLUSTODIAN_CHAOS_ETCD_ENDPOINTS").is_some() {
            replay_trace(
                &artifact.trace,
                artifact.invariant_failure.as_ref(),
                Some(attempts),
            )
            .await
        } else {
            println!("replay requires CLUSTODIAN_CHAOS_ETCD_ENDPOINTS for real-stack attempts");
            ReplayStats::new(0, 0)
        };
        let mut updated = artifact.clone();
        updated.replay = stats.clone();
        let replay_artifact = Path::new(path).with_file_name(format!(
            "{}-replayed.json",
            Path::new(path)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("failure")
        ));
        write_json(&replay_artifact, &updated).await?;
        println!(
            "replay seed={} attempts={} reproduced={} classification={:?} rate={:.2} artifact={}",
            artifact.seed,
            stats.attempts,
            stats.reproduced,
            stats.classification,
            stats.reproduction_rate,
            replay_artifact.display()
        );
        return Ok(());
    }
    let trace: Trace = serde_json::from_slice(&bytes).map_err(ChaosError::Serialization)?;
    let replay_path = Path::new(path).with_extension("replay.json");
    write_json(&replay_path, &trace).await?;
    println!(
        "logical replay seed={} attempts={} actions={} trace={}",
        trace.seed,
        attempts,
        trace.actions.len(),
        replay_path.display()
    );
    Ok(())
}

fn invariant_of(error: &ChaosError) -> Option<&clustodian_chaos::InvariantViolation> {
    match error {
        ChaosError::Invariant(violation) => Some(violation),
        ChaosError::Io(_) | ChaosError::Serialization(_) | ChaosError::InvalidArguments(_) => None,
    }
}

async fn replay_trace(
    trace: &Trace,
    expected: Option<&clustodian_chaos::InvariantViolation>,
    attempts_override: Option<u32>,
) -> ReplayStats {
    let attempts = attempts_override.unwrap_or_else(|| {
        env::var("CLUSTODIAN_CHAOS_REPLAY_ATTEMPTS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(clustodian_chaos::DEFAULT_REPLAY_ATTEMPTS)
    });
    let Some(base_prefix) = env::var("CLUSTODIAN_CHAOS_PREFIX").ok() else {
        return ReplayStats::new(0, 0);
    };
    let base_work = env::var("CLUSTODIAN_CHAOS_WORK_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/clustodian-chaos"));
    let mut reproduced = 0;
    for attempt in 0..attempts {
        let prefix = format!("{base_prefix}-replay-{}-{}", trace.seed, attempt + 1);
        let work = base_work.join(format!("replay-{}-{}", trace.seed, attempt + 1));
        if let Err(error) = execute_trace(trace, Some(&prefix), Some(&work)).await {
            let matches = match (expected, invariant_of(&error)) {
                (Some(wanted), Some(actual)) => wanted.name == actual.name,
                (Some(_), None) => false,
                (None, _) => true,
            };
            if matches {
                reproduced += 1;
            }
        }
    }
    ReplayStats::new(attempts, reproduced)
}

async fn minimize(arguments: Vec<String>) -> Result<(), ChaosError> {
    let path = arguments.first().ok_or_else(|| {
        ChaosError::InvalidArguments(String::from("minimize requires an artifact"))
    })?;
    let artifact: FailureArtifact = read_json(path).await?;
    let expected = artifact.invariant_failure.clone().ok_or_else(|| {
        ChaosError::InvalidArguments(String::from(
            "minimize requires an artifact with an invariant failure",
        ))
    })?;
    required_env("CLUSTODIAN_CHAOS_ETCD_ENDPOINTS")?;
    required_env("CLUSTODIAN_CHAOS_OBSERVER_ENDPOINTS")?;
    let base_prefix = required_env("CLUSTODIAN_CHAOS_PREFIX")?;
    let attempts = optional_u64(&arguments, "--attempts")?
        .or_else(|| {
            env::var("CLUSTODIAN_CHAOS_MINIMIZE_ATTEMPTS")
                .ok()
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or(1)
        .try_into()
        .map_err(|_| ChaosError::InvalidArguments(String::from("--attempts is too large")))?;
    if attempts == 0 {
        return Err(ChaosError::InvalidArguments(String::from(
            "--attempts must be positive",
        )));
    }

    let base_work = env::var("CLUSTODIAN_CHAOS_WORK_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/clustodian-chaos"));
    let mut tester = CandidateTester {
        expected: &expected,
        base_prefix: &base_prefix,
        base_work: &base_work,
        next_id: 0,
        attempts,
    };
    let original = artifact.trace.clone();
    let original_stats = tester.test(&original).await?;
    if original_stats.reproduced == 0 {
        return Err(ChaosError::InvalidArguments(format!(
            "original artifact did not reproduce invariant {} in {} attempt(s)",
            expected.name, attempts
        )));
    }

    let minimized = tester.minimize(original.clone()).await?;
    let final_stats = tester.test(&minimized).await?;
    let output = Path::new(path).with_file_name(format!("{}-min.json", artifact.seed));
    let mut minimized_artifact = artifact;
    minimized_artifact.trace = minimized.clone();
    minimized_artifact.replay = final_stats.clone();
    write_json(&output, &minimized_artifact).await?;
    println!(
        "minimized seed={} invariant={} actions={} -> {} faults={} -> {} candidates={} reproduction={}/{} output={}",
        minimized_artifact.seed,
        expected.name,
        original.actions.len(),
        minimized.actions.len(),
        original.faults.len(),
        minimized.faults.len(),
        tester.next_id,
        final_stats.reproduced,
        final_stats.attempts,
        output.display(),
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum CrashProcess {
    Controller,
    Participant,
}

struct CrashCase {
    point: &'static str,
    process: CrashProcess,
}

const CRASH_CASES: &[CrashCase] = &[
    CrashCase {
        point: "controller_after_lease_grant",
        process: CrashProcess::Controller,
    },
    CrashCase {
        point: "controller_after_election_candidate_registration",
        process: CrashProcess::Controller,
    },
    CrashCase {
        point: "controller_after_active_election_txn",
        process: CrashProcess::Controller,
    },
    CrashCase {
        point: "controller_after_election_acquisition",
        process: CrashProcess::Controller,
    },
    CrashCase {
        point: "controller_after_snapshot",
        process: CrashProcess::Controller,
    },
    CrashCase {
        point: "controller_after_reconcile_before_publish",
        process: CrashProcess::Controller,
    },
    CrashCase {
        point: "controller_before_output_txn",
        process: CrashProcess::Controller,
    },
    CrashCase {
        point: "controller_after_output_txn",
        process: CrashProcess::Controller,
    },
    CrashCase {
        point: "controller_after_output_txn_before_post_publish_resnapshot",
        process: CrashProcess::Controller,
    },
    CrashCase {
        point: "participant_after_lease_grant",
        process: CrashProcess::Participant,
    },
    CrashCase {
        point: "participant_after_live_registration",
        process: CrashProcess::Participant,
    },
    CrashCase {
        point: "participant_after_live_registration_before_watch",
        process: CrashProcess::Participant,
    },
    CrashCase {
        point: "participant_after_transition_delivery_before_callback",
        process: CrashProcess::Participant,
    },
    CrashCase {
        point: "participant_after_callback_success_before_current_state",
        process: CrashProcess::Participant,
    },
    CrashCase {
        point: "participant_before_completion_txn",
        process: CrashProcess::Participant,
    },
    CrashCase {
        point: "participant_after_completion_txn",
        process: CrashProcess::Participant,
    },
];

async fn crash_matrix(arguments: Vec<String>) -> Result<(), ChaosError> {
    let endpoints = required_env("CLUSTODIAN_CHAOS_ETCD_ENDPOINTS")?;
    let controller_endpoints =
        env::var("CLUSTODIAN_CHAOS_CONTROLLER_ENDPOINTS").unwrap_or_else(|_| endpoints.clone());
    let participant_endpoints =
        env::var("CLUSTODIAN_CHAOS_PARTICIPANT_ENDPOINTS").unwrap_or_else(|_| endpoints.clone());
    let observer_endpoints = required_env("CLUSTODIAN_CHAOS_OBSERVER_ENDPOINTS")?;
    let base_prefix = optional_string(&arguments, "--prefix")?
        .or_else(|| env::var("CLUSTODIAN_CHAOS_PREFIX").ok())
        .ok_or_else(|| ChaosError::InvalidArguments(String::from("--prefix is required")))?;
    let base_work = optional_string(&arguments, "--work-dir")?
        .map(PathBuf::from)
        .or_else(|| {
            env::var("CLUSTODIAN_CHAOS_WORK_DIR")
                .ok()
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| PathBuf::from("target/clustodian-chaos/crash-matrix"));
    let cluster = env::var("CLUSTODIAN_CHAOS_CLUSTER").unwrap_or_else(|_| String::from("chaos"));
    let node_binary = env::var("CLUSTODIAN_CHAOS_NODE_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/debug/clustodian-chaos-node"));
    let config = SeededGenerator::new(0).generate("pr", 0).initial_config;

    for (index, case) in CRASH_CASES.iter().enumerate() {
        run_crash_case(
            case,
            index,
            &config,
            &endpoints,
            &observer_endpoints,
            &base_prefix,
            &base_work,
            &cluster,
            &node_binary,
            &controller_endpoints,
            &participant_endpoints,
        )
        .await?;
        println!("crash window {}: PASS", case.point);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_crash_case(
    case: &CrashCase,
    index: usize,
    config: &ClusterConfig,
    endpoints: &str,
    observer_endpoints: &str,
    base_prefix: &str,
    base_work: &Path,
    cluster: &str,
    node_binary: &Path,
    controller_endpoints: &str,
    participant_endpoints: &str,
) -> Result<(), ChaosError> {
    let prefix = format!("{base_prefix}-crash-matrix-{index}");
    let work_directory = base_work.join(format!("{index}-{}", case.point));
    let admin = ClusterAdmin::new(connect(endpoints, &prefix, cluster).await?);
    let observer = ClusterObserver::new(connect(observer_endpoints, &prefix, cluster).await?);
    admin
        .ensure_cluster(cluster)
        .await
        .map_err(|error| ChaosError::InvalidArguments(format!("ensure cluster: {error}")))?;
    for participant in &config.participants {
        put_instance(&admin, participant).await?;
    }
    let mut supervisor = ProcessSupervisor::new(
        node_binary,
        &work_directory,
        cluster.to_owned(),
        prefix,
        controller_endpoints.to_owned(),
        participant_endpoints.to_owned(),
        observer_endpoints.to_owned(),
        toxiproxy_enabled(),
    )?;
    supervisor.start_observer()?;

    match case.process {
        CrashProcess::Controller => {
            for participant in &config.participants {
                supervisor.start_participant(participant)?;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            for resource in &config.resources {
                put_resource(&admin, resource).await?;
            }
            supervisor.set_failpoint(case.point, "return(abort)")?;
            supervisor.start_controller("controller-a")?;
            wait_for_crash_exit(&mut supervisor, case.process, "controller-a").await?;
            assert_named_fault_hit(&supervisor, case.point)?;
            supervisor.set_failpoint(case.point, "off")?;
            supervisor.start_controller("controller-a")?;
        }
        CrashProcess::Participant => {
            for participant in &config.participants[1..] {
                supervisor.start_participant(participant)?;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            for resource in &config.resources {
                put_resource(&admin, resource).await?;
            }
            supervisor.start_controller("controller-a")?;
            wait_for_convergence(&observer, 30_000, config).await?;
            supervisor.set_failpoint(case.point, "return(abort)")?;
            if case.point.contains("completion")
                || case.point.contains("transition_delivery")
                || case.point.contains("callback_success")
            {
                let mut changed = config.clone();
                let resource = changed
                    .resources
                    .iter_mut()
                    .find(|resource| resource.name == "cache")
                    .ok_or_else(|| {
                        ChaosError::InvalidArguments(String::from(
                            "crash matrix requires the cache resource",
                        ))
                    })?;
                match &mut resource.placement {
                    PlacementConfig::SemiAuto { preference_lists } => {
                        preference_lists.insert(
                            String::from("cache_0"),
                            vec![String::from("node-2"), String::from("node-3")],
                        );
                    }
                    PlacementConfig::Crush => {
                        return Err(ChaosError::InvalidArguments(String::from(
                            "cache crash-matrix resource must be semi-auto",
                        )));
                    }
                }
                let changed_resource = changed
                    .resources
                    .iter()
                    .find(|resource| resource.name == "cache")
                    .expect("resource was found above");
                put_resource(&admin, changed_resource).await?;
                wait_for_crash_exit(&mut supervisor, case.process, "node-1").await?;
                assert_named_fault_hit(&supervisor, case.point)?;
                supervisor.set_failpoint(case.point, "off")?;
                supervisor.start_participant(
                    config
                        .participants
                        .first()
                        .expect("crash matrix has node-1 as its first participant"),
                )?;
                wait_for_convergence(&observer, 30_000, &changed).await?;
                return Ok(());
            }
            supervisor.start_participant(
                config
                    .participants
                    .first()
                    .expect("crash matrix has node-1 as its first participant"),
            )?;
            wait_for_crash_exit(&mut supervisor, case.process, "node-1").await?;
            assert_named_fault_hit(&supervisor, case.point)?;
            supervisor.set_failpoint(case.point, "off")?;
            supervisor.start_participant(
                config
                    .participants
                    .first()
                    .expect("crash matrix has node-1 as its first participant"),
            )?;
        }
    }
    wait_for_convergence(&observer, 30_000, config).await
}

fn assert_named_fault_hit(supervisor: &ProcessSupervisor, point: &str) -> Result<(), ChaosError> {
    let key = format!("failpoint/{point}");
    let hit_count = supervisor.fault_hit_count(&key);
    supervisor.record_fault_activation(&FaultActivation {
        key: key.clone(),
        configured: true,
        hit_count,
    })?;
    if hit_count == 0 {
        return Err(ChaosError::Invariant(
            clustodian_chaos::InvariantViolation {
                class: clustodian_chaos::InvariantClass::Convergence,
                name: String::from("fault_boundary_reached"),
                detail: format!("configured fault {key} was never reached"),
                observer_revision: 0,
            },
        ));
    }
    Ok(())
}

async fn wait_for_crash_exit(
    supervisor: &mut ProcessSupervisor,
    process: CrashProcess,
    id: &str,
) -> Result<(), ChaosError> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let running = match process {
            CrashProcess::Controller => supervisor.controller_running(id),
            CrashProcess::Participant => supervisor.participant_running(id),
        };
        if !running {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ChaosError::InvalidArguments(format!(
                "failpoint process {id} did not abort"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

struct CandidateTester<'a> {
    expected: &'a clustodian_chaos::InvariantViolation,
    base_prefix: &'a str,
    base_work: &'a Path,
    next_id: usize,
    attempts: u32,
}

impl CandidateTester<'_> {
    async fn test(&mut self, trace: &Trace) -> Result<ReplayStats, ChaosError> {
        let candidate_id = self.next_id;
        self.next_id += 1;
        let mut reproduced = 0;
        for attempt in 0..self.attempts {
            let prefix = format!(
                "{}-minimize-{}-{}-{}-{}",
                self.base_prefix,
                std::process::id(),
                trace.seed,
                candidate_id,
                attempt + 1
            );
            let work = self.base_work.join(format!(
                "minimize-{}-{}-{}-{}",
                std::process::id(),
                trace.seed,
                candidate_id,
                attempt + 1
            ));
            if let Err(ChaosError::Invariant(actual)) =
                execute_trace(trace, Some(&prefix), Some(&work)).await
            {
                if actual.class == self.expected.class && actual.name == self.expected.name {
                    reproduced += 1;
                }
            }
        }
        Ok(ReplayStats::new(self.attempts, reproduced))
    }

    async fn reproduces(&mut self, trace: &Trace, phase: &str) -> Result<bool, ChaosError> {
        let stats = self.test(trace).await?;
        if stats.reproduced == 0 {
            return Ok(false);
        }
        println!(
            "minimize accepted phase={} actions={} faults={} reproduction={}/{}",
            phase,
            trace.actions.len(),
            trace.faults.len(),
            stats.reproduced,
            stats.attempts
        );
        Ok(true)
    }

    async fn minimize(&mut self, mut current: Trace) -> Result<Trace, ChaosError> {
        loop {
            let before = current.clone();
            current = self.minimize_action_ranges(current).await?;
            current = self.minimize_fault_ranges(current).await?;
            current = self.minimize_fault_parameters(current).await?;
            current = self.minimize_topology(current).await?;
            if current == before {
                return Ok(current);
            }
        }
    }

    async fn minimize_action_ranges(&mut self, mut current: Trace) -> Result<Trace, ChaosError> {
        let mut granularity = 2;
        loop {
            let length = current.actions.len();
            if length < 2 {
                return Ok(current);
            }
            let chunk_size = length.div_ceil(granularity);
            let mut accepted = None;
            for start in (0..length).step_by(chunk_size) {
                let end = (start + chunk_size).min(length);
                let Some(candidate) = remove_action_range(&current, start, end) else {
                    continue;
                };
                if self.reproduces(&candidate, "remove-actions").await? {
                    accepted = Some(candidate);
                    break;
                }
            }
            if let Some(candidate) = accepted {
                current = candidate;
                granularity = granularity.saturating_sub(1).max(2);
            } else if granularity < length {
                granularity = (granularity * 2).min(length);
            } else {
                return Ok(current);
            }
        }
    }

    async fn minimize_fault_ranges(&mut self, mut current: Trace) -> Result<Trace, ChaosError> {
        let mut granularity = 2;
        loop {
            let length = current.faults.len();
            if length < 2 {
                return Ok(current);
            }
            let chunk_size = length.div_ceil(granularity);
            let mut accepted = None;
            for start in (0..length).step_by(chunk_size) {
                let end = (start + chunk_size).min(length);
                let Some(candidate) = remove_fault_range(&current, start, end) else {
                    continue;
                };
                if self.reproduces(&candidate, "remove-faults").await? {
                    accepted = Some(candidate);
                    break;
                }
            }
            if let Some(candidate) = accepted {
                current = candidate;
                granularity = granularity.saturating_sub(1).max(2);
            } else if granularity < length {
                granularity = (granularity * 2).min(length);
            } else {
                return Ok(current);
            }
        }
    }

    async fn minimize_fault_parameters(&mut self, mut current: Trace) -> Result<Trace, ChaosError> {
        loop {
            let mut accepted = None;
            for fault_index in 0..current.faults.len() {
                for candidate in reduce_fault(&current, fault_index) {
                    if self.reproduces(&candidate, "reduce-fault").await? {
                        accepted = Some(candidate);
                        break;
                    }
                }
                if accepted.is_some() {
                    break;
                }
            }
            let Some(candidate) = accepted else {
                return Ok(current);
            };
            current = candidate;
        }
    }

    async fn minimize_topology(&mut self, mut current: Trace) -> Result<Trace, ChaosError> {
        loop {
            let mut accepted = None;
            for candidate in reduce_cluster_and_resources(&current) {
                if self.reproduces(&candidate, "reduce-topology").await? {
                    accepted = Some(candidate);
                    break;
                }
            }
            let Some(candidate) = accepted else {
                return Ok(current);
            };
            current = candidate;
        }
    }
}

fn required_u64(arguments: &[String], name: &str) -> Result<u64, ChaosError> {
    optional_u64(arguments, name)?
        .ok_or_else(|| ChaosError::InvalidArguments(format!("{name} is required")))
}

fn optional_u64(arguments: &[String], name: &str) -> Result<Option<u64>, ChaosError> {
    let Some(index) = arguments.iter().position(|argument| argument == name) else {
        return Ok(None);
    };
    let value = arguments
        .get(index + 1)
        .ok_or_else(|| ChaosError::InvalidArguments(format!("{name} requires a value")))?;
    value
        .parse()
        .map(Some)
        .map_err(|_| ChaosError::InvalidArguments(format!("invalid value for {name}: {value}")))
}

fn optional_string(arguments: &[String], name: &str) -> Result<Option<String>, ChaosError> {
    let Some(index) = arguments.iter().position(|argument| argument == name) else {
        return Ok(None);
    };
    arguments
        .get(index + 1)
        .cloned()
        .map(Some)
        .ok_or_else(|| ChaosError::InvalidArguments(format!("{name} requires a value")))
}

fn print_help() {
    println!(
        "clustodian-chaos run --seed <u64> [--steps <n>] [--profile <pr|nightly|soak>]\n\
         clustodian-chaos replay <failure.json> [--attempts <n>]\n\
         clustodian-chaos minimize <failure.json> [--attempts <n>]\n\
         clustodian-chaos crash-matrix [--prefix <prefix>] [--work-dir <path>]\n\
         clustodian-chaos quorum-loss [--prefix <prefix>] [--work-dir <path>]"
    );
}
