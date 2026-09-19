use clustodian::controller::{
    compute_intermediate_and_throttle, generate_transitions, select_transitions,
    OperationalPendingTransition, RebalanceType, ResourceTransitionInput,
    StateTransitionThrottleConfig, ThrottleScope,
};
use clustodian::model::{
    leader_standby, BestPossibleState, CurrentState, IdealState, InstanceId,
    ParticipantSessionState, PartitionId, ResourceId, SessionId, State, StateCardinality,
};
use clustodian::rebalance::{compute_crush_assignment, CrushInstance, CrushTopology};
use clustodian::transition::{PendingTransition, TransitionRequest};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::Path;
use std::process::ExitCode;

mod etcd;

#[derive(Debug, Deserialize)]
struct Scenario {
    scenario_version: u8,
    operation: String,
    #[serde(default)]
    case: Option<String>,
    #[serde(default)]
    parameters: Option<Map<String, Value>>,
    #[serde(default)]
    state_model: Option<StateModelSpec>,
    #[serde(default)]
    queries: Option<Vec<StateQuery>>,
    #[serde(default)]
    resource: Option<ResourceInput>,
    #[serde(default)]
    partitions: Option<Vec<String>>,
    #[serde(default)]
    instances: Option<InstanceInput>,
    #[serde(default)]
    current_state: Option<StateMap>,
    #[serde(default)]
    target_state: Option<StateMap>,
    #[serde(default)]
    replicas: Option<usize>,
    #[serde(default)]
    live_instances: Option<Vec<String>>,
    #[serde(default)]
    preference_lists: Option<BTreeMap<String, Vec<String>>>,
    #[serde(default)]
    state_counts: Option<Vec<StateCountSpec>>,
    #[serde(default)]
    max_partitions_per_instance: Option<i64>,
    #[serde(default)]
    topology: Option<TopologySpec>,
    #[serde(default)]
    candidate_transitions: Option<Vec<TransitionSpec>>,
    #[serde(default)]
    pending_transitions: Option<Vec<TransitionSpec>>,
    #[serde(default)]
    throttle_configs: Option<Vec<ThrottleConfigSpec>>,
    #[serde(default)]
    resources: Option<ResourceCollection>,
    #[serde(default)]
    routing_queries: Option<Vec<RoutingQuery>>,
    #[serde(default)]
    steps: Option<Vec<ParticipantSessionStep>>,
    #[serde(default)]
    session_comparisons: Option<Vec<SessionComparisonSpec>>,
}

type StateMap = BTreeMap<String, BTreeMap<String, String>>;

#[derive(Debug, Deserialize)]
struct StateModelSpec {
    kind: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct StateQuery {
    from: String,
    to: String,
}

#[derive(Debug, Deserialize)]
struct ResourceSpec {
    name: String,
    partitions: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ResourceInput {
    Detailed(ResourceSpec),
    Name(String),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum InstanceInput {
    Structured(Vec<InstanceSpec>),
    Names(Vec<String>),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ResourceCollection {
    M8(Vec<M8ResourceSpec>),
    M6(Vec<M6ResourceSpec>),
    M7(Vec<M7ResourceSpec>),
}

#[derive(Debug, Deserialize)]
struct M8ResourceSpec {
    name: String,
    #[serde(default)]
    state_model: Option<String>,
    partitions: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op")]
enum ParticipantSessionStep {
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
    #[serde(rename = "inject_session_current_state")]
    InjectSessionCurrentState {
        instance: String,
        session: String,
        resource: String,
        states: BTreeMap<String, String>,
    },
    #[serde(rename = "checkpoint")]
    Checkpoint { id: String },
}

#[derive(Debug, Deserialize)]
struct InstanceSpec {
    name: String,
    #[serde(default)]
    #[allow(dead_code)]
    session_id: Option<String>,
    #[serde(default)]
    domain: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct StateCountSpec {
    #[allow(dead_code)]
    state: String,
    count: i64,
}

#[derive(Debug, Deserialize)]
struct TopologySpec {
    path: String,
    fault_zone_type: String,
    end_node_type: String,
}

#[derive(Debug, Deserialize)]
struct TransitionSpec {
    #[serde(default)]
    resource: Option<String>,
    partition: String,
    instance: String,
    from: String,
    to: String,
}

#[derive(Debug, Deserialize)]
struct ThrottleConfigSpec {
    rebalance_type: String,
    scope: String,
    max_transitions: usize,
}

#[derive(Debug, Deserialize)]
struct M6ResourceSpec {
    name: String,
    replicas: usize,
    min_active_replicas: i64,
    preference_lists: BTreeMap<String, Vec<String>>,
    current_state: StateMap,
    best_possible_state: StateMap,
    selected_transitions: Vec<M6TransitionSpec>,
}

#[derive(Debug, Deserialize)]
struct M6TransitionSpec {
    resource: String,
    partition: String,
    instance: String,
    from: String,
    to: String,
    #[allow(dead_code)]
    message_type: String,
}

#[derive(Debug, Deserialize)]
struct M7ResourceSpec {
    name: String,
    current_state: StateMap,
}

#[derive(Debug, Deserialize)]
struct RoutingQuery {
    id: String,
    resource: String,
    partition: String,
    state: String,
}

#[derive(Debug, Deserialize)]
struct SessionComparisonSpec {
    left: String,
    right: String,
}

fn main() -> ExitCode {
    let mut arguments = env::args_os();
    let program = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .unwrap_or_else(|| String::from("clustodian-conformance"));

    let Some(argument) = arguments.next() else {
        eprintln!("{program}: usage: {program} <scenario.json>");
        return ExitCode::from(2);
    };
    if arguments.next().is_some() {
        eprintln!("{program}: usage: {program} <scenario.json>");
        return ExitCode::from(2);
    }
    if argument == "--help" || argument == "-h" {
        println!("Usage: {program} <scenario.json>");
        println!("\nSupports inspect_state_model, generate_transitions, compute_semi_auto_best_possible, compute_crush_assignment, select_transitions, compute_intermediate_and_throttle, compute_external_view_and_routing, participant_session_semantics, and etcd_coordination_semantics.");
        return ExitCode::SUCCESS;
    }

    match run(Path::new(&argument)) {
        Ok(result) => {
            println!(
                "{}",
                serde_json::to_string(&result).expect("conformance result is serializable")
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{program}: {error}");
            ExitCode::from(1)
        }
    }
}

fn run(path: &Path) -> Result<Value, Box<dyn std::error::Error>> {
    let scenario: Scenario = serde_json::from_str(&fs::read_to_string(path)?)?;
    if scenario.scenario_version != 1 {
        return Err("unsupported scenario_version; expected 1".into());
    }
    match scenario.operation.as_str() {
        "inspect_state_model" => {
            require_leader_standby(scenario.state_model.as_ref())?;
            inspect_state_model(&scenario)
        }
        "generate_transitions" => {
            require_leader_standby(scenario.state_model.as_ref())?;
            generate_transition_result(&scenario)
        }
        "compute_semi_auto_best_possible" => {
            require_leader_standby(scenario.state_model.as_ref())?;
            compute_semi_auto_result(&scenario)
        }
        "compute_crush_assignment" => compute_crush_result(&scenario),
        "select_transitions" => {
            require_leader_standby(scenario.state_model.as_ref())?;
            select_transition_result(&scenario)
        }
        "compute_intermediate_and_throttle" => {
            require_leader_standby(scenario.state_model.as_ref())?;
            compute_intermediate_and_throttle_result(&scenario)
        }
        "compute_external_view_and_routing" => compute_external_view_and_routing_result(&scenario),
        "participant_session_semantics" => {
            if env::var_os("CLUSTODIAN_M9_ETCD_ENDPOINT").is_some()
                && env::var_os("CLUSTODIAN_M9_ETCD_PREFIX").is_some()
            {
                run_async(etcd::participant_session_result(&scenario))
            } else {
                participant_session_result(&scenario)
            }
        }
        "etcd_coordination_semantics" => run_async(etcd::native_result(&scenario)),
        operation => Err(format!("unsupported operation: {operation}").into()),
    }
}

fn run_async<F>(future: F) -> Result<Value, Box<dyn std::error::Error>>
where
    F: std::future::Future<Output = Result<Value, Box<dyn std::error::Error>>>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(future)
}

fn participant_session_result(scenario: &Scenario) -> Result<Value, Box<dyn std::error::Error>> {
    let instance_names = scenario.named_instances()?;
    if instance_names.is_empty() {
        return Err("instances must not be empty".into());
    }
    let configured_instances = instance_names
        .iter()
        .map(|name| InstanceId::try_from(name.as_str()))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if configured_instances.len() != instance_names.len() {
        return Err("instances contains a duplicate".into());
    }

    let resource_specs = match scenario.resources.as_ref() {
        Some(ResourceCollection::M8(resources)) => resources,
        Some(ResourceCollection::M6(_)) | Some(ResourceCollection::M7(_)) => {
            return Err("participant_session_semantics requires M8 resources".into())
        }
        None => return Err("participant_session_semantics requires resources".into()),
    };
    let mut resource_partitions = BTreeMap::new();
    let leader_standby_initial_state = leader_standby().initial_state().clone();
    let mut participant = ParticipantSessionState::new();
    for resource_spec in resource_specs {
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
        participant.set_resource_initial_state(resource, leader_standby_initial_state.clone());
    }

    let steps = scenario
        .steps
        .as_ref()
        .ok_or("participant_session_semantics requires steps")?;
    let mut logical_sessions = BTreeMap::new();
    let mut checkpoints = Vec::new();
    for step in steps {
        match step {
            ParticipantSessionStep::Connect { instance, session } => {
                let instance_id = configured_instance(instance, &configured_instances)?;
                let session_id = participant.connect(instance_id)?;
                bind_logical_session(&mut logical_sessions, session, session_id)?;
            }
            ParticipantSessionStep::Disconnect { instance, session } => {
                let instance_id = configured_instance(instance, &configured_instances)?;
                let session_id = bound_session(&logical_sessions, session)?;
                participant.disconnect(&instance_id, session_id)?;
            }
            ParticipantSessionStep::ExpireAndReconnect {
                instance,
                from_session,
                to_session,
            } => {
                let instance_id = configured_instance(instance, &configured_instances)?;
                let old_session_id = bound_session(&logical_sessions, from_session)?;
                let new_session_id =
                    participant.expire_and_reconnect(&instance_id, old_session_id)?;
                bind_logical_session(&mut logical_sessions, to_session, new_session_id)?;
            }
            ParticipantSessionStep::PublishCurrentState {
                instance,
                session,
                resource,
                states,
            } => {
                let instance_id = configured_instance(instance, &configured_instances)?;
                let session_id = bound_session(&logical_sessions, session)?;
                let resource_id = configured_resource(resource, &resource_partitions)?;
                participant.publish_current_state(
                    &instance_id,
                    session_id,
                    resource_id,
                    parse_session_states(states)?,
                )?;
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
                participant.inject_session_current_state(
                    &instance_id,
                    session_id,
                    resource_id,
                    parse_session_states(states)?,
                )?;
            }
            ParticipantSessionStep::Checkpoint { id } => {
                if id.is_empty() {
                    return Err("checkpoint id must not be empty".into());
                }
                checkpoints.push(participant_checkpoint(
                    id,
                    &participant.snapshot(),
                    &logical_sessions,
                    &resource_partitions,
                )?);
            }
        }
    }

    let session_comparisons = scenario_session_comparisons(scenario, &logical_sessions)?;
    Ok(json!({
        "operation": "participant_session_semantics",
        "checkpoints": checkpoints,
        "session_comparisons": session_comparisons,
    }))
}

fn configured_instance(
    name: &str,
    configured_instances: &BTreeSet<InstanceId>,
) -> Result<InstanceId, Box<dyn std::error::Error>> {
    let instance = InstanceId::try_from(name)?;
    if !configured_instances.contains(&instance) {
        return Err(format!("unknown configured instance: {name}").into());
    }
    Ok(instance)
}

fn configured_resource(
    name: &str,
    resources: &BTreeMap<ResourceId, BTreeSet<PartitionId>>,
) -> Result<ResourceId, Box<dyn std::error::Error>> {
    let resource = ResourceId::try_from(name)?;
    if !resources.contains_key(&resource) {
        return Err(format!("unknown configured resource: {name}").into());
    }
    Ok(resource)
}

fn parse_session_states(
    states: &BTreeMap<String, String>,
) -> Result<BTreeMap<PartitionId, State>, Box<dyn std::error::Error>> {
    states
        .iter()
        .map(|(partition, state)| {
            Ok((
                PartitionId::try_from(partition.as_str())?,
                State::try_from(state.as_str())?,
            ))
        })
        .collect()
}

fn bound_session(
    logical_sessions: &BTreeMap<String, SessionId>,
    label: &str,
) -> Result<SessionId, Box<dyn std::error::Error>> {
    logical_sessions
        .get(label)
        .copied()
        .ok_or_else(|| format!("unknown session label: {label}").into())
}

fn bind_logical_session(
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

fn logical_session_label(
    logical_sessions: &BTreeMap<String, SessionId>,
    session_id: SessionId,
) -> Result<&str, Box<dyn std::error::Error>> {
    logical_sessions
        .iter()
        .find_map(|(label, bound)| (*bound == session_id).then_some(label.as_str()))
        .ok_or_else(|| format!("unbound production session: {session_id:?}").into())
}

fn participant_checkpoint(
    id: &str,
    snapshot: &clustodian::model::ParticipantSessionSnapshot,
    logical_sessions: &BTreeMap<String, SessionId>,
    resource_partitions: &BTreeMap<ResourceId, BTreeSet<PartitionId>>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut live_instances = Map::new();
    for (instance, live_instance) in snapshot.live_instances() {
        live_instances.insert(
            instance.as_str().to_owned(),
            Value::String(
                logical_session_label(logical_sessions, live_instance.session_id())?.to_owned(),
            ),
        );
    }

    let mut active_current_state = Map::new();
    for (instance, active) in snapshot.active_current_state() {
        let mut resources = Map::new();
        for (resource, partitions) in resource_partitions {
            let Some(current_state) = active.resources().get(resource) else {
                continue;
            };
            let mut partition_states = Map::new();
            for partition in partitions {
                if let Some(state) = current_state.state(partition, instance) {
                    partition_states.insert(
                        partition.as_str().to_owned(),
                        Value::String(state.as_str().to_owned()),
                    );
                }
            }
            if !partition_states.is_empty() {
                resources.insert(
                    resource.as_str().to_owned(),
                    Value::Object(partition_states),
                );
            }
        }

        active_current_state.insert(
            instance.as_str().to_owned(),
            json!({
                "session": logical_session_label(logical_sessions, active.session_id())?,
                "resources": resources,
            }),
        );
    }

    Ok(json!({
        "id": id,
        "live_instances": live_instances,
        "active_current_state": active_current_state,
    }))
}

fn scenario_session_comparisons(
    scenario: &Scenario,
    logical_sessions: &BTreeMap<String, SessionId>,
) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
    let Some(raw_comparisons) = scenario.session_comparisons.as_ref() else {
        return Ok(Vec::new());
    };
    raw_comparisons
        .iter()
        .map(|comparison| {
            let left = bound_session(logical_sessions, &comparison.left)?;
            let right = bound_session(logical_sessions, &comparison.right)?;
            Ok(json!({
                "left": comparison.left,
                "right": comparison.right,
                "equal": left == right,
            }))
        })
        .collect()
}

impl Scenario {
    fn structured_instances(
        &self,
        operation: &str,
    ) -> Result<&[InstanceSpec], Box<dyn std::error::Error>> {
        match self.instances.as_ref() {
            Some(InstanceInput::Structured(instances)) => Ok(instances),
            Some(InstanceInput::Names(_)) => {
                Err(format!("{operation} requires structured instances").into())
            }
            None => Err(format!("{operation} requires instances").into()),
        }
    }

    fn named_instances(&self) -> Result<&[String], Box<dyn std::error::Error>> {
        match self.instances.as_ref() {
            Some(InstanceInput::Names(instances)) => Ok(instances),
            Some(InstanceInput::Structured(_)) => {
                Err("compute_external_view_and_routing requires string instances".into())
            }
            None => Err("compute_external_view_and_routing requires instances".into()),
        }
    }
}

fn require_leader_standby(
    model: Option<&StateModelSpec>,
) -> Result<(), Box<dyn std::error::Error>> {
    let model = model.ok_or("state_model is required for this operation")?;
    if model.kind != "built_in" || model.name != "LeaderStandby" {
        return Err("M2 supports only the built-in LeaderStandby state model".into());
    }
    Ok(())
}

fn inspect_state_model(scenario: &Scenario) -> Result<Value, Box<dyn std::error::Error>> {
    let model = leader_standby();
    let queries = scenario
        .queries
        .as_ref()
        .ok_or("inspect_state_model requires queries")?;
    let mut next_states = Vec::with_capacity(queries.len());
    for query in queries {
        let from = State::try_from(query.from.as_str())?;
        let to = State::try_from(query.to.as_str())?;
        let next = model.next_state_toward(&from, &to).map(State::as_str);
        next_states.push(json!({
            "from": from.as_str(),
            "to": to.as_str(),
            "next": next,
        }));
    }

    let mut state_counts = Map::new();
    for state in model.states_in_priority_order() {
        let cardinality = model
            .cardinality_for(state)
            .ok_or_else(|| format!("missing cardinality for {}", state.as_str()))?;
        state_counts.insert(
            state.as_str().to_owned(),
            Value::String(cardinality_string(cardinality)),
        );
    }
    let transition_priority: Vec<String> = model
        .transitions_in_priority_order()
        .iter()
        .map(|transition| format!("{}-{}", transition.source(), transition.target()))
        .collect();
    Ok(json!({
        "implementation": "clustodian",
        "operation": "inspect_state_model",
        "state_model": {
            "name": model.name(),
            "initial_state": model.initial_state().as_str(),
            "valid": true,
            "top_state": model.highest_priority_state().as_str(),
            "single_top_state": model.has_single_highest_priority_state(),
            "states_priority": model
                .states_in_priority_order()
                .iter()
                .map(State::as_str)
                .collect::<Vec<_>>(),
            "transition_priority": transition_priority,
            "state_counts": state_counts,
            "next_states": next_states,
        },
    }))
}

fn generate_transition_result(scenario: &Scenario) -> Result<Value, Box<dyn std::error::Error>> {
    let resource = detailed_resource(scenario, "generate_transitions")?;
    if resource.partitions.is_empty() {
        return Err("resource.partitions must not be empty".into());
    }
    let instances = scenario.structured_instances("generate_transitions")?;
    if instances.is_empty() {
        return Err("instances must not be empty".into());
    }
    let current_map = scenario
        .current_state
        .as_ref()
        .ok_or("generate_transitions requires current_state")?;
    let target_map = scenario
        .target_state
        .as_ref()
        .ok_or("generate_transitions requires target_state")?;

    let valid_partitions: BTreeSet<&str> = resource.partitions.iter().map(String::as_str).collect();
    let valid_instances: BTreeSet<&str> = instances.iter().map(|item| item.name.as_str()).collect();
    if valid_partitions.len() != resource.partitions.len() {
        return Err("resource.partitions contains a duplicate".into());
    }
    if valid_instances.len() != instances.len() {
        return Err("instances contains a duplicate".into());
    }

    let resource_id = ResourceId::try_from(resource.name.as_str())?;
    let mut current = CurrentState::builder();
    add_state_map(
        &mut current,
        current_map,
        &valid_partitions,
        &valid_instances,
        "current_state",
    )?;
    let mut target = BestPossibleState::builder();
    add_state_map(
        &mut target,
        target_map,
        &valid_partitions,
        &valid_instances,
        "target_state",
    )?;

    let model = leader_standby();
    let current = current.build();
    let target = target.build();
    let requests = generate_transitions(&resource_id, &current, &target, &model)?;
    let transitions = requests
        .iter()
        .map(|request| {
            json!({
                "resource": request.resource().as_str(),
                "partition": request.partition().as_str(),
                "instance": request.instance().as_str(),
                "from": request.source_state().as_str(),
                "to": request.target_state().as_str(),
                "message_type": "STATE_TRANSITION",
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "implementation": "clustodian",
        "operation": "generate_transitions",
        "transitions": transitions,
    }))
}

fn compute_semi_auto_result(scenario: &Scenario) -> Result<Value, Box<dyn std::error::Error>> {
    let resource = detailed_resource(scenario, "compute_semi_auto_best_possible")?;
    let instances = scenario.structured_instances("compute_semi_auto_best_possible")?;
    let current_map = scenario
        .current_state
        .as_ref()
        .ok_or("compute_semi_auto_best_possible requires current_state")?;
    let replicas = scenario
        .replicas
        .ok_or("compute_semi_auto_best_possible requires replicas")?;
    let live_names = scenario
        .live_instances
        .as_ref()
        .ok_or("compute_semi_auto_best_possible requires live_instances")?;
    let preference_lists = scenario
        .preference_lists
        .as_ref()
        .ok_or("compute_semi_auto_best_possible requires preference_lists")?;

    let valid_partitions: BTreeSet<&str> = resource.partitions.iter().map(String::as_str).collect();
    let valid_instances: BTreeSet<&str> = instances.iter().map(|item| item.name.as_str()).collect();
    if valid_partitions.len() != resource.partitions.len() {
        return Err("resource.partitions contains a duplicate".into());
    }
    if valid_instances.len() != instances.len() {
        return Err("instances contains a duplicate".into());
    }

    let resource_id = ResourceId::try_from(resource.name.as_str())?;
    let mut ideal_builder = IdealState::builder(resource_id, replicas);
    for partition_name in &resource.partitions {
        let names = preference_lists
            .get(partition_name)
            .ok_or_else(|| format!("preference_lists is missing partition: {partition_name}"))?;
        let preference_list = names
            .iter()
            .map(|name| InstanceId::try_from(name.as_str()))
            .collect::<Result<Vec<_>, _>>()?;
        ideal_builder.set_preference_list(
            PartitionId::try_from(partition_name.as_str())?,
            preference_list,
        )?;
    }
    let ideal_state = ideal_builder.build()?;

    let mut current = CurrentState::builder();
    add_state_map(
        &mut current,
        current_map,
        &valid_partitions,
        &valid_instances,
        "current_state",
    )?;

    let declared_instances: BTreeSet<&str> = valid_instances;
    let live_instances = live_names
        .iter()
        .filter(|name| declared_instances.contains(name.as_str()))
        .map(|name| InstanceId::try_from(name.as_str()))
        .collect::<Result<BTreeSet<_>, _>>()?;

    let assignment = clustodian::rebalance::compute_semi_auto_best_possible_state(
        &ideal_state,
        &current.build(),
        &live_instances,
        &leader_standby(),
    )?;
    let mut best_possible_state = Map::new();
    for partition_name in &resource.partitions {
        let partition = PartitionId::try_from(partition_name.as_str())?;
        let mut states = Map::new();
        if let Some(instance_states) = assignment.entries().get(&partition) {
            for (instance, state) in instance_states {
                states.insert(
                    instance.as_str().to_owned(),
                    Value::String(state.as_str().to_owned()),
                );
            }
        }
        best_possible_state.insert(partition_name.clone(), Value::Object(states));
    }

    Ok(json!({
        "implementation": "clustodian",
        "operation": "compute_semi_auto_best_possible",
        "best_possible_state": best_possible_state,
    }))
}

fn select_transition_result(scenario: &Scenario) -> Result<Value, Box<dyn std::error::Error>> {
    let resource = detailed_resource(scenario, "select_transitions")?;
    let instances = scenario.structured_instances("select_transitions")?;
    let current_map = scenario
        .current_state
        .as_ref()
        .ok_or("select_transitions requires current_state")?;
    let replicas = scenario
        .replicas
        .ok_or("select_transitions requires replicas")?;
    let live_names = scenario
        .live_instances
        .as_ref()
        .ok_or("select_transitions requires live_instances")?;
    let preference_lists = scenario
        .preference_lists
        .as_ref()
        .ok_or("select_transitions requires preference_lists")?;
    let candidate_specs = scenario
        .candidate_transitions
        .as_ref()
        .ok_or("select_transitions requires candidate_transitions")?;
    let pending_specs = scenario
        .pending_transitions
        .as_ref()
        .ok_or("select_transitions requires pending_transitions")?;

    let valid_partitions: BTreeSet<&str> = resource.partitions.iter().map(String::as_str).collect();
    let valid_instances: BTreeSet<&str> = instances.iter().map(|item| item.name.as_str()).collect();
    if valid_partitions.len() != resource.partitions.len() {
        return Err("resource.partitions contains a duplicate".into());
    }
    if valid_instances.len() != instances.len() {
        return Err("instances contains a duplicate".into());
    }
    if live_names
        .iter()
        .any(|name| !valid_instances.contains(name.as_str()))
    {
        return Err("live_instances contains an unknown instance".into());
    }
    let live_instances = live_names
        .iter()
        .map(|name| InstanceId::try_from(name.as_str()))
        .collect::<Result<BTreeSet<_>, _>>()?;

    let resource_id = ResourceId::try_from(resource.name.as_str())?;
    let mut ideal_builder = IdealState::builder(resource_id.clone(), replicas);
    for partition_name in &resource.partitions {
        let names = preference_lists
            .get(partition_name)
            .ok_or_else(|| format!("preference_lists is missing partition: {partition_name}"))?;
        let preference_list = names
            .iter()
            .map(|name| {
                if !valid_instances.contains(name.as_str()) {
                    return Err(format!("preference list contains unknown instance: {name}").into());
                }
                InstanceId::try_from(name.as_str()).map_err(Into::into)
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        ideal_builder.set_preference_list(
            PartitionId::try_from(partition_name.as_str())?,
            preference_list,
        )?;
    }
    let ideal_state = ideal_builder.build()?;

    let mut current = CurrentState::builder();
    add_state_map(
        &mut current,
        current_map,
        &valid_partitions,
        &valid_instances,
        "current_state",
    )?;

    let mut candidates = Vec::with_capacity(candidate_specs.len());
    for transition in candidate_specs {
        validate_transition_spec(
            transition,
            &valid_partitions,
            &valid_instances,
            "candidate_transitions",
        )?;
        candidates.push(TransitionRequest::new(
            resource_id.clone(),
            PartitionId::try_from(transition.partition.as_str())?,
            InstanceId::try_from(transition.instance.as_str())?,
            State::try_from(transition.from.as_str())?,
            State::try_from(transition.to.as_str())?,
        ));
    }
    let mut pending = Vec::with_capacity(pending_specs.len());
    for transition in pending_specs {
        validate_transition_spec(
            transition,
            &valid_partitions,
            &valid_instances,
            "pending_transitions",
        )?;
        pending.push(PendingTransition::new(
            PartitionId::try_from(transition.partition.as_str())?,
            InstanceId::try_from(transition.instance.as_str())?,
            State::try_from(transition.from.as_str())?,
            State::try_from(transition.to.as_str())?,
        ));
    }

    let selected = select_transitions(
        &resource_id,
        &ideal_state,
        &current.build(),
        &live_instances,
        &candidates,
        &pending,
        &leader_standby(),
    )?;
    let selected_transitions = selected
        .iter()
        .map(|request| {
            json!({
                "resource": request.resource().as_str(),
                "partition": request.partition().as_str(),
                "instance": request.instance().as_str(),
                "from": request.source_state().as_str(),
                "to": request.target_state().as_str(),
                "message_type": "STATE_TRANSITION",
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "implementation": "clustodian",
        "operation": "select_transitions",
        "selected_transitions": selected_transitions,
    }))
}

fn compute_intermediate_and_throttle_result(
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let resource_specs = match scenario.resources.as_ref() {
        Some(ResourceCollection::M6(resources)) => resources,
        Some(ResourceCollection::M7(_)) | Some(ResourceCollection::M8(_)) => {
            return Err("compute_intermediate_and_throttle requires M6 resources".into())
        }
        None => return Err("compute_intermediate_and_throttle requires resources".into()),
    };
    let live_names = scenario
        .live_instances
        .as_ref()
        .ok_or("compute_intermediate_and_throttle requires live_instances")?;

    let mut all_instances = live_names.iter().cloned().collect::<BTreeSet<_>>();
    for resource in resource_specs {
        for names in resource.preference_lists.values() {
            all_instances.extend(names.iter().cloned());
        }
        collect_state_map_instances(&resource.current_state, &mut all_instances);
        collect_state_map_instances(&resource.best_possible_state, &mut all_instances);
        for transition in &resource.selected_transitions {
            all_instances.insert(transition.instance.clone());
        }
    }
    for transition in scenario.pending_transitions.as_deref().unwrap_or(&[]) {
        all_instances.insert(transition.instance.clone());
    }

    let live_instances = live_names
        .iter()
        .map(|name| InstanceId::try_from(name.as_str()))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let model = leader_standby();
    let mut resources = Vec::with_capacity(resource_specs.len());
    for resource in resource_specs {
        let resource_id = ResourceId::try_from(resource.name.as_str())?;
        let valid_partitions = resource
            .preference_lists
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        if valid_partitions.is_empty() {
            return Err(format!("resource {} has no preference lists", resource.name).into());
        }
        let valid_instances = all_instances
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let valid_partition_names = valid_partitions
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut current = CurrentState::builder();
        add_state_map(
            &mut current,
            &resource.current_state,
            &valid_partition_names,
            &valid_instances,
            "current_state",
        )?;
        let mut best = BestPossibleState::builder();
        add_state_map(
            &mut best,
            &resource.best_possible_state,
            &valid_partition_names,
            &valid_instances,
            "best_possible_state",
        )?;
        let mut preference_lists = BTreeMap::new();
        for (partition_name, names) in &resource.preference_lists {
            let partition = PartitionId::try_from(partition_name.as_str())?;
            let instances = names
                .iter()
                .map(|name| InstanceId::try_from(name.as_str()))
                .collect::<Result<Vec<_>, _>>()?;
            preference_lists.insert(partition, instances);
        }
        let min_active_replicas = match resource.min_active_replicas {
            -1 => None,
            value if value >= 0 => Some(value as usize),
            _ => return Err("min_active_replicas must be -1 or non-negative".into()),
        };
        let mut selected = Vec::with_capacity(resource.selected_transitions.len());
        for transition in &resource.selected_transitions {
            if transition.resource != resource.name {
                return Err(format!(
                    "selected transition resource mismatch: {}",
                    transition.resource
                )
                .into());
            }
            if !valid_partitions.contains(&transition.partition) {
                return Err(format!(
                    "selected_transitions contains unknown partition: {}",
                    transition.partition
                )
                .into());
            }
            selected.push(TransitionRequest::new(
                resource_id.clone(),
                PartitionId::try_from(transition.partition.as_str())?,
                InstanceId::try_from(transition.instance.as_str())?,
                State::try_from(transition.from.as_str())?,
                State::try_from(transition.to.as_str())?,
            ));
        }
        resources.push(ResourceTransitionInput::new(
            resource_id,
            resource.replicas,
            min_active_replicas,
            preference_lists,
            current.build(),
            best.build(),
            selected,
        ));
    }

    let mut pending = Vec::new();
    for transition in scenario.pending_transitions.as_deref().unwrap_or(&[]) {
        let resource = transition
            .resource
            .as_deref()
            .ok_or("M6 pending transition requires resource")?;
        pending.push(OperationalPendingTransition::new(
            ResourceId::try_from(resource)?,
            PartitionId::try_from(transition.partition.as_str())?,
            InstanceId::try_from(transition.instance.as_str())?,
            State::try_from(transition.from.as_str())?,
            State::try_from(transition.to.as_str())?,
        ));
    }
    let configs = scenario
        .throttle_configs
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .map(parse_throttle_config)
        .collect::<Result<Vec<_>, _>>()?;
    let result =
        compute_intermediate_and_throttle(&resources, &live_instances, &pending, &configs, &model)?;

    let mut intermediate = Map::new();
    for (resource, partitions) in result.intermediate_state().entries() {
        let mut partition_values = Map::new();
        for (partition, instances) in partitions {
            let mut instance_values = Map::new();
            for (instance, state) in instances {
                instance_values.insert(
                    instance.as_str().to_owned(),
                    Value::String(state.as_str().to_owned()),
                );
            }
            partition_values.insert(
                partition.as_str().to_owned(),
                Value::Object(instance_values),
            );
        }
        intermediate.insert(
            resource.as_str().to_owned(),
            Value::Object(partition_values),
        );
    }
    let dispatchable_transitions = result
        .dispatchable_transitions()
        .iter()
        .map(|transition| {
            json!({
                "resource": transition.resource().as_str(),
                "partition": transition.partition().as_str(),
                "instance": transition.instance().as_str(),
                "from": transition.source_state().as_str(),
                "to": transition.target_state().as_str(),
                "message_type": "STATE_TRANSITION",
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "implementation": "clustodian",
        "operation": "compute_intermediate_and_throttle",
        "intermediate_state": intermediate,
        "dispatchable_transitions": dispatchable_transitions,
    }))
}

fn collect_state_map_instances(states: &StateMap, instances: &mut BTreeSet<String>) {
    for partition in states.values() {
        instances.extend(partition.keys().cloned());
    }
}

fn compute_external_view_and_routing_result(
    scenario: &Scenario,
) -> Result<Value, Box<dyn std::error::Error>> {
    let instance_names = scenario.named_instances()?;
    if instance_names.is_empty() {
        return Err("instances must not be empty".into());
    }
    let configured_instances = instance_names
        .iter()
        .map(|name| InstanceId::try_from(name.as_str()))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if configured_instances.len() != instance_names.len() {
        return Err("instances contains a duplicate".into());
    }

    let resource_specs = match scenario.resources.as_ref() {
        Some(ResourceCollection::M7(resources)) => resources,
        Some(ResourceCollection::M6(_)) | Some(ResourceCollection::M8(_)) => {
            return Err("compute_external_view_and_routing requires M7 resources".into())
        }
        None => return Err("compute_external_view_and_routing requires resources".into()),
    };
    if resource_specs.is_empty() {
        return Err("resources must not be empty".into());
    }

    let mut current_states = BTreeMap::new();
    for resource_spec in resource_specs {
        let resource = ResourceId::try_from(resource_spec.name.as_str())?;
        let mut current_state = CurrentState::builder();
        for (partition_name, instance_states) in &resource_spec.current_state {
            let partition = PartitionId::try_from(partition_name.as_str())?;
            for (instance_name, state_name) in instance_states {
                current_state.set_state(
                    partition.clone(),
                    InstanceId::try_from(instance_name.as_str())?,
                    State::try_from(state_name.as_str())?,
                )?;
            }
        }
        if current_states
            .insert(resource, current_state.build())
            .is_some()
        {
            return Err(format!("duplicate resource: {}", resource_spec.name).into());
        }
    }

    let external_view = clustodian::model::ExternalView::from_current_states(current_states);
    let mut external_view_value = Map::new();
    for (resource, partitions) in external_view.entries() {
        let mut partition_values = Map::new();
        for (partition, instances) in partitions {
            let mut instance_values = Map::new();
            for (instance, state) in instances {
                instance_values.insert(
                    instance.as_str().to_owned(),
                    Value::String(state.as_str().to_owned()),
                );
            }
            partition_values.insert(
                partition.as_str().to_owned(),
                Value::Object(instance_values),
            );
        }
        external_view_value.insert(
            resource.as_str().to_owned(),
            Value::Object(partition_values),
        );
    }

    let routing = clustodian::routing::RoutingSnapshot::from_external_view(
        external_view,
        configured_instances,
    );
    let queries = scenario
        .routing_queries
        .as_ref()
        .ok_or("compute_external_view_and_routing requires routing_queries")?;
    let mut routing_results = Vec::with_capacity(queries.len());
    for query in queries {
        let resource = ResourceId::try_from(query.resource.as_str())?;
        let partition = PartitionId::try_from(query.partition.as_str())?;
        let state = State::try_from(query.state.as_str())?;
        let instances = routing
            .instances_for(&resource, &partition, &state)
            .into_iter()
            .map(|instance| Value::String(instance.as_str().to_owned()))
            .collect::<Vec<_>>();
        routing_results.push(json!({
            "id": query.id,
            "instances": instances,
        }));
    }

    Ok(json!({
        "implementation": "clustodian",
        "operation": "compute_external_view_and_routing",
        "external_view": external_view_value,
        "routing_results": routing_results,
    }))
}

fn parse_throttle_config(
    config: &ThrottleConfigSpec,
) -> Result<StateTransitionThrottleConfig, Box<dyn std::error::Error>> {
    let rebalance_type = match config.rebalance_type.as_str() {
        "ANY" => RebalanceType::Any,
        "RECOVERY_BALANCE" => RebalanceType::RecoveryBalance,
        "LOAD_BALANCE" => RebalanceType::LoadBalance,
        value => return Err(format!("unsupported rebalance_type: {value}").into()),
    };
    let scope = match config.scope.as_str() {
        "CLUSTER" => ThrottleScope::Cluster,
        "RESOURCE" => ThrottleScope::Resource,
        "INSTANCE" => ThrottleScope::Instance,
        value => return Err(format!("unsupported throttle scope: {value}").into()),
    };
    Ok(StateTransitionThrottleConfig::new(
        rebalance_type,
        scope,
        config.max_transitions,
    ))
}

fn validate_transition_spec(
    transition: &TransitionSpec,
    valid_partitions: &BTreeSet<&str>,
    valid_instances: &BTreeSet<&str>,
    field: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if !valid_partitions.contains(transition.partition.as_str()) {
        return Err(format!(
            "{field} contains unknown partition: {}",
            transition.partition
        )
        .into());
    }
    if !valid_instances.contains(transition.instance.as_str()) {
        return Err(format!("{field} contains unknown instance: {}", transition.instance).into());
    }
    Ok(())
}

fn compute_crush_result(scenario: &Scenario) -> Result<Value, Box<dyn std::error::Error>> {
    let resource = scenario
        .resource
        .as_ref()
        .ok_or("compute_crush_assignment requires resource")?;
    let ResourceInput::Name(resource_name) = resource else {
        return Err("compute_crush_assignment requires resource to be a string".into());
    };
    let partitions = scenario
        .partitions
        .as_ref()
        .ok_or("compute_crush_assignment requires partitions")?;
    let topology = scenario
        .topology
        .as_ref()
        .ok_or("compute_crush_assignment requires topology")?;
    let max_partitions_per_instance = scenario
        .max_partitions_per_instance
        .ok_or("compute_crush_assignment requires max_partitions_per_instance")?;
    if max_partitions_per_instance != -1 {
        return Err("M4 supports only max_partitions_per_instance = -1".into());
    }
    let state_counts = scenario
        .state_counts
        .as_ref()
        .ok_or("compute_crush_assignment requires state_counts")?;
    let replica_count = state_counts.iter().try_fold(0_usize, |total, state| {
        if state.count < 0 {
            return Err(format!("state count must not be negative: {}", state.state));
        }
        total
            .checked_add(state.count as usize)
            .ok_or_else(|| "state counts exceed usize".to_owned())
    })?;
    let instances = scenario.structured_instances("compute_crush_assignment")?;
    let crush_instances = instances
        .iter()
        .map(|instance| {
            let domain = instance
                .domain
                .as_ref()
                .ok_or_else(|| format!("instance {} requires domain", instance.name))?;
            let fault_zone = domain.get(&topology.fault_zone_type).ok_or_else(|| {
                format!(
                    "instance {} domain is missing {}",
                    instance.name, topology.fault_zone_type
                )
            })?;
            Ok(CrushInstance::new(
                InstanceId::try_from(instance.name.as_str())?,
                fault_zone.clone(),
            )?)
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let live_instances = scenario
        .live_instances
        .as_ref()
        .ok_or("compute_crush_assignment requires live_instances")?
        .iter()
        .map(|name| InstanceId::try_from(name.as_str()))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let partition_ids = partitions
        .iter()
        .map(|name| PartitionId::try_from(name.as_str()))
        .collect::<Result<Vec<_>, _>>()?;
    let assignment = compute_crush_assignment(
        &ResourceId::try_from(resource_name.as_str())?,
        &partition_ids,
        replica_count,
        &crush_instances,
        &live_instances,
        &CrushTopology::new(
            &topology.path,
            &topology.fault_zone_type,
            &topology.end_node_type,
        )?,
    )?;
    let mut preference_lists = Map::new();
    for partition in &partition_ids {
        let instances = assignment
            .get(partition)
            .ok_or_else(|| format!("missing assignment for partition {partition}"))?;
        preference_lists.insert(
            partition.as_str().to_owned(),
            Value::Array(
                instances
                    .iter()
                    .map(|instance| Value::String(instance.as_str().to_owned()))
                    .collect(),
            ),
        );
    }
    Ok(json!({
        "implementation": "clustodian",
        "operation": "compute_crush_assignment",
        "preference_lists": preference_lists,
    }))
}

fn detailed_resource<'a>(
    scenario: &'a Scenario,
    operation: &str,
) -> Result<&'a ResourceSpec, Box<dyn std::error::Error>> {
    let resource = scenario
        .resource
        .as_ref()
        .ok_or_else(|| format!("{operation} requires resource"))?;
    match resource {
        ResourceInput::Detailed(resource) => Ok(resource),
        ResourceInput::Name(_) => Err(format!("{operation} requires object resource").into()),
    }
}

fn add_state_map(
    builder: &mut impl StateMapBuilder,
    states: &StateMap,
    valid_partitions: &BTreeSet<&str>,
    valid_instances: &BTreeSet<&str>,
    field: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for (partition_name, instance_states) in states {
        if !valid_partitions.contains(partition_name.as_str()) {
            return Err(format!("{field} contains unknown partition: {partition_name}").into());
        }
        let partition = PartitionId::try_from(partition_name.as_str())?;
        for (instance_name, state_name) in instance_states {
            if !valid_instances.contains(instance_name.as_str()) {
                return Err(format!("{field} contains unknown instance: {instance_name}").into());
            }
            builder.set_state(
                partition.clone(),
                InstanceId::try_from(instance_name.as_str())?,
                State::try_from(state_name.as_str())?,
            )?;
        }
    }
    Ok(())
}

trait StateMapBuilder {
    fn set_state(
        &mut self,
        partition: PartitionId,
        instance: InstanceId,
        state: State,
    ) -> Result<(), clustodian::model::ReplicaStateError>;
}

impl StateMapBuilder for clustodian::model::CurrentStateBuilder {
    fn set_state(
        &mut self,
        partition: PartitionId,
        instance: InstanceId,
        state: State,
    ) -> Result<(), clustodian::model::ReplicaStateError> {
        clustodian::model::CurrentStateBuilder::set_state(self, partition, instance, state)
            .map(|_| ())
    }
}

impl StateMapBuilder for clustodian::model::BestPossibleStateBuilder {
    fn set_state(
        &mut self,
        partition: PartitionId,
        instance: InstanceId,
        state: State,
    ) -> Result<(), clustodian::model::ReplicaStateError> {
        clustodian::model::BestPossibleStateBuilder::set_state(self, partition, instance, state)
            .map(|_| ())
    }
}

fn cardinality_string(cardinality: StateCardinality) -> String {
    match cardinality {
        StateCardinality::Exact(value) => value.to_string(),
        StateCardinality::ReplicaCount => String::from("R"),
        StateCardinality::NodeCount => String::from("N"),
        StateCardinality::Unbounded => String::from("-1"),
    }
}
