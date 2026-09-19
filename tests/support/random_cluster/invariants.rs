use super::model::{ClusterModel, ParticipantLifecycle};
use clustodian::model::{InstanceId, PartitionId};
use clustodian::observe::ClusterSnapshot;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LogicalTransition {
    resource: String,
    partition: String,
    instance: String,
    target_session: u64,
    from: String,
    to: String,
    message_type: String,
}

#[derive(Default)]
pub struct MessageHistory {
    pending: BTreeMap<LogicalTransition, String>,
    retired: BTreeMap<LogicalTransition, BTreeSet<String>>,
    controller_failover_pending: bool,
}

impl MessageHistory {
    pub fn note_controller_failover(&mut self) {
        self.controller_failover_pending = true;
    }
}

pub fn check_safety(
    model: &ClusterModel,
    snapshot: &ClusterSnapshot,
    history: &mut MessageHistory,
) -> Result<(), String> {
    if snapshot.controllers.active.len() > 1 {
        return Err(format!(
            "more than one active controller: {:?}",
            snapshot.controllers.active
        ));
    }

    for (instance, current) in &snapshot.active_current_state {
        let Some(live_session) = snapshot.live_instances.get(instance) else {
            return Err(format!(
                "active CurrentState exists for non-live instance {instance}"
            ));
        };
        if current.session != *live_session {
            return Err(format!(
                "active CurrentState session mismatch for {instance}: state={}, live={live_session}",
                current.session
            ));
        }
        for (resource, partitions) in &current.resources {
            let Some(resource_model) = model.resources.get(resource) else {
                return Err(format!(
                    "active replica belongs to unknown resource {resource}"
                ));
            };
            if !resource_model.eligible_instances.contains(instance) {
                return Err(format!(
                    "active replica {instance} is not configured for resource {resource}"
                ));
            }
            for partition in partitions.keys() {
                if !partition_name_is_valid(resource_model, partition) {
                    return Err(format!(
                        "active replica uses unknown partition {resource}/{partition}"
                    ));
                }
            }
        }
    }

    for message in &snapshot.pending_transitions {
        let Some(live_session) = snapshot.live_instances.get(&message.instance) else {
            return Err(format!(
                "pending transition targets non-live instance {}",
                message.instance
            ));
        };
        if message.target_session != *live_session {
            return Err(format!(
                "pending transition targets stale session for {}: target={}, live={live_session}",
                message.instance, message.target_session
            ));
        }
        if message.message_id.is_empty() {
            return Err(String::from("pending transition has an empty message_id"));
        }
    }
    let message_ids = snapshot
        .pending_transitions
        .iter()
        .map(|message| message.message_id.as_str())
        .collect::<BTreeSet<_>>();
    if message_ids.len() != snapshot.pending_transitions.len() {
        return Err(String::from(
            "pending transition message IDs are not unique",
        ));
    }
    check_message_history(snapshot, history)?;

    let mut leaders = BTreeMap::<(String, String), usize>::new();
    for current in snapshot.active_current_state.values() {
        for (resource, partitions) in &current.resources {
            for (partition, state) in partitions {
                if state == "LEADER" {
                    *leaders
                        .entry((resource.clone(), partition.clone()))
                        .or_default() += 1;
                }
            }
        }
    }
    for ((resource, partition), count) in leaders {
        if count > 1 {
            return Err(format!(
                "leader safety violation for {resource}/{partition}: {count} leaders"
            ));
        }
    }

    for (instance, participant) in &model.participants {
        if matches!(participant.lifecycle, ParticipantLifecycle::Stopped)
            && snapshot.live_instances.contains_key(instance)
        {
            return Err(format!(
                "stopped participant {instance} became live without a start action"
            ));
        }
    }
    Ok(())
}

pub fn check_convergence(model: &ClusterModel, snapshot: &ClusterSnapshot) -> Result<(), String> {
    if model
        .controllers
        .values()
        .any(|lifecycle| matches!(lifecycle, super::model::ControllerLifecycle::Running))
        && snapshot.controllers.active.len() != 1
    {
        return Err(format!(
            "expected one active controller, observed {:?}",
            snapshot.controllers.active
        ));
    }

    if model.paused_handlers.is_empty() && !snapshot.pending_transitions.is_empty() {
        return Err(format!(
            "{} pending transitions remain",
            snapshot.pending_transitions.len()
        ));
    }

    let expected_external = aggregate_active_current_state(snapshot);
    if snapshot.external_view != expected_external {
        return Err(format!(
            "ExternalView differs from active CurrentState: expected={expected_external}, observed={}",
            snapshot.external_view
        ));
    }

    check_routing(snapshot)?;

    if model.paused_handlers.is_empty() {
        check_replica_cardinality(model, snapshot)?;
        check_best_possible_state(model, snapshot)?;
    }
    Ok(())
}

fn check_best_possible_state(
    model: &ClusterModel,
    snapshot: &ClusterSnapshot,
) -> Result<(), String> {
    let live = snapshot
        .live_instances
        .keys()
        .map(|instance| {
            clustodian::model::InstanceId::new(instance.clone())
                .map_err(|error| format!("invalid live instance {instance}: {error}"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let instances = model
        .participants
        .iter()
        .filter(|(_, participant)| participant.configured)
        .map(|(instance, participant)| {
            clustodian::rebalance::CrushInstance::new(
                clustodian::model::InstanceId::new(instance.clone())
                    .map_err(|error| format!("invalid instance {instance}: {error}"))?,
                participant.zone.clone(),
            )
            .map_err(|error| format!("CRUSH instance {instance}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let topology = clustodian::rebalance::CrushTopology::new("/instance", "instance", "instance")
        .map_err(|error| format!("CRUSH topology: {error}"))?;
    let state_model = clustodian::model::leader_standby();
    let mut expected = BTreeMap::<String, BTreeMap<String, BTreeMap<String, String>>>::new();
    for resource in model.resources.values() {
        let resource_id = clustodian::model::ResourceId::new(resource.name.clone())
            .map_err(|error| format!("invalid resource {}: {error}", resource.name))?;
        let partitions = (0..resource.partition_count)
            .map(|index| {
                clustodian::model::PartitionId::new(format!("{}_{}", resource.name, index)).map_err(
                    |error| format!("invalid partition {}/{}: {error}", resource.name, index),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let preference_lists: BTreeMap<PartitionId, Vec<InstanceId>> = match &resource.placement {
            super::action::Placement::SemiAuto { preference_lists } => preference_lists
                .iter()
                .map(|(partition, instances)| {
                    Ok((
                        PartitionId::new(partition.clone())
                            .map_err(|error| format!("partition {partition}: {error}"))?,
                        instances
                            .iter()
                            .map(|instance| {
                                clustodian::model::InstanceId::new(instance.clone())
                                    .map_err(|error| format!("instance {instance}: {error}"))
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                })
                .collect::<Result<_, String>>()?,
            super::action::Placement::Crush => clustodian::rebalance::compute_crush_assignment(
                &resource_id,
                &partitions,
                resource.replica_count.min(live.len()),
                &instances,
                &live,
                &topology,
            )
            .map_err(|error| format!("CRUSH assignment for {}: {error}", resource.name))?,
            super::action::Placement::CrushWithTopology {
                path,
                fault_zone_type,
                end_node_type,
            } => {
                let topology =
                    clustodian::rebalance::CrushTopology::new(path, fault_zone_type, end_node_type)
                        .map_err(|error| {
                            format!("CRUSH topology for {}: {error}", resource.name)
                        })?;
                clustodian::rebalance::compute_crush_assignment(
                    &resource_id,
                    &partitions,
                    resource.replica_count.min(live.len()),
                    &instances,
                    &live,
                    &topology,
                )
                .map_err(|error| format!("CRUSH assignment for {}: {error}", resource.name))?
            }
        };
        let mut ideal_builder = clustodian::model::IdealState::builder(
            resource_id,
            resource.replica_count.min(live.len()),
        );
        for (partition, preference_list) in preference_lists {
            ideal_builder
                .set_preference_list(partition, preference_list)
                .map_err(|error| {
                    format!("BestPossibleState input for {}: {error}", resource.name)
                })?;
        }
        let ideal = ideal_builder
            .build()
            .map_err(|error| format!("BestPossibleState ideal for {}: {error}", resource.name))?;
        let best = clustodian::rebalance::compute_semi_auto_best_possible_state(
            &ideal,
            &clustodian::model::CurrentState::default(),
            &live,
            &state_model,
        )
        .map_err(|error| format!("BestPossibleState for {}: {error}", resource.name))?;
        for (partition, states) in best.entries() {
            let output = expected.entry(resource.name.clone()).or_default();
            let partition_output = output.entry(partition.to_string()).or_default();
            for (instance, state) in states {
                partition_output.insert(instance.to_string(), state.to_string());
            }
        }
    }
    let expected = serde_json::to_value(expected)
        .map_err(|error| format!("serialize BestPossibleState: {error}"))?;
    if snapshot.external_view != expected {
        return Err(format!(
            "ExternalView differs from configuration+liveness BestPossibleState: expected={expected}, observed={}",
            snapshot.external_view
        ));
    }
    Ok(())
}

fn check_message_history(
    snapshot: &ClusterSnapshot,
    history: &mut MessageHistory,
) -> Result<(), String> {
    let current = snapshot
        .pending_transitions
        .iter()
        .map(|message| {
            (
                LogicalTransition {
                    resource: message.resource.clone(),
                    partition: message.partition.clone(),
                    instance: message.instance.clone(),
                    target_session: message.target_session,
                    from: message.from.clone(),
                    to: message.to.clone(),
                    message_type: message.message_type.clone(),
                },
                message.message_id.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    for (logical, previous_id) in &history.pending {
        if let Some(current_id) = current.get(logical) {
            if current_id != previous_id && history.controller_failover_pending {
                return Err(format!(
                    "pending transition identity changed during reconciliation: {logical:?}: {previous_id} -> {current_id}"
                ));
            }
        } else {
            history
                .retired
                .entry(logical.clone())
                .or_default()
                .insert(previous_id.clone());
        }
    }
    for (logical, current_id) in &current {
        if history
            .retired
            .get(logical)
            .is_some_and(|ids| ids.contains(current_id))
        {
            return Err(format!(
                "new transition attempt reused retired message identity {logical:?}: {current_id}"
            ));
        }
    }
    history.pending = current;
    history.controller_failover_pending = false;
    Ok(())
}

fn aggregate_active_current_state(snapshot: &ClusterSnapshot) -> Value {
    let mut aggregate = BTreeMap::<String, BTreeMap<String, BTreeMap<String, String>>>::new();
    for (instance, current) in &snapshot.active_current_state {
        for (resource, partitions) in &current.resources {
            for (partition, state) in partitions {
                aggregate
                    .entry(resource.clone())
                    .or_default()
                    .entry(partition.clone())
                    .or_default()
                    .insert(instance.clone(), state.clone());
            }
        }
    }
    serde_json::to_value(aggregate).expect("BTreeMap state aggregation is serializable")
}

fn check_routing(snapshot: &ClusterSnapshot) -> Result<(), String> {
    let mut expected = BTreeMap::<(String, String, String), Vec<String>>::new();
    let external: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>> =
        serde_json::from_value(snapshot.external_view.clone())
            .map_err(|error| format!("ExternalView is not a routing map: {error}"))?;
    for (resource, partitions) in external {
        for (partition, instances) in partitions {
            let mut states = BTreeMap::<String, Vec<String>>::new();
            for (instance, state) in instances {
                states.entry(state).or_default().push(instance);
            }
            for (state, mut instances) in states {
                instances.sort();
                expected.insert((resource.clone(), partition.clone(), state), instances);
            }
        }
    }

    let observed = snapshot
        .routing_results
        .iter()
        .map(|result| {
            (
                (
                    result.resource.clone(),
                    result.partition.clone(),
                    result.state.clone(),
                ),
                result.instances.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if observed != expected {
        return Err(format!(
            "routing does not agree with ExternalView: expected={expected:?}, observed={observed:?}"
        ));
    }
    Ok(())
}

fn check_replica_cardinality(
    model: &ClusterModel,
    snapshot: &ClusterSnapshot,
) -> Result<(), String> {
    let external: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>> =
        serde_json::from_value(snapshot.external_view.clone())
            .map_err(|error| format!("ExternalView is not a replica map: {error}"))?;
    for resource in model.resources.values() {
        for index in 0..resource.partition_count {
            let partition = format!("{}_{}", resource.name, index);
            let eligible_live = resource
                .eligible_instances
                .iter()
                .filter(|instance| snapshot.live_instances.contains_key(*instance))
                .count();
            let expected_replicas = match &resource.placement {
                super::action::Placement::SemiAuto { preference_lists } => preference_lists
                    .get(&partition)
                    .map(|instances| {
                        instances
                            .iter()
                            .filter(|instance| snapshot.live_instances.contains_key(*instance))
                            .count()
                    })
                    .unwrap_or_default(),
                super::action::Placement::Crush
                | super::action::Placement::CrushWithTopology { .. } => {
                    resource.replica_count.min(eligible_live)
                }
            };
            let Some(instances) = external
                .get(&resource.name)
                .and_then(|partitions| partitions.get(&partition))
            else {
                if expected_replicas == 0 {
                    continue;
                }
                return Err(format!(
                    "missing settled partition {}/{}",
                    resource.name, partition
                ));
            };
            if instances.len() != expected_replicas {
                return Err(format!(
                    "wrong settled replica count for {}/{}: expected {}, observed {}",
                    resource.name,
                    partition,
                    expected_replicas,
                    instances.len()
                ));
            }
            let leader_count = instances
                .values()
                .filter(|state| state.as_str() == "LEADER")
                .count();
            if expected_replicas > 0 && leader_count != 1 {
                return Err(format!(
                    "wrong settled leader count for {}/{}: observed {}",
                    resource.name, partition, leader_count
                ));
            }
        }
    }
    Ok(())
}

fn partition_name_is_valid(resource: &super::model::ResourceModel, partition: &str) -> bool {
    (0..resource.partition_count).any(|index| partition == format!("{}_{}", resource.name, index))
}
