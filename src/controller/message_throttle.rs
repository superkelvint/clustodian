use super::intermediate_state::IntermediateState;
use crate::model::{
    BestPossibleState, CurrentState, InstanceId, PartitionId, ResourceId, State, StateCardinality,
    StateModelDefinition,
};
use crate::transition::TransitionRequest;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// The operational class used by Helix state-transition throttles.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RebalanceType {
    Any,
    RecoveryBalance,
    LoadBalance,
}

/// The scope at which a state-transition quota applies.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ThrottleScope {
    Cluster,
    Resource,
    Instance,
}

/// One supported Helix state-transition throttle configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateTransitionThrottleConfig {
    rebalance_type: RebalanceType,
    scope: ThrottleScope,
    max_transitions: usize,
    target: Option<ThrottleTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ThrottleTarget {
    Resource(ResourceId),
    Instance(InstanceId),
}

impl StateTransitionThrottleConfig {
    /// Construct a state-transition quota.
    pub fn new(
        rebalance_type: RebalanceType,
        scope: ThrottleScope,
        max_transitions: usize,
    ) -> Self {
        Self {
            rebalance_type,
            scope,
            max_transitions,
            target: None,
        }
    }

    /// Construct a state-transition quota for one resource.
    pub fn for_resource(
        rebalance_type: RebalanceType,
        resource: ResourceId,
        max_transitions: usize,
    ) -> Self {
        Self {
            rebalance_type,
            scope: ThrottleScope::Resource,
            max_transitions,
            target: Some(ThrottleTarget::Resource(resource)),
        }
    }

    /// Construct a state-transition quota for one instance.
    pub fn for_instance(
        rebalance_type: RebalanceType,
        instance: InstanceId,
        max_transitions: usize,
    ) -> Self {
        Self {
            rebalance_type,
            scope: ThrottleScope::Instance,
            max_transitions,
            target: Some(ThrottleTarget::Instance(instance)),
        }
    }

    /// Return the kind of work counted by this quota.
    pub fn rebalance_type(&self) -> RebalanceType {
        self.rebalance_type
    }

    /// Return the scope at which this quota applies.
    pub fn scope(&self) -> ThrottleScope {
        self.scope
    }

    /// Return the maximum number of in-flight transitions.
    pub fn max_transitions(&self) -> usize {
        self.max_transitions
    }

    pub(crate) fn target(&self) -> Option<&ThrottleTarget> {
        self.target.as_ref()
    }
}

/// A selected transition already in flight before this controller cycle.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationalPendingTransition {
    resource: ResourceId,
    partition: PartitionId,
    instance: InstanceId,
    source_state: State,
    target_state: State,
}

impl OperationalPendingTransition {
    /// Construct an in-flight transition with its resource identity.
    pub fn new(
        resource: ResourceId,
        partition: PartitionId,
        instance: InstanceId,
        source_state: State,
        target_state: State,
    ) -> Self {
        Self {
            resource,
            partition,
            instance,
            source_state,
            target_state,
        }
    }

    /// Return the resource identity.
    pub fn resource(&self) -> &ResourceId {
        &self.resource
    }

    /// Return the partition identity.
    pub fn partition(&self) -> &PartitionId {
        &self.partition
    }

    /// Return the target instance identity.
    pub fn instance(&self) -> &InstanceId {
        &self.instance
    }

    /// Return the state before this transition.
    pub fn source_state(&self) -> &State {
        &self.source_state
    }

    /// Return the state after this transition.
    pub fn target_state(&self) -> &State {
        &self.target_state
    }
}

/// All per-resource inputs consumed by M6.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceTransitionInput {
    resource: ResourceId,
    replicas: usize,
    min_active_replicas: Option<usize>,
    preference_lists: BTreeMap<PartitionId, Vec<InstanceId>>,
    current_state: CurrentState,
    best_possible_state: BestPossibleState,
    selected_transitions: Vec<TransitionRequest>,
}

impl ResourceTransitionInput {
    /// Construct the complete input for one resource's controller stages.
    pub fn new(
        resource: ResourceId,
        replicas: usize,
        min_active_replicas: Option<usize>,
        preference_lists: BTreeMap<PartitionId, Vec<InstanceId>>,
        current_state: CurrentState,
        best_possible_state: BestPossibleState,
        selected_transitions: Vec<TransitionRequest>,
    ) -> Self {
        Self {
            resource,
            replicas,
            min_active_replicas,
            preference_lists,
            current_state,
            best_possible_state,
            selected_transitions,
        }
    }
}

/// Result of intermediate-state calculation and operational throttling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntermediateThrottleResult {
    intermediate_state: IntermediateState,
    dispatchable_transitions: Vec<TransitionRequest>,
}

impl IntermediateThrottleResult {
    pub(crate) fn new(
        intermediate_state: IntermediateState,
        dispatchable_transitions: Vec<TransitionRequest>,
    ) -> Self {
        Self {
            intermediate_state,
            dispatchable_transitions,
        }
    }

    pub fn intermediate_state(&self) -> &IntermediateState {
        &self.intermediate_state
    }

    pub fn dispatchable_transitions(&self) -> &[TransitionRequest] {
        &self.dispatchable_transitions
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IntermediateThrottleError {
    DuplicateResource(ResourceId),
    DuplicatePendingTransition {
        partition: PartitionId,
        instance: InstanceId,
    },
    ResourceMismatch {
        expected: ResourceId,
        actual: ResourceId,
    },
    UnknownPendingResource(ResourceId),
    UnknownSelectedPartition {
        resource: ResourceId,
        partition: PartitionId,
    },
}

impl fmt::Display for IntermediateThrottleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateResource(resource) => write!(formatter, "duplicate resource {resource}"),
            Self::DuplicatePendingTransition {
                partition,
                instance,
            } => write!(
                formatter,
                "duplicate pending transition for partition {partition} and instance {instance}"
            ),
            Self::ResourceMismatch { expected, actual } => write!(
                formatter,
                "selected transition belongs to resource {actual}, expected {expected}"
            ),
            Self::UnknownPendingResource(resource) => {
                write!(formatter, "pending transition references unknown resource {resource}")
            }
            Self::UnknownSelectedPartition { resource, partition } => write!(
                formatter,
                "selected transition references unknown partition {partition} in resource {resource}"
            ),
        }
    }
}

impl std::error::Error for IntermediateThrottleError {}

/// Compute the supported Helix intermediate state and state-transition quotas.
pub fn compute_intermediate_and_throttle(
    resources: &[ResourceTransitionInput],
    live_instances: &BTreeSet<InstanceId>,
    pending: &[OperationalPendingTransition],
    throttle_configs: &[StateTransitionThrottleConfig],
    state_model: &StateModelDefinition,
) -> Result<IntermediateThrottleResult, IntermediateThrottleError> {
    let mut resource_indexes = BTreeMap::new();
    for (index, resource) in resources.iter().enumerate() {
        if resource_indexes
            .insert(resource.resource.clone(), index)
            .is_some()
        {
            return Err(IntermediateThrottleError::DuplicateResource(
                resource.resource.clone(),
            ));
        }
    }

    let mut pending_by_resource: BTreeMap<&ResourceId, Vec<&OperationalPendingTransition>> =
        BTreeMap::new();
    for transition in pending {
        if !resource_indexes.contains_key(transition.resource()) {
            return Err(IntermediateThrottleError::UnknownPendingResource(
                transition.resource().clone(),
            ));
        }
    }
    let mut pending_keys = BTreeSet::new();
    for transition in pending {
        if !pending_keys.insert((
            transition.resource().clone(),
            transition.partition().clone(),
            transition.instance().clone(),
        )) {
            return Err(IntermediateThrottleError::DuplicatePendingTransition {
                partition: transition.partition().clone(),
                instance: transition.instance().clone(),
            });
        }
        pending_by_resource
            .entry(transition.resource())
            .or_default()
            .push(transition);
    }

    let mut controller = ThrottleController::new(resources, live_instances, throttle_configs);
    let dropped_state = state_model
        .states_in_priority_order()
        .iter()
        .find(|state| state.as_str() == "DROPPED");
    for resource in resources {
        charge_pending(
            resource,
            pending_by_resource
                .get(&resource.resource)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            live_instances,
            state_model,
            &mut controller,
        );
    }

    let mut dispatchable = Vec::new();
    let mut intermediate = BTreeMap::new();
    for resource in resources {
        validate_selected(resource, state_model)?;
        let mut derived = resource.current_state.entries().clone();

        let mut selected_by_partition: BTreeMap<&PartitionId, Vec<&TransitionRequest>> =
            BTreeMap::new();
        for transition in &resource.selected_transitions {
            selected_by_partition
                .entry(transition.partition())
                .or_default()
                .push(transition);
        }

        // Helix returns the best-possible map unchanged when this resource has
        // no selected messages. The M6 scenarios use this path only when the
        // two maps already agree, but retaining the stage boundary matters.
        if resource.selected_transitions.is_empty() {
            intermediate.insert(
                resource.resource.clone(),
                resource.best_possible_state.entries().clone(),
            );
            continue;
        }

        let mut partition_order = selected_by_partition.keys().copied().collect::<Vec<_>>();
        partition_order.sort_by(|left, right| {
            partition_priority(resource, left, right, state_model.highest_priority_state())
        });
        for partition in partition_order {
            let Some(messages) = selected_by_partition.get(&partition) else {
                continue;
            };
            let mut messages = messages.clone();
            messages.sort_by(|left, right| message_priority(resource, left, right, state_model));
            let required = required_states(resource, partition, live_instances, state_model);
            for transition in messages {
                if pending_by_resource_matches(
                    pending_by_resource.get(&resource.resource),
                    transition,
                ) {
                    // A pending transition is already charged above. The
                    // selection stage may return it again because planning
                    // starts from the pre-transition CurrentState; charging
                    // it a second time would incorrectly consume dynamic
                    // throttle capacity and hide newly admissible work.
                    continue;
                }
                let rebalance_type = rebalance_type(&required, &derived, transition);
                if controller.should_throttle(
                    rebalance_type,
                    &resource.resource,
                    transition.instance(),
                ) {
                    continue;
                }
                controller.charge(rebalance_type, &resource.resource, transition.instance());
                derived
                    .entry(transition.partition().clone())
                    .or_default()
                    .insert(
                        transition.instance().clone(),
                        transition.target_state().clone(),
                    );
                dispatchable.push(transition.clone());
            }
        }

        apply_pending(
            &mut derived,
            pending_by_resource.get(&resource.resource),
            dropped_state,
        );
        for transition in resource
            .selected_transitions
            .iter()
            .filter(|transition| dispatchable.contains(transition))
        {
            if Some(transition.target_state()) == dropped_state {
                if let Some(partition) = derived.get_mut(transition.partition()) {
                    partition.remove(transition.instance());
                }
            } else {
                derived
                    .entry(transition.partition().clone())
                    .or_default()
                    .insert(
                        transition.instance().clone(),
                        transition.target_state().clone(),
                    );
            }
        }
        intermediate.insert(resource.resource.clone(), derived);
    }

    Ok(IntermediateThrottleResult::new(
        IntermediateState::from_states(intermediate),
        dispatchable,
    ))
}

fn pending_by_resource_matches(
    pending: Option<&Vec<&OperationalPendingTransition>>,
    transition: &TransitionRequest,
) -> bool {
    pending.is_some_and(|pending| {
        pending.iter().any(|candidate| {
            candidate.partition() == transition.partition()
                && candidate.instance() == transition.instance()
                && candidate.source_state() == transition.source_state()
                && candidate.target_state() == transition.target_state()
        })
    })
}

fn validate_selected(
    resource: &ResourceTransitionInput,
    state_model: &StateModelDefinition,
) -> Result<(), IntermediateThrottleError> {
    for transition in &resource.selected_transitions {
        if transition.resource() != &resource.resource {
            return Err(IntermediateThrottleError::ResourceMismatch {
                expected: resource.resource.clone(),
                actual: transition.resource().clone(),
            });
        }
        if !resource
            .preference_lists
            .contains_key(transition.partition())
            || (!resource
                .preference_lists
                .get(transition.partition())
                .is_some_and(|preference| !preference.is_empty())
                && !is_drop_transition(transition, resource, state_model))
        {
            return Err(IntermediateThrottleError::UnknownSelectedPartition {
                resource: resource.resource.clone(),
                partition: transition.partition().clone(),
            });
        }
    }
    Ok(())
}

fn is_drop_transition(
    transition: &TransitionRequest,
    resource: &ResourceTransitionInput,
    state_model: &StateModelDefinition,
) -> bool {
    let Some(dropped) = state_model
        .states_in_priority_order()
        .iter()
        .find(|state| state.as_str() == "DROPPED")
    else {
        return false;
    };
    resource
        .current_state
        .entries()
        .contains_key(transition.partition())
        && state_model.next_state_toward(transition.source_state(), dropped)
            == Some(transition.target_state())
}

fn required_states(
    resource: &ResourceTransitionInput,
    partition: &PartitionId,
    live_instances: &BTreeSet<InstanceId>,
    state_model: &StateModelDefinition,
) -> BTreeMap<State, usize> {
    let candidate_count = resource
        .preference_lists
        .get(partition)
        .map(|preference| {
            preference
                .iter()
                .filter(|instance| live_instances.contains(*instance))
                .count()
        })
        .unwrap_or(0);
    let mut remaining_candidates = candidate_count;
    let mut remaining_replicas = resource.min_active_replicas.unwrap_or(resource.replicas);
    let mut result = BTreeMap::new();
    for state in state_model.states_in_priority_order() {
        if remaining_candidates == 0 {
            break;
        }
        match state_model.cardinality_for(state) {
            Some(StateCardinality::Exact(count)) if count > 0 => {
                let count = (count as usize).min(remaining_candidates);
                remaining_candidates -= count;
                remaining_replicas = remaining_replicas.saturating_sub(count);
                result.insert(state.clone(), count);
            }
            Some(StateCardinality::NodeCount) => {
                result.insert(state.clone(), remaining_candidates);
                remaining_replicas = remaining_replicas.saturating_sub(remaining_candidates);
                remaining_candidates = 0;
            }
            Some(StateCardinality::ReplicaCount) | Some(StateCardinality::Unbounded) | None => {}
            Some(StateCardinality::Exact(_)) => {}
        }
    }
    for state in state_model.states_in_priority_order() {
        if state_model.cardinality_for(state) == Some(StateCardinality::ReplicaCount)
            && remaining_candidates > 0
            && remaining_replicas > 0
        {
            result.insert(state.clone(), remaining_candidates.min(remaining_replicas));
            break;
        }
    }
    result
}

fn rebalance_type(
    required: &BTreeMap<State, usize>,
    current: &BTreeMap<PartitionId, BTreeMap<InstanceId, State>>,
    transition: &TransitionRequest,
) -> RebalanceType {
    let mut remaining = required.clone();
    if let Some(states) = current.get(transition.partition()) {
        for state in states.values() {
            if let Some(count) = remaining.get_mut(state) {
                if *count == 1 {
                    remaining.remove(state);
                } else {
                    *count -= 1;
                }
            }
        }
    }
    if remaining.contains_key(transition.target_state()) {
        RebalanceType::RecoveryBalance
    } else {
        RebalanceType::LoadBalance
    }
}

fn partition_priority(
    resource: &ResourceTransitionInput,
    left: &PartitionId,
    right: &PartitionId,
    top_state: &State,
) -> std::cmp::Ordering {
    let left_current = resource.current_state.entries().get(left);
    let right_current = resource.current_state.entries().get(right);
    let left_best = resource.best_possible_state.entries().get(left);
    let right_best = resource.best_possible_state.entries().get(right);
    let key = |current: Option<&BTreeMap<InstanceId, State>>,
               best: Option<&BTreeMap<InstanceId, State>>| {
        let miss_top = usize::from(
            !current.is_some_and(|states| states.values().any(|state| state == top_state)),
        );
        let active = best
            .map(|best| {
                let mut remaining = BTreeMap::<&State, usize>::new();
                for state in best.values() {
                    *remaining.entry(state).or_default() += 1;
                }
                current
                    .into_iter()
                    .flat_map(|states| states.values())
                    .filter(|state| {
                        let Some(count) = remaining.get_mut(state) else {
                            return false;
                        };
                        if *count == 0 {
                            return false;
                        }
                        *count -= 1;
                        true
                    })
                    .count()
            })
            .unwrap_or(0);
        let matched = best
            .map(|best| {
                current
                    .into_iter()
                    .flat_map(|states| states.iter())
                    .filter(|(instance, state)| best.get(*instance) == Some(*state))
                    .count()
            })
            .unwrap_or(0);
        (miss_top, active, matched)
    };
    key(left_current, left_best)
        .cmp(&key(right_current, right_best))
        .then_with(|| left.cmp(right))
}

fn message_priority(
    resource: &ResourceTransitionInput,
    left: &TransitionRequest,
    right: &TransitionRequest,
    state_model: &StateModelDefinition,
) -> std::cmp::Ordering {
    let left_state = state_model
        .states_in_priority_order()
        .iter()
        .position(|state| state == left.target_state())
        .unwrap_or(usize::MAX);
    let right_state = state_model
        .states_in_priority_order()
        .iter()
        .position(|state| state == right.target_state())
        .unwrap_or(usize::MAX);
    if left.target_state() != right.target_state() {
        return left_state.cmp(&right_state);
    }
    let left_position = resource
        .preference_lists
        .get(left.partition())
        .and_then(|list| list.iter().position(|instance| instance == left.instance()));
    let right_position = resource
        .preference_lists
        .get(right.partition())
        .and_then(|list| {
            list.iter()
                .position(|instance| instance == right.instance())
        });
    left_position
        .cmp(&right_position)
        .then_with(|| left.instance().cmp(right.instance()))
}

fn apply_pending(
    states: &mut BTreeMap<PartitionId, BTreeMap<InstanceId, State>>,
    pending: Option<&Vec<&OperationalPendingTransition>>,
    dropped_state: Option<&State>,
) {
    if let Some(pending) = pending {
        for transition in pending {
            if Some(transition.target_state()) == dropped_state {
                if let Some(partition) = states.get_mut(transition.partition()) {
                    partition.remove(transition.instance());
                }
            } else {
                states
                    .entry(transition.partition().clone())
                    .or_default()
                    .insert(
                        transition.instance().clone(),
                        transition.target_state().clone(),
                    );
            }
        }
    }
}

#[derive(Default)]
struct ThrottleController {
    cluster: BTreeMap<RebalanceType, usize>,
    resources: BTreeMap<ResourceId, BTreeMap<RebalanceType, usize>>,
    instances: BTreeMap<InstanceId, BTreeMap<RebalanceType, usize>>,
}

impl ThrottleController {
    fn new(
        resources: &[ResourceTransitionInput],
        live_instances: &BTreeSet<InstanceId>,
        configs: &[StateTransitionThrottleConfig],
    ) -> Self {
        let mut controller = Self::default();
        for resource in resources {
            controller
                .resources
                .entry(resource.resource.clone())
                .or_default();
        }
        for instance in live_instances {
            controller.instances.entry(instance.clone()).or_default();
        }
        for config in configs {
            match config.scope {
                ThrottleScope::Cluster => {
                    controller
                        .cluster
                        .insert(config.rebalance_type, config.max_transitions);
                }
                ThrottleScope::Resource => {
                    if let Some(ThrottleTarget::Resource(resource)) = config.target() {
                        if let Some(quotas) = controller.resources.get_mut(resource) {
                            quotas.insert(config.rebalance_type, config.max_transitions);
                        }
                    } else {
                        for quotas in controller.resources.values_mut() {
                            quotas.insert(config.rebalance_type, config.max_transitions);
                        }
                    }
                }
                ThrottleScope::Instance => {
                    if let Some(ThrottleTarget::Instance(instance)) = config.target() {
                        if let Some(quotas) = controller.instances.get_mut(instance) {
                            quotas.insert(config.rebalance_type, config.max_transitions);
                        }
                    } else {
                        for quotas in controller.instances.values_mut() {
                            quotas.insert(config.rebalance_type, config.max_transitions);
                        }
                    }
                }
            }
        }
        controller
    }

    fn should_throttle(
        &self,
        rebalance_type: RebalanceType,
        resource: &ResourceId,
        instance: &InstanceId,
    ) -> bool {
        quota_exhausted(&self.cluster, rebalance_type)
            || self
                .resources
                .get(resource)
                .is_some_and(|quota| quota_exhausted(quota, rebalance_type))
            || self
                .instances
                .get(instance)
                .is_some_and(|quota| quota_exhausted(quota, rebalance_type))
    }

    fn charge(
        &mut self,
        rebalance_type: RebalanceType,
        resource: &ResourceId,
        instance: &InstanceId,
    ) {
        charge_quota(&mut self.cluster, rebalance_type);
        if let Some(quota) = self.resources.get_mut(resource) {
            charge_quota(quota, rebalance_type);
        }
        if let Some(quota) = self.instances.get_mut(instance) {
            charge_quota(quota, rebalance_type);
        }
    }
}

fn quota_exhausted(quota: &BTreeMap<RebalanceType, usize>, rebalance_type: RebalanceType) -> bool {
    quota
        .get(&RebalanceType::Any)
        .is_some_and(|value| *value == 0)
        || quota.get(&rebalance_type).is_some_and(|value| *value == 0)
}

fn charge_quota(quota: &mut BTreeMap<RebalanceType, usize>, rebalance_type: RebalanceType) {
    if let Some(value) = quota.get_mut(&RebalanceType::Any) {
        *value = value.saturating_sub(1);
    }
    if rebalance_type != RebalanceType::Any {
        if let Some(value) = quota.get_mut(&rebalance_type) {
            *value = value.saturating_sub(1);
        }
    }
}

fn charge_pending(
    resource: &ResourceTransitionInput,
    pending: &[&OperationalPendingTransition],
    live_instances: &BTreeSet<InstanceId>,
    state_model: &StateModelDefinition,
    controller: &mut ThrottleController,
) {
    let mut by_partition: BTreeMap<&PartitionId, Vec<&OperationalPendingTransition>> =
        BTreeMap::new();
    for transition in pending {
        by_partition
            .entry(transition.partition())
            .or_default()
            .push(transition);
    }
    for (partition, transitions) in by_partition {
        let required = required_states(resource, partition, live_instances, state_model);
        let mut transitions = transitions;
        transitions
            .sort_by(|left, right| message_pending_priority(resource, left, right, state_model));
        for transition in transitions {
            let current = resource
                .current_state
                .entries()
                .get(partition)
                .and_then(|states| states.get(transition.instance()));
            if current != Some(transition.source_state())
                || current == Some(transition.target_state())
            {
                continue;
            }
            let current_map = resource.current_state.entries();
            let rebalance = rebalance_type(&required, current_map, &pending_as_request(transition));
            controller.charge(rebalance, &resource.resource, transition.instance());
        }
    }
}

fn pending_as_request(transition: &OperationalPendingTransition) -> TransitionRequest {
    TransitionRequest::new(
        transition.resource.clone(),
        transition.partition.clone(),
        transition.instance.clone(),
        transition.source_state.clone(),
        transition.target_state.clone(),
    )
}

fn message_pending_priority(
    resource: &ResourceTransitionInput,
    left: &OperationalPendingTransition,
    right: &OperationalPendingTransition,
    state_model: &StateModelDefinition,
) -> std::cmp::Ordering {
    message_priority(
        resource,
        &pending_as_request(left),
        &pending_as_request(right),
        state_model,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        compute_intermediate_and_throttle, OperationalPendingTransition, RebalanceType,
        ResourceTransitionInput, StateTransitionThrottleConfig, ThrottleScope,
    };
    use crate::model::{
        leader_standby, BestPossibleState, CurrentState, InstanceId, PartitionId, ResourceId, State,
    };
    use crate::transition::TransitionRequest;
    use std::collections::{BTreeMap, BTreeSet};

    fn state(name: &str) -> State {
        State::new(name).expect("valid state")
    }

    fn instance(name: &str) -> InstanceId {
        InstanceId::new(name).expect("valid instance")
    }

    fn partition(name: &str) -> PartitionId {
        PartitionId::new(name).expect("valid partition")
    }

    fn resource() -> ResourceId {
        ResourceId::new("documents").expect("valid resource")
    }

    fn state_map(entries: &[(&str, &str, &str)]) -> CurrentState {
        let mut builder = CurrentState::builder();
        for (partition_name, instance_name, state_name) in entries {
            builder
                .set_state(
                    partition(partition_name),
                    instance(instance_name),
                    state(state_name),
                )
                .expect("unique state entry");
        }
        builder.build()
    }

    fn best_state(entries: &[(&str, &str, &str)]) -> BestPossibleState {
        let mut builder = BestPossibleState::builder();
        for (partition_name, instance_name, state_name) in entries {
            builder
                .set_state(
                    partition(partition_name),
                    instance(instance_name),
                    state(state_name),
                )
                .expect("unique state entry");
        }
        builder.build()
    }

    fn input() -> ResourceTransitionInput {
        let resource = resource();
        let mut preference_lists = BTreeMap::new();
        preference_lists.insert(
            partition("p0"),
            vec![instance("node-a"), instance("node-b")],
        );
        preference_lists.insert(
            partition("p1"),
            vec![instance("node-a"), instance("node-c")],
        );
        let selected = [("p0", "node-b"), ("p1", "node-c")]
            .into_iter()
            .map(|(partition_name, instance_name)| {
                TransitionRequest::new(
                    resource.clone(),
                    partition(partition_name),
                    instance(instance_name),
                    state("OFFLINE"),
                    state("STANDBY"),
                )
            })
            .collect();
        ResourceTransitionInput::new(
            resource,
            2,
            None,
            preference_lists,
            state_map(&[
                ("p0", "node-a", "LEADER"),
                ("p0", "node-b", "OFFLINE"),
                ("p1", "node-a", "LEADER"),
                ("p1", "node-c", "OFFLINE"),
            ]),
            best_state(&[
                ("p0", "node-a", "LEADER"),
                ("p0", "node-b", "STANDBY"),
                ("p1", "node-a", "LEADER"),
                ("p1", "node-c", "STANDBY"),
            ]),
            selected,
        )
    }

    fn live() -> BTreeSet<InstanceId> {
        ["node-a", "node-b", "node-c"]
            .into_iter()
            .map(instance)
            .collect()
    }

    #[test]
    fn unlimited_quota_dispatches_every_selected_transition() {
        let result =
            compute_intermediate_and_throttle(&[input()], &live(), &[], &[], &leader_standby())
                .expect("valid M6 input");

        assert_eq!(result.dispatchable_transitions().len(), 2);
        assert_eq!(
            result
                .intermediate_state()
                .state(&resource(), &partition("p1"), &instance("node-c")),
            Some(&state("STANDBY"))
        );
    }

    #[test]
    fn cluster_quota_is_shared_by_partitions() {
        let config =
            StateTransitionThrottleConfig::new(RebalanceType::Any, ThrottleScope::Cluster, 1);
        let result = compute_intermediate_and_throttle(
            &[input()],
            &live(),
            &[],
            &[config],
            &leader_standby(),
        )
        .expect("valid M6 input");

        assert_eq!(result.dispatchable_transitions().len(), 1);
        assert_eq!(
            result
                .intermediate_state()
                .state(&resource(), &partition("p1"), &instance("node-c")),
            Some(&state("OFFLINE"))
        );
    }

    #[test]
    fn resource_quota_can_target_one_resource() {
        let config = StateTransitionThrottleConfig::for_resource(RebalanceType::Any, resource(), 1);
        let result = compute_intermediate_and_throttle(
            &[input()],
            &live(),
            &[],
            &[config],
            &leader_standby(),
        )
        .expect("valid M6 input");

        assert_eq!(result.dispatchable_transitions().len(), 1);
    }

    #[test]
    fn recovery_quota_classifies_missing_required_replica() {
        let mut resource_input = input();
        resource_input.min_active_replicas = Some(2);
        let config = StateTransitionThrottleConfig::new(
            RebalanceType::RecoveryBalance,
            ThrottleScope::Cluster,
            1,
        );
        let result = compute_intermediate_and_throttle(
            &[resource_input],
            &live(),
            &[],
            &[config],
            &leader_standby(),
        )
        .expect("valid M6 input");

        assert_eq!(result.dispatchable_transitions().len(), 1);
    }

    #[test]
    fn pending_transition_consumes_shared_quota() {
        let pending = OperationalPendingTransition::new(
            resource(),
            partition("p0"),
            instance("node-b"),
            state("OFFLINE"),
            state("STANDBY"),
        );
        let config =
            StateTransitionThrottleConfig::new(RebalanceType::Any, ThrottleScope::Cluster, 1);
        let result = compute_intermediate_and_throttle(
            &[input()],
            &live(),
            &[pending],
            &[config],
            &leader_standby(),
        )
        .expect("valid M6 input");

        assert!(result.dispatchable_transitions().is_empty());
        assert_eq!(
            result
                .intermediate_state()
                .state(&resource(), &partition("p0"), &instance("node-b")),
            Some(&state("STANDBY"))
        );
    }

    #[test]
    fn duplicate_pending_replica_entries_are_rejected() {
        let pending = OperationalPendingTransition::new(
            resource(),
            partition("p0"),
            instance("node-b"),
            state("OFFLINE"),
            state("STANDBY"),
        );
        let error = compute_intermediate_and_throttle(
            &[input()],
            &live(),
            &[pending.clone(), pending],
            &[],
            &leader_standby(),
        )
        .expect_err("duplicate pending replica must be rejected");

        assert!(matches!(
            error,
            super::IntermediateThrottleError::DuplicatePendingTransition { .. }
        ));
    }
}
