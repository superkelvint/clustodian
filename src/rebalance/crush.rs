//! Deterministic CRUSH placement compatible with the supported Helix inputs.

use crate::model::{InstanceId, PartitionId, ResourceId};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

const DEFAULT_NODE_WEIGHT: u64 = 1_000;
const MAX_RETRY: usize = 10;
const MAX_LOOPBACK_COUNT: usize = 50;
const HASH_MASK: u64 = 0xffff_ffff;
const CRUSH_HASH_SEED: u64 = 1_315_423_911;

/// The two-level topology needed by the Helix 2.0.1 CRUSH scenarios.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrushTopology {
    path: String,
    fault_zone_type: String,
    end_node_type: String,
}

impl CrushTopology {
    /// Construct a topology from Helix's path and node-type names.
    pub fn new(
        path: impl Into<String>,
        fault_zone_type: impl Into<String>,
        end_node_type: impl Into<String>,
    ) -> Result<Self, CrushError> {
        let topology = Self {
            path: path.into(),
            fault_zone_type: fault_zone_type.into(),
            end_node_type: end_node_type.into(),
        };
        topology.validate()?;
        Ok(topology)
    }

    fn validate(&self) -> Result<(), CrushError> {
        let path = self
            .path
            .split('/')
            .filter(|component| !component.is_empty())
            .collect::<Vec<_>>();
        let flat = path.len() == 1
            && path[0] == self.end_node_type
            && self.fault_zone_type == self.end_node_type;
        let hierarchical =
            path.len() == 2 && path[0] == self.fault_zone_type && path[1] == self.end_node_type;
        if !flat && !hierarchical {
            return Err(CrushError::UnsupportedTopology {
                path: self.path.clone(),
                fault_zone_type: self.fault_zone_type.clone(),
                end_node_type: self.end_node_type.clone(),
            });
        }
        Ok(())
    }

    fn fault_zone_type(&self) -> &str {
        &self.fault_zone_type
    }

    fn end_node_type(&self) -> &str {
        &self.end_node_type
    }
}

/// An instance and its fault-zone location in a CRUSH topology.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrushInstance {
    instance: InstanceId,
    fault_zone: String,
}

impl CrushInstance {
    /// Construct a topology leaf for an instance.
    pub fn new(instance: InstanceId, fault_zone: impl Into<String>) -> Result<Self, CrushError> {
        let fault_zone = fault_zone.into();
        if fault_zone.is_empty() {
            return Err(CrushError::EmptyFaultZone);
        }
        Ok(Self {
            instance,
            fault_zone,
        })
    }

    /// Return the instance identity.
    pub fn instance(&self) -> &InstanceId {
        &self.instance
    }

    /// Return the fault-zone identity.
    pub fn fault_zone(&self) -> &str {
        &self.fault_zone
    }
}

/// Errors raised by the supported CRUSH placement operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CrushError {
    DuplicateInstance(InstanceId),
    DuplicatePartition(PartitionId),
    EmptyFaultZone,
    LiveInstanceUnknown(InstanceId),
    NoInstances,
    NoPartitions,
    SelectedNonInstance,
    UnsupportedTopology {
        path: String,
        fault_zone_type: String,
        end_node_type: String,
    },
}

impl fmt::Display for CrushError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateInstance(instance) => {
                write!(formatter, "duplicate instance: {instance}")
            }
            Self::DuplicatePartition(partition) => {
                write!(formatter, "duplicate partition: {partition}")
            }
            Self::EmptyFaultZone => formatter.write_str("fault zone must not be empty"),
            Self::LiveInstanceUnknown(instance) => {
                write!(
                    formatter,
                    "live instance is not in the topology: {instance}"
                )
            }
            Self::NoInstances => formatter.write_str("CRUSH requires at least one instance"),
            Self::NoPartitions => formatter.write_str("CRUSH requires at least one partition"),
            Self::SelectedNonInstance => formatter.write_str("CRUSH selected a non-instance node"),
            Self::UnsupportedTopology {
                path,
                fault_zone_type,
                end_node_type,
            } => write!(
                formatter,
                "unsupported CRUSH topology {path:?} ({fault_zone_type:?} -> {end_node_type:?})"
            ),
        }
    }
}

impl std::error::Error for CrushError {}

/// Compute Helix 2.0.1 CRUSH preference lists.
pub fn compute_crush_assignment(
    _resource: &ResourceId,
    partitions: &[PartitionId],
    replica_count: usize,
    instances: &[CrushInstance],
    live_instances: &BTreeSet<InstanceId>,
    topology: &CrushTopology,
) -> Result<BTreeMap<PartitionId, Vec<InstanceId>>, CrushError> {
    topology.validate()?;
    if partitions.is_empty() {
        return Err(CrushError::NoPartitions);
    }
    if instances.is_empty() {
        return Err(CrushError::NoInstances);
    }

    let mut partition_names = BTreeSet::new();
    for partition in partitions {
        if !partition_names.insert(partition) {
            return Err(CrushError::DuplicatePartition(partition.clone()));
        }
    }

    let mut instance_names = BTreeSet::new();
    for instance in instances {
        if !instance_names.insert(instance.instance.clone()) {
            return Err(CrushError::DuplicateInstance(instance.instance.clone()));
        }
    }
    for instance in live_instances {
        if !instance_names.contains(instance) {
            return Err(CrushError::LiveInstanceUnknown(instance.clone()));
        }
    }

    let tree = TopologyTree::build(instances, live_instances, topology);
    let mut assignment = BTreeMap::new();
    for partition in partitions {
        let input = java_string_hash(partition.as_str()) as i32 as i64;
        let selected = tree.select_replicas(tree.root, input, replica_count);
        let mut preference_list = Vec::with_capacity(selected.len());
        for node in selected {
            let Some(instance) = tree.nodes[node].instance.as_ref() else {
                return Err(CrushError::SelectedNonInstance);
            };
            preference_list.push(instance.clone());
        }
        assignment.insert(partition.clone(), preference_list);
    }
    Ok(assignment)
}

#[derive(Clone, Debug)]
struct Node {
    // Node indexes and child indexes are physical CRUSH coordinates. They do
    // not escape this module; callers receive only InstanceId values.
    name: String,
    node_type: String,
    id: u64,
    weight: u64,
    failed: bool,
    children: Vec<usize>,
    instance: Option<InstanceId>,
}

struct TopologyTree {
    nodes: Vec<Node>,
    root: usize,
    topology: CrushTopology,
}

impl TopologyTree {
    fn build(
        instances: &[CrushInstance],
        live_instances: &BTreeSet<InstanceId>,
        topology: &CrushTopology,
    ) -> Self {
        let mut tree = Self {
            nodes: Vec::with_capacity(instances.len() * 2 + 1),
            root: 0,
            topology: topology.clone(),
        };
        tree.root = tree.add_node("root", "ROOT", None, false);
        let mut zones = BTreeMap::new();
        for instance in instances {
            if topology.fault_zone_type() == topology.end_node_type() {
                let leaf = tree.add_node(
                    instance.instance().as_str(),
                    topology.end_node_type(),
                    Some(instance.instance.clone()),
                    !live_instances.contains(instance.instance()),
                );
                if live_instances.contains(instance.instance()) {
                    tree.nodes[leaf].weight = DEFAULT_NODE_WEIGHT;
                    tree.nodes[tree.root].weight += DEFAULT_NODE_WEIGHT;
                }
                tree.nodes[tree.root].children.push(leaf);
                continue;
            }
            let zone = if let Some(zone) = zones.get(instance.fault_zone()) {
                *zone
            } else {
                let zone = tree.add_node(
                    instance.fault_zone(),
                    topology.fault_zone_type(),
                    None,
                    false,
                );
                tree.nodes[tree.root].children.push(zone);
                zones.insert(instance.fault_zone().to_owned(), zone);
                zone
            };
            let is_live = live_instances.contains(instance.instance());
            let leaf = tree.add_node(
                instance.instance().as_str(),
                topology.end_node_type(),
                Some(instance.instance.clone()),
                !is_live,
            );
            if is_live {
                tree.nodes[leaf].weight = DEFAULT_NODE_WEIGHT;
                tree.nodes[zone].weight += DEFAULT_NODE_WEIGHT;
                tree.nodes[tree.root].weight += DEFAULT_NODE_WEIGHT;
            }
            tree.nodes[zone].children.push(leaf);
        }
        tree
    }

    fn add_node(
        &mut self,
        name: &str,
        node_type: &str,
        instance: Option<InstanceId>,
        failed: bool,
    ) -> usize {
        let node = Node {
            name: name.to_owned(),
            node_type: node_type.to_owned(),
            id: sha1_first_u32(name),
            weight: 0,
            failed,
            children: Vec::new(),
            instance,
        };
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    fn select_replicas(&self, top_node: usize, data: i64, replica_count: usize) -> Vec<usize> {
        let mut nodes = Vec::with_capacity(replica_count);
        let mut selected_zones = BTreeSet::new();
        let mut input = data;
        let mut count = replica_count;
        let mut tries = 0;
        while nodes.len() < replica_count {
            self.add_replicas_for_attempt(top_node, input, count, &mut nodes, &mut selected_zones);
            count = replica_count.saturating_sub(nodes.len());
            if count > 0 {
                input = jenkins_hash_one(input);
                tries += 1;
                if tries >= MAX_RETRY {
                    break;
                }
            }
        }
        nodes
    }

    fn add_replicas_for_attempt(
        &self,
        top_node: usize,
        input: i64,
        replica_count: usize,
        selected_nodes: &mut Vec<usize>,
        selected_zones: &mut BTreeSet<usize>,
    ) {
        if self.topology.fault_zone_type() != self.topology.end_node_type() {
            let zones = self.select_nodes_of_type(
                top_node,
                input,
                replica_count,
                self.topology.fault_zone_type(),
                selected_zones,
            );
            selected_zones.extend(zones.iter().copied());
            for zone in zones {
                let end_nodes = self.select_nodes_of_type(
                    zone,
                    input,
                    1,
                    self.topology.end_node_type(),
                    &BTreeSet::new(),
                );
                selected_nodes.extend(end_nodes);
            }
        } else {
            let selected = selected_nodes.iter().copied().collect::<BTreeSet<_>>();
            selected_nodes.extend(self.select_nodes_of_type(
                top_node,
                input,
                replica_count,
                self.topology.end_node_type(),
                &selected,
            ));
        }
    }

    fn select_nodes_of_type(
        &self,
        parent: usize,
        input: i64,
        count: usize,
        node_type: &str,
        excluded: &BTreeSet<usize>,
    ) -> Vec<usize> {
        let mut selected = Vec::with_capacity(count);
        for r in 1..=count {
            let mut failure = 0;
            let mut loopback_count = 0;
            let mut escape = false;
            let out = 'origin: loop {
                let mut input_node = parent;
                let mut rejected = BTreeSet::new();
                loop {
                    let round = r + failure;
                    let candidate = self.choose_child(input_node, input, round);
                    if self.nodes[candidate].node_type != node_type {
                        input_node = candidate;
                        continue;
                    }
                    if selected.contains(&candidate) || excluded.contains(&candidate) {
                        rejected.insert(candidate);
                        if self.all_children_eliminated(input_node, &selected, &rejected) {
                            if loopback_count == MAX_LOOPBACK_COUNT {
                                escape = true;
                                break 'origin candidate;
                            }
                            loopback_count += 1;
                            failure += 1;
                            continue 'origin;
                        }
                        failure += 1;
                        continue;
                    }
                    if self.is_node_unavailable(candidate) {
                        failure += 1;
                        if loopback_count == MAX_LOOPBACK_COUNT {
                            escape = true;
                            break 'origin candidate;
                        }
                        loopback_count += 1;
                        continue 'origin;
                    }
                    break 'origin candidate;
                }
            };
            if escape {
                continue;
            }
            selected.push(out);
        }
        selected
    }

    fn choose_child(&self, parent: usize, input: i64, round: usize) -> usize {
        let children = &self.nodes[parent].children;
        let mut sorted = children.clone();
        sorted.sort_by(|left, right| self.nodes[*right].weight.cmp(&self.nodes[*left].weight));

        let mut straws = Vec::with_capacity(sorted.len());
        let mut num_left = sorted.len() as f32;
        let mut straw = 1.0_f32;
        let mut w_below = 0.0_f32;
        let mut last_w = 0.0_f32;
        let mut index = 0;
        while index < sorted.len() {
            let current = sorted[index];
            if self.nodes[current].weight == 0 {
                straws.push((current, 0_u64, index));
                index += 1;
                continue;
            }
            straws.push((current, (straw * 65_536.0_f32) as u64, index));
            index += 1;
            if index == sorted.len() {
                break;
            }
            let current_weight = self.nodes[sorted[index]].weight as f32;
            let previous_weight = self.nodes[sorted[index - 1]].weight as f32;
            if current_weight == previous_weight {
                continue;
            }
            w_below += (previous_weight - last_w) * num_left;
            let mut group_end = index;
            while group_end < sorted.len()
                && self.nodes[sorted[group_end]].weight as f32 == current_weight
            {
                num_left -= 1.0;
                group_end += 1;
            }
            let w_next = num_left * (current_weight - previous_weight);
            let p_below = w_below / (w_below + w_next);
            straw *= (1.0_f64 / f64::from(p_below)).powf(1.0_f64 / f64::from(num_left)) as f32;
            last_w = previous_weight;
        }
        let capacity = java_hash_map_capacity(straws.len());
        straws.sort_by_key(|(node, _, insertion)| {
            (
                java_hash_map_bucket(&self.nodes[*node].name, capacity),
                *insertion,
            )
        });
        let mut selected = straws[0].0;
        let mut high_score = 0_u64;
        for (node, node_straw, _) in straws {
            let hash = jenkins_hash_three(input, self.nodes[node].id, round as i64) & 0xffff;
            let score = hash * node_straw;
            if score > high_score {
                selected = node;
                high_score = score;
            }
        }
        selected
    }

    fn is_node_unavailable(&self, node: usize) -> bool {
        self.nodes[node].weight == 0
            || (self.nodes[node].children.is_empty() && self.nodes[node].failed)
    }

    fn all_children_eliminated(
        &self,
        parent: usize,
        selected: &[usize],
        rejected: &BTreeSet<usize>,
    ) -> bool {
        self.nodes[parent].children.iter().all(|child| {
            self.is_node_unavailable(*child) || selected.contains(child) || rejected.contains(child)
        })
    }
}

// The following helpers reproduce Java's ordering and hash arithmetic. Keep
// them separate from the placement algorithm so the compatibility details do
// not obscure the selection flow above.
fn java_hash_map_capacity(entries: usize) -> u32 {
    let mut capacity = 16_u32;
    while entries > (capacity as usize * 3) / 4 {
        capacity *= 2;
    }
    capacity
}

fn java_hash_map_bucket(name: &str, capacity: u32) -> u32 {
    let hash = java_string_hash(name);
    (hash ^ (hash >> 16)) & (capacity - 1)
}

fn java_string_hash(value: &str) -> u32 {
    let mut hash = 0_u32;
    for code_unit in value.encode_utf16() {
        hash = hash.wrapping_mul(31).wrapping_add(u32::from(code_unit));
    }
    hash
}

fn sha1_first_u32(value: &str) -> u64 {
    let mut message = value.as_bytes().to_vec();
    let bit_length = (message.len() as u64) * 8;
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_length.to_be_bytes());

    let mut h = [
        0x6745_2301_u32,
        0xefcd_ab89,
        0x98ba_dcfe,
        0x1032_5476,
        0xc3d2_e1f0,
    ];
    debug_assert_eq!(message.len() % 64, 0);
    let chunks = message.chunks_exact(64);
    for chunk in chunks {
        let mut words = [0_u32; 80];
        for (index, word) in words[..16].iter_mut().enumerate() {
            *word = u32::from_be_bytes([
                chunk[index * 4],
                chunk[index * 4 + 1],
                chunk[index * 4 + 2],
                chunk[index * 4 + 3],
            ]);
        }
        for index in 16..80 {
            words[index] =
                (words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16])
                    .rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (index, word) in words.iter().enumerate() {
            let (function, constant) = match index {
                0..=19 => ((b & c) | ((!b) & d), 0x5a82_7999),
                20..=39 => (b ^ c ^ d, 0x6ed9_eba1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1b_bcdc),
                _ => (b ^ c ^ d, 0xca62_c1d6),
            };
            let temporary = a
                .rotate_left(5)
                .wrapping_add(function)
                .wrapping_add(e)
                .wrapping_add(constant)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temporary;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }
    u64::from(h[0])
}

fn jenkins_hash_one(value: i64) -> i64 {
    let a = value as u64 & HASH_MASK;
    let (b, x, hash) = hash_mix(a, 231_232, (CRUSH_HASH_SEED ^ a) & HASH_MASK);
    let _ = (b, x);
    let (_, _, hash) = hash_mix(1_232, a, hash);
    hash as i32 as i64
}

fn jenkins_hash_three(a: i64, b: u64, c: i64) -> u64 {
    let mut a = a as u64 & HASH_MASK;
    let mut b = b & HASH_MASK;
    let mut c = c as u64 & HASH_MASK;
    let mut hash = (CRUSH_HASH_SEED ^ a ^ b ^ c) & HASH_MASK;
    let mut x = 231_232_u64;
    let mut y = 1_232_u64;
    let mixed = hash_mix(a, b, hash);
    a = mixed.0;
    b = mixed.1;
    hash = mixed.2;
    let mixed = hash_mix(c, x, hash);
    c = mixed.0;
    x = mixed.1;
    hash = mixed.2;
    let mixed = hash_mix(y, a, hash);
    y = mixed.0;
    let _ = mixed.1;
    hash = mixed.2;
    let mixed = hash_mix(b, x, hash);
    let _ = (mixed.0, mixed.1);
    hash = mixed.2;
    let mixed = hash_mix(y, c, hash);
    mixed.2
}

fn hash_mix(mut a: u64, mut b: u64, mut c: u64) -> (u64, u64, u64) {
    a = a.wrapping_sub(b) & HASH_MASK;
    a = a.wrapping_sub(c) & HASH_MASK;
    a = (a ^ (c >> 13)) & HASH_MASK;
    b = b.wrapping_sub(c) & HASH_MASK;
    b = b.wrapping_sub(a) & HASH_MASK;
    b = (b ^ ((a << 8) & HASH_MASK)) & HASH_MASK;
    c = c.wrapping_sub(a) & HASH_MASK;
    c = c.wrapping_sub(b) & HASH_MASK;
    c = (c ^ (b >> 13)) & HASH_MASK;
    a = a.wrapping_sub(b) & HASH_MASK;
    a = a.wrapping_sub(c) & HASH_MASK;
    a = (a ^ (c >> 12)) & HASH_MASK;
    b = b.wrapping_sub(c) & HASH_MASK;
    b = b.wrapping_sub(a) & HASH_MASK;
    b = (b ^ ((a << 16) & HASH_MASK)) & HASH_MASK;
    c = c.wrapping_sub(a) & HASH_MASK;
    c = c.wrapping_sub(b) & HASH_MASK;
    c = (c ^ (b >> 5)) & HASH_MASK;
    a = a.wrapping_sub(b) & HASH_MASK;
    a = a.wrapping_sub(c) & HASH_MASK;
    a = (a ^ (c >> 3)) & HASH_MASK;
    b = b.wrapping_sub(c) & HASH_MASK;
    b = b.wrapping_sub(a) & HASH_MASK;
    b = (b ^ ((a << 10) & HASH_MASK)) & HASH_MASK;
    c = c.wrapping_sub(a) & HASH_MASK;
    c = c.wrapping_sub(b) & HASH_MASK;
    c = (c ^ (b >> 15)) & HASH_MASK;
    (a, b, c)
}

#[cfg(test)]
mod tests {
    use super::{compute_crush_assignment, CrushError, CrushInstance, CrushTopology};
    use crate::model::{InstanceId, PartitionId, ResourceId};
    use std::collections::BTreeSet;

    fn instance(name: &str, zone: &str) -> CrushInstance {
        CrushInstance::new(InstanceId::new(name).unwrap(), zone).unwrap()
    }

    fn topology() -> CrushTopology {
        CrushTopology::new("/zone/instance", "zone", "instance").unwrap()
    }

    #[test]
    fn flat_topology_uses_direct_instance_selection() {
        let instances = [
            instance("node-a", "zone-a"),
            instance("node-b", "zone-b"),
            instance("node-c", "zone-c"),
        ];
        let live = instances
            .iter()
            .map(|value| value.instance().clone())
            .collect();
        let topology = CrushTopology::new("/instance", "instance", "instance").unwrap();
        let result = compute_crush_assignment(
            &ResourceId::new("documents").unwrap(),
            &partitions(4),
            2,
            &instances,
            &live,
            &topology,
        )
        .unwrap();
        assert_eq!(
            result[&PartitionId::new("documents_0").unwrap()],
            vec![
                InstanceId::new("node-c").unwrap(),
                InstanceId::new("node-a").unwrap()
            ]
        );
        assert_eq!(
            result[&PartitionId::new("documents_3").unwrap()],
            vec![
                InstanceId::new("node-a").unwrap(),
                InstanceId::new("node-b").unwrap()
            ]
        );
    }

    fn partitions(count: usize) -> Vec<PartitionId> {
        (0..count)
            .map(|index| PartitionId::new(format!("documents_{index}")).unwrap())
            .collect()
    }

    #[test]
    fn placement_is_deterministic_and_zone_aware() {
        let instances = vec![
            instance("node-a1", "zone-a"),
            instance("node-a2", "zone-a"),
            instance("node-b1", "zone-b"),
            instance("node-b2", "zone-b"),
            instance("node-c1", "zone-c"),
            instance("node-c2", "zone-c"),
        ];
        let live = instances
            .iter()
            .map(|item| item.instance().clone())
            .collect::<BTreeSet<_>>();
        let resource = ResourceId::new("documents").unwrap();
        let partitions = partitions(24);
        let first =
            compute_crush_assignment(&resource, &partitions, 3, &instances, &live, &topology())
                .unwrap();
        let second =
            compute_crush_assignment(&resource, &partitions, 3, &instances, &live, &topology())
                .unwrap();
        assert_eq!(first, second);
        for preference_list in first.values() {
            assert_eq!(preference_list.len(), 3);
            let zones = preference_list
                .iter()
                .map(|instance| {
                    instances
                        .iter()
                        .find(|candidate| candidate.instance() == instance)
                        .unwrap()
                        .fault_zone()
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(zones.len(), 3);
        }
    }

    #[test]
    fn insufficient_fault_zones_returns_available_replicas() {
        let instances = vec![instance("node-a1", "zone-a"), instance("node-a2", "zone-a")];
        let live = instances
            .iter()
            .map(|item| item.instance().clone())
            .collect::<BTreeSet<_>>();
        let result = compute_crush_assignment(
            &ResourceId::new("documents").unwrap(),
            &partitions(1),
            3,
            &instances,
            &live,
            &topology(),
        )
        .unwrap();
        assert_eq!(result.values().next().unwrap().len(), 1);
    }

    #[test]
    fn unavailable_instances_are_not_selected() {
        let instances = vec![instance("node-a", "zone-a"), instance("node-b", "zone-b")];
        let live = [InstanceId::new("node-a").unwrap()].into_iter().collect();
        let result = compute_crush_assignment(
            &ResourceId::new("documents").unwrap(),
            &partitions(1),
            2,
            &instances,
            &live,
            &topology(),
        )
        .unwrap();
        assert_eq!(
            result.values().next().unwrap(),
            &vec![InstanceId::new("node-a").unwrap()]
        );
    }

    #[test]
    fn unequal_zone_weights_keep_each_positive_zone_in_the_straw_bucket() {
        let instances = vec![
            instance("a-0", "zone-a"),
            instance("a-1", "zone-a"),
            instance("a-2", "zone-a"),
            instance("b-0", "zone-b"),
            instance("b-1", "zone-b"),
            instance("c-0", "zone-c"),
        ];
        let live = instances
            .iter()
            .map(|item| item.instance().clone())
            .collect::<BTreeSet<_>>();
        let result = compute_crush_assignment(
            &ResourceId::new("documents").unwrap(),
            &partitions(16),
            3,
            &instances,
            &live,
            &topology(),
        )
        .unwrap();
        for preference_list in result.values() {
            assert_eq!(preference_list.len(), 3);
            let zones = preference_list
                .iter()
                .map(|selected| {
                    instances
                        .iter()
                        .find(|candidate| candidate.instance() == selected)
                        .unwrap()
                        .fault_zone()
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(zones.len(), 3);
        }
    }

    #[test]
    fn rejects_invalid_topologies_and_assignment_inputs() {
        assert!(matches!(
            CrushTopology::new("/rack/instance", "zone", "instance"),
            Err(CrushError::UnsupportedTopology { .. })
        ));
        assert_eq!(
            CrushInstance::new(InstanceId::new("node-a").unwrap(), "").unwrap_err(),
            CrushError::EmptyFaultZone
        );

        let resource = ResourceId::new("documents").unwrap();
        let node_a = instance("node-a", "zone-a");
        let live_a = [InstanceId::new("node-a").unwrap()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            compute_crush_assignment(
                &resource,
                &[],
                1,
                std::slice::from_ref(&node_a),
                &live_a,
                &topology(),
            )
            .unwrap_err(),
            CrushError::NoPartitions
        );
        assert_eq!(
            compute_crush_assignment(
                &resource,
                &partitions(1),
                1,
                &[],
                &BTreeSet::new(),
                &topology()
            )
            .unwrap_err(),
            CrushError::NoInstances
        );
        assert!(matches!(
            compute_crush_assignment(
                &resource,
                &[
                    PartitionId::new("p0").unwrap(),
                    PartitionId::new("p0").unwrap()
                ],
                1,
                std::slice::from_ref(&node_a),
                &live_a,
                &topology(),
            ),
            Err(CrushError::DuplicatePartition(_))
        ));
        assert!(matches!(
            compute_crush_assignment(
                &resource,
                &partitions(1),
                1,
                &[node_a.clone(), instance("node-a", "zone-b")],
                &live_a,
                &topology(),
            ),
            Err(CrushError::DuplicateInstance(_))
        ));
        assert!(matches!(
            compute_crush_assignment(
                &resource,
                &partitions(1),
                1,
                &[node_a],
                &[InstanceId::new("node-b").unwrap()].into_iter().collect(),
                &topology(),
            ),
            Err(CrushError::LiveInstanceUnknown(_))
        ));
    }

    #[test]
    fn crush_errors_have_stable_operator_messages() {
        let instance_id = InstanceId::new("node-a").unwrap();
        let partition_id = PartitionId::new("p0").unwrap();
        let errors = [
            CrushError::DuplicateInstance(instance_id.clone()),
            CrushError::DuplicatePartition(partition_id),
            CrushError::EmptyFaultZone,
            CrushError::LiveInstanceUnknown(instance_id),
            CrushError::NoInstances,
            CrushError::NoPartitions,
            CrushError::SelectedNonInstance,
            CrushError::UnsupportedTopology {
                path: String::from("/rack/instance"),
                fault_zone_type: String::from("zone"),
                end_node_type: String::from("instance"),
            },
        ];
        assert!(errors.iter().all(|error| !error.to_string().is_empty()));
    }
}
