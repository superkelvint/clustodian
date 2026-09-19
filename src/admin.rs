//! Administrative writes for controller-consumed cluster metadata.

use crate::coordination::etcd::{CoordinationError, EtcdCoordination};
use crate::model::{InstanceId, PartitionId, ResourceId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const ADMIN_CAS_RETRIES: usize = 32;

const INSTANCE_CONFIGS_KEY: &str = "controller/instance-configs";
const THROTTLES_KEY: &str = "controller/throttles";

/// An instance configuration supplied to the controller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstanceSpec {
    pub instance_id: String,
    pub zone: String,
}

/// A controller transition throttle configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ThrottleSpec {
    pub scope: String,
    pub rebalance_type: String,
    pub max_in_flight: usize,
}

/// A typed transition limit supplied by the application facade.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct TransitionLimitSpec {
    pub(crate) target: TransitionLimitTarget,
    pub(crate) rebalance_type: TransitionRebalanceType,
    pub(crate) max_in_flight: usize,
}

/// The target of a typed transition limit.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum TransitionLimitTarget {
    Cluster,
    Resource(ResourceId),
    Instance(InstanceId),
}

/// The class of transitions counted by a typed transition limit.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum TransitionRebalanceType {
    Any,
    RecoveryBalance,
    LoadBalance,
}

/// The placement strategy for a resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlacementSpec {
    Crush,
    /// CRUSH placement using an explicit topology and fault-domain policy.
    CrushWithTopology {
        topology: CrushTopologySpec,
    },
    SemiAuto {
        preference_lists: BTreeMap<String, Vec<String>>,
    },
}

/// The topology used by a topology-aware CRUSH resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrushTopologySpec {
    pub path: String,
    pub fault_zone_type: String,
    pub end_node_type: String,
}

impl CrushTopologySpec {
    /// Construct a topology specification. Structural validation happens when
    /// the controller builds the rebalance topology.
    pub fn new(
        path: impl Into<String>,
        fault_zone_type: impl Into<String>,
        end_node_type: impl Into<String>,
    ) -> Self {
        Self {
            path: path.into(),
            fault_zone_type: fault_zone_type.into(),
            end_node_type: end_node_type.into(),
        }
    }
}

/// A resource configuration supplied to the controller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceSpec {
    pub name: String,
    pub partitions: usize,
    pub replicas: usize,
    pub state_model: String,
    pub placement: PlacementSpec,
}

/// Writes normal controller metadata into a coordination namespace.
#[derive(Clone)]
pub struct ClusterAdmin {
    coordination: EtcdCoordination,
}

impl ClusterAdmin {
    /// Create an administrator for one coordination namespace.
    pub fn new(coordination: EtcdCoordination) -> Self {
        Self { coordination }
    }

    /// Establish the cluster identity consumed by administrative operations.
    pub async fn ensure_cluster(&self, cluster: &str) -> Result<(), CoordinationError> {
        if cluster != self.coordination.cluster()
            || cluster.trim().is_empty()
            || cluster.as_bytes().contains(&0)
        {
            return Err(CoordinationError::InvalidCluster);
        }
        if self
            .coordination
            .put_metadata_if_absent("controller/cluster", cluster)
            .await?
        {
            return Ok(());
        }

        let existing = self.coordination.get_metadata("controller/cluster").await?;
        if existing.and_then(|entry| entry.value().map(str::to_owned)) == Some(cluster.to_owned()) {
            Ok(())
        } else {
            Err(CoordinationError::InvalidCluster)
        }
    }

    /// Add or replace one controller-visible instance configuration.
    pub async fn put_instance(&self, spec: InstanceSpec) -> Result<(), CoordinationError> {
        let instance =
            InstanceId::new(spec.instance_id.clone()).map_err(|_| CoordinationError::InvalidKey)?;
        if spec.zone.trim().is_empty() || spec.zone.as_bytes().contains(&0) {
            return Err(CoordinationError::InvalidValue);
        }
        for _ in 0..ADMIN_CAS_RETRIES {
            let current = self.coordination.get_metadata(INSTANCE_CONFIGS_KEY).await?;
            let mut instances = decode_instances(current.as_ref())?;
            instances.retain(|record| record.name != instance.as_str());
            instances.push(InstanceRecord {
                name: instance.to_string(),
                zone: spec.zone.clone(),
            });
            instances.sort_by(|left, right| left.name.cmp(&right.name));
            let mut existing = decode_instances(current.as_ref())?;
            existing.sort_by(|left, right| left.name.cmp(&right.name));
            if instances == existing {
                return Ok(());
            }
            let value =
                serde_json::to_string(&instances).map_err(|_| CoordinationError::InvalidValue)?;
            let applied = match current {
                Some(entry) => self
                    .coordination
                    .compare_and_put_metadata(INSTANCE_CONFIGS_KEY, entry.revision(), &value)
                    .await?
                    .applied(),
                None => {
                    self.coordination
                        .put_metadata_if_absent(INSTANCE_CONFIGS_KEY, &value)
                        .await?
                }
            };
            if applied {
                return Ok(());
            }
        }
        Err(CoordinationError::Contention)
    }

    /// Remove one instance from controller-visible configuration.
    pub async fn remove_instance(&self, instance_id: &str) -> Result<(), CoordinationError> {
        let instance =
            InstanceId::new(instance_id.to_owned()).map_err(|_| CoordinationError::InvalidKey)?;
        for _ in 0..ADMIN_CAS_RETRIES {
            let Some(current) = self.coordination.get_metadata(INSTANCE_CONFIGS_KEY).await? else {
                return Ok(());
            };
            let mut instances = decode_instances(Some(&current))?;
            if !instances
                .iter()
                .any(|record| record.name == instance.as_str())
            {
                return Ok(());
            }
            instances.retain(|record| record.name != instance.as_str());
            let value =
                serde_json::to_string(&instances).map_err(|_| CoordinationError::InvalidValue)?;
            if self
                .coordination
                .compare_and_put_metadata_if_live_absent(
                    INSTANCE_CONFIGS_KEY,
                    current.revision(),
                    &instance,
                    &value,
                )
                .await?
                .applied()
            {
                return Ok(());
            }
            if self.coordination.live_session(&instance).await?.is_some() {
                return Err(CoordinationError::InstanceStillLive(instance));
            }
        }
        Err(CoordinationError::Contention)
    }

    /// Add or replace one resource configuration in the controller metadata.
    pub async fn put_resource(&self, spec: ResourceSpec) -> Result<(), CoordinationError> {
        let resource =
            ResourceId::new(spec.name.clone()).map_err(|_| CoordinationError::InvalidKey)?;
        if spec.partitions == 0 || spec.replicas == 0 || spec.state_model != "LeaderStandby" {
            return Err(CoordinationError::InvalidValue);
        }
        let placement = match spec.placement {
            PlacementSpec::Crush => PlacementRecord {
                kind: String::from("CRUSH"),
                replicas: spec.replicas,
                partitions: (0..spec.partitions)
                    .map(|index| format!("{}_{}", resource.as_str(), index))
                    .collect(),
                preference_lists: BTreeMap::new(),
                topology: None,
            },
            PlacementSpec::CrushWithTopology { topology } => PlacementRecord {
                kind: String::from("CRUSH"),
                replicas: spec.replicas,
                partitions: (0..spec.partitions)
                    .map(|index| format!("{}_{}", resource.as_str(), index))
                    .collect(),
                preference_lists: BTreeMap::new(),
                topology: Some(CrushTopologyRecord {
                    path: topology.path,
                    fault_zone_type: topology.fault_zone_type,
                    end_node_type: topology.end_node_type,
                }),
            },
            PlacementSpec::SemiAuto { preference_lists } => {
                if preference_lists.len() != spec.partitions {
                    return Err(CoordinationError::InvalidValue);
                }
                for (partition, instances) in &preference_lists {
                    PartitionId::new(partition.clone())
                        .map_err(|_| CoordinationError::InvalidKey)?;
                    if instances.len() != spec.replicas {
                        return Err(CoordinationError::InvalidValue);
                    }
                    if instances.iter().collect::<BTreeSet<_>>().len() != instances.len() {
                        return Err(CoordinationError::InvalidValue);
                    }
                    for instance in instances {
                        InstanceId::new(instance.clone())
                            .map_err(|_| CoordinationError::InvalidKey)?;
                    }
                }
                PlacementRecord {
                    kind: String::from("SEMI_AUTO"),
                    replicas: spec.replicas,
                    partitions: Vec::new(),
                    preference_lists,
                    topology: None,
                }
            }
        };
        let record = ResourceRecord {
            name: resource.to_string(),
            state_model: spec.state_model,
            placement,
        };
        let value = serde_json::to_string(&record).map_err(|_| CoordinationError::InvalidValue)?;
        let key = format!("controller/resources/{}", resource.as_str());
        if self
            .coordination
            .get_metadata(&key)
            .await?
            .and_then(|entry| entry.value().map(str::to_owned))
            .as_deref()
            == Some(value.as_str())
        {
            return Ok(());
        }
        self.coordination.put_metadata(&key, &value).await?;
        Ok(())
    }

    /// Resource deletion is intentionally disabled until tombstone lifecycle
    /// support can make the operation restart-safe.
    pub async fn remove_resource(&self, resource_id: &str) -> Result<(), CoordinationError> {
        ResourceId::new(resource_id.to_owned()).map_err(|_| CoordinationError::InvalidKey)?;
        Err(CoordinationError::ResourceDeletionDisabled)
    }

    /// Replace the controller transition throttle configuration.
    pub async fn put_throttles(
        &self,
        throttles: Vec<ThrottleSpec>,
    ) -> Result<(), CoordinationError> {
        if throttles
            .iter()
            .any(|throttle| throttle.max_in_flight == 0 || throttle.scope.trim().is_empty())
        {
            return Err(CoordinationError::InvalidValue);
        }
        let value =
            serde_json::to_string(&throttles).map_err(|_| CoordinationError::InvalidValue)?;
        self.coordination
            .put_metadata(THROTTLES_KEY, &value)
            .await?;
        Ok(())
    }

    /// Replace the typed controller transition throttle configuration.
    pub(crate) async fn put_transition_limits(
        &self,
        limits: Vec<TransitionLimitSpec>,
    ) -> Result<(), CoordinationError> {
        if limits.iter().any(|limit| limit.max_in_flight == 0) {
            return Err(CoordinationError::InvalidValue);
        }
        let records: Vec<_> = limits
            .into_iter()
            .map(TransitionLimitRecord::from)
            .collect();
        let value = serde_json::to_string(&records).map_err(|_| CoordinationError::InvalidValue)?;
        self.coordination
            .put_metadata(THROTTLES_KEY, &value)
            .await?;
        Ok(())
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
struct InstanceRecord {
    name: String,
    zone: String,
}

fn decode_instances(
    entry: Option<&crate::coordination::etcd::MetadataEntry>,
) -> Result<Vec<InstanceRecord>, CoordinationError> {
    entry
        .and_then(|entry| entry.value())
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| CoordinationError::InvalidValue)
        .map(|instances| instances.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::{
        decode_instances, CrushTopologySpec, TransitionLimitRecord, TransitionLimitSpec,
        TransitionLimitTarget, TransitionRebalanceType,
    };
    use crate::coordination::etcd::{MetadataEntry, Revision};
    use crate::model::{InstanceId, ResourceId};

    fn entry(value: &str) -> MetadataEntry {
        MetadataEntry {
            value: Some(value.to_owned()),
            revision: Revision::new(1).unwrap(),
        }
    }

    #[test]
    fn decodes_optional_instance_metadata_and_rejects_bad_json() {
        assert!(decode_instances(None).unwrap().is_empty());
        assert!(decode_instances(Some(&entry("[]"))).unwrap().is_empty());
        let instances =
            decode_instances(Some(&entry(r#"[{"name":"node-a","zone":"zone-a"}]"#))).unwrap();
        assert_eq!(instances[0].name, "node-a");
        assert_eq!(instances[0].zone, "zone-a");
        assert!(decode_instances(Some(&entry("not-json"))).is_err());
        let topology = CrushTopologySpec::new("/zone/instance", "zone", "instance");
        assert_eq!(topology.path, "/zone/instance");
        assert_eq!(topology.fault_zone_type, "zone");
        assert_eq!(topology.end_node_type, "instance");
    }

    #[test]
    fn transition_limits_encode_all_targets_and_rebalance_types() {
        let cases = [
            (
                TransitionLimitTarget::Cluster,
                TransitionRebalanceType::Any,
                ("CLUSTER", None, None, "ANY"),
            ),
            (
                TransitionLimitTarget::Resource(ResourceId::new("documents").unwrap()),
                TransitionRebalanceType::RecoveryBalance,
                ("RESOURCE", Some("documents"), None, "RECOVERY_BALANCE"),
            ),
            (
                TransitionLimitTarget::Instance(InstanceId::new("node-a").unwrap()),
                TransitionRebalanceType::LoadBalance,
                ("INSTANCE", None, Some("node-a"), "LOAD_BALANCE"),
            ),
        ];

        for (target, rebalance_type, expected) in cases {
            let record = TransitionLimitRecord::from(TransitionLimitSpec {
                target,
                rebalance_type,
                max_in_flight: 3,
            });
            assert_eq!(record.scope, expected.0);
            assert_eq!(record.resource.as_deref(), expected.1);
            assert_eq!(record.instance.as_deref(), expected.2);
            assert_eq!(record.rebalance_type, expected.3);
            assert_eq!(record.max_in_flight, 3);
        }
    }
}

#[derive(Serialize)]
struct ResourceRecord {
    name: String,
    state_model: String,
    placement: PlacementRecord,
}

#[derive(Serialize)]
struct PlacementRecord {
    kind: String,
    replicas: usize,
    partitions: Vec<String>,
    preference_lists: BTreeMap<String, Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topology: Option<CrushTopologyRecord>,
}

#[derive(Serialize)]
struct CrushTopologyRecord {
    path: String,
    fault_zone_type: String,
    end_node_type: String,
}

#[derive(Serialize)]
struct TransitionLimitRecord {
    scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    instance: Option<String>,
    rebalance_type: &'static str,
    max_in_flight: usize,
}

impl From<TransitionLimitSpec> for TransitionLimitRecord {
    fn from(limit: TransitionLimitSpec) -> Self {
        let (scope, resource, instance) = match limit.target {
            TransitionLimitTarget::Cluster => ("CLUSTER", None, None),
            TransitionLimitTarget::Resource(resource) => {
                ("RESOURCE", Some(resource.to_string()), None)
            }
            TransitionLimitTarget::Instance(instance) => {
                ("INSTANCE", None, Some(instance.to_string()))
            }
        };
        let rebalance_type = match limit.rebalance_type {
            TransitionRebalanceType::Any => "ANY",
            TransitionRebalanceType::RecoveryBalance => "RECOVERY_BALANCE",
            TransitionRebalanceType::LoadBalance => "LOAD_BALANCE",
        };
        Self {
            scope,
            resource,
            instance,
            rebalance_type,
            max_in_flight: limit.max_in_flight,
        }
    }
}
