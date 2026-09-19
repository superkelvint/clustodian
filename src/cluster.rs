//! Application-facing composition and configuration facade.

use crate::admin::{
    ClusterAdmin, CrushTopologySpec, PlacementSpec as LowLevelPlacementSpec, TransitionLimitSpec,
    TransitionLimitTarget, TransitionRebalanceType,
};
use crate::coordination::etcd::{CoordinationError, EtcdConnectionOptions, EtcdCoordination};
use crate::model::{InstanceId, ResourceId};
use crate::observability::{RuntimeEvent, RuntimeEventHook};
use crate::observe::{Observer, WaitError};
use crate::participant::{
    erase_resource_handler, ParticipantRuntime, ParticipantRuntimeError, ResourceHandler,
};
use crate::routing::RoutingError;
use crate::runtime::{ControllerRuntime, ControllerRuntimeConfig};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:2379";
const DEFAULT_CLUSTER: &str = "default";
const DEFAULT_ZONE: &str = "default";
const DEFAULT_CONTROLLER_LEASE_TTL_MS: u64 = 60_000;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn lease_ttl_millis(lease_ttl: Duration) -> Result<u64, ApplicationError> {
    u64::try_from(lease_ttl.as_millis()).map_err(|_| {
        ApplicationError::Config(ConfigError::Invalid(
            "controller lease TTL is too large".to_owned(),
        ))
    })
}

/// Errors raised while loading or validating application configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    Missing(String),
    Invalid(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing(name) => write!(formatter, "missing configuration value {name}"),
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Errors raised by the application facade.
#[derive(Debug)]
pub enum ApplicationError {
    Config(ConfigError),
    Coordination(CoordinationError),
    Runtime(crate::Error),
    Spec(String),
    Participant(ParticipantRuntimeError),
    Controller(crate::Error),
    Callback(BoxError),
    Signal(std::io::Error),
    Routing(RoutingError),
    Wait(WaitError),
}

impl fmt::Display for ApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(error) => error.fmt(formatter),
            Self::Coordination(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
            Self::Spec(error) => formatter.write_str(error),
            Self::Participant(error) => write!(formatter, "participant runtime error: {error}"),
            Self::Controller(error) => write!(formatter, "controller runtime error: {error}"),
            Self::Callback(error) => write!(formatter, "application callback failed: {error}"),
            Self::Signal(error) => write!(formatter, "signal handler setup failed: {error}"),
            Self::Routing(error) => error.fmt(formatter),
            Self::Wait(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ApplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::Coordination(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Callback(error) => Some(error.as_ref()),
            Self::Signal(error) => Some(error),
            Self::Routing(error) => Some(error),
            Self::Wait(error) => Some(error),
            Self::Participant(error) => Some(error),
            Self::Controller(error) => Some(error),
            Self::Spec(_) => None,
        }
    }
}

impl From<ConfigError> for ApplicationError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<CoordinationError> for ApplicationError {
    fn from(error: CoordinationError) -> Self {
        Self::Coordination(error)
    }
}

impl From<crate::Error> for ApplicationError {
    fn from(error: crate::Error) -> Self {
        Self::Runtime(error)
    }
}

impl From<ParticipantRuntimeError> for ApplicationError {
    fn from(error: ParticipantRuntimeError) -> Self {
        Self::Participant(error)
    }
}

impl From<RoutingError> for ApplicationError {
    fn from(error: RoutingError) -> Self {
        Self::Routing(error)
    }
}

impl From<WaitError> for ApplicationError {
    fn from(error: WaitError) -> Self {
        Self::Wait(error)
    }
}

/// Connection settings for one Clustodian cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConfig {
    cluster: String,
    endpoints: Vec<String>,
    namespace: String,
    connection_options: EtcdConnectionOptions,
}

impl ClusterConfig {
    /// Create local-development configuration for a logical cluster.
    pub fn new(cluster: impl Into<String>) -> Self {
        let cluster = cluster.into();
        Self {
            namespace: format!("/clustodian/{cluster}"),
            cluster,
            endpoints: vec![DEFAULT_ENDPOINT.to_owned()],
            connection_options: EtcdConnectionOptions::new(),
        }
    }

    /// Load configuration from `CLUSTODIAN_*` environment variables.
    pub fn from_env() -> Result<Self, ConfigError> {
        let cluster = env::var("CLUSTODIAN_CLUSTER").unwrap_or_else(|_| DEFAULT_CLUSTER.to_owned());
        let endpoints =
            env::var("CLUSTODIAN_ETCD_ENDPOINTS").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_owned());
        let namespace =
            env::var("CLUSTODIAN_NAMESPACE").unwrap_or_else(|_| format!("/clustodian/{cluster}"));
        let mut config = Self::new(cluster)
            .etcd_endpoints(endpoints.split(',').map(str::trim))
            .namespace(namespace);
        let mut options = EtcdConnectionOptions::new();
        if let (Ok(username), Ok(password)) = (
            env::var("CLUSTODIAN_ETCD_USERNAME"),
            env::var("CLUSTODIAN_ETCD_PASSWORD"),
        ) {
            options = options.with_user(username, password);
        } else if env::var("CLUSTODIAN_ETCD_USERNAME").is_ok()
            || env::var("CLUSTODIAN_ETCD_PASSWORD").is_ok()
        {
            return Err(ConfigError::Invalid(
                "CLUSTODIAN_ETCD_USERNAME and CLUSTODIAN_ETCD_PASSWORD must be set together"
                    .to_owned(),
            ));
        }
        if let Some(path) = env::var_os("CLUSTODIAN_ETCD_CA_CERT_FILE") {
            options = options.with_ca_certificate(std::fs::read(path).map_err(|error| {
                ConfigError::Invalid(format!("failed to read etcd CA certificate: {error}"))
            })?);
        }
        let client_certificate = env::var_os("CLUSTODIAN_ETCD_CLIENT_CERT_FILE");
        let client_key = env::var_os("CLUSTODIAN_ETCD_CLIENT_KEY_FILE");
        match (client_certificate, client_key) {
            (Some(certificate), Some(key)) => {
                options = options.with_client_identity(
                    std::fs::read(certificate).map_err(|error| {
                        ConfigError::Invalid(format!(
                            "failed to read etcd client certificate: {error}"
                        ))
                    })?,
                    std::fs::read(key).map_err(|error| {
                        ConfigError::Invalid(format!("failed to read etcd client key: {error}"))
                    })?,
                );
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(ConfigError::Invalid(
                    "CLUSTODIAN_ETCD_CLIENT_CERT_FILE and CLUSTODIAN_ETCD_CLIENT_KEY_FILE must be set together"
                        .to_owned(),
                ));
            }
            (None, None) => {}
        }
        if let Ok(domain) = env::var("CLUSTODIAN_ETCD_TLS_DOMAIN_NAME") {
            options = options.with_tls_domain_name(domain);
        }
        config = config.etcd_connection_options(options);
        config.validate()
    }

    /// Set the ordered etcd endpoints used for client failover.
    pub fn etcd_endpoints<I, S>(mut self, endpoints: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.endpoints = endpoints
            .into_iter()
            .map(|endpoint| endpoint.as_ref().to_owned())
            .collect();
        self
    }

    /// Set the coordination namespace, called a prefix by the backend API.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Set typed etcd transport, TLS, and authentication options.
    pub fn etcd_connection_options(mut self, options: EtcdConnectionOptions) -> Self {
        self.connection_options = options;
        self
    }

    /// Alias for [`Self::etcd_connection_options`].
    pub fn etcd_options(self, options: EtcdConnectionOptions) -> Self {
        self.etcd_connection_options(options)
    }

    /// Return the logical cluster name.
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// Return the configured etcd endpoints.
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// Return the configured coordination namespace.
    pub fn namespace_value(&self) -> &str {
        &self.namespace
    }

    /// Return the typed etcd connection options.
    pub fn connection_options(&self) -> &EtcdConnectionOptions {
        &self.connection_options
    }

    fn validate(self) -> Result<Self, ConfigError> {
        if self.cluster.trim().is_empty() || self.cluster.as_bytes().contains(&0) {
            return Err(ConfigError::Invalid(
                "cluster name must not be empty or contain NUL".to_owned(),
            ));
        }
        if self.endpoints.is_empty()
            || self
                .endpoints
                .iter()
                .any(|endpoint| endpoint.trim().is_empty())
        {
            return Err(ConfigError::Invalid(
                "at least one non-empty etcd endpoint is required".to_owned(),
            ));
        }
        if self.namespace.trim().is_empty() || self.namespace.as_bytes().contains(&0) {
            return Err(ConfigError::Invalid(
                "namespace must not be empty or contain NUL".to_owned(),
            ));
        }
        self.connection_options
            .validate()
            .map_err(ConfigError::Invalid)?;
        Ok(self)
    }
}

/// A resource state model supported by the application API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateModel {
    LeaderStandby,
}

/// An instance declaration in a cluster specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstanceSpec {
    instance_id: String,
    zone: String,
}

impl InstanceSpec {
    /// Declare an instance in the default zone.
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
            zone: DEFAULT_ZONE.to_owned(),
        }
    }

    /// Set the failure-domain zone for this instance.
    pub fn zone(mut self, zone: impl Into<String>) -> Self {
        self.zone = zone.into();
        self
    }

    fn into_low_level(self) -> crate::admin::InstanceSpec {
        crate::admin::InstanceSpec {
            instance_id: self.instance_id,
            zone: self.zone,
        }
    }
}

impl From<&str> for InstanceSpec {
    fn from(instance_id: &str) -> Self {
        Self::new(instance_id)
    }
}

impl From<String> for InstanceSpec {
    fn from(instance_id: String) -> Self {
        Self::new(instance_id)
    }
}

/// A topology for topology-aware CRUSH placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Topology {
    path: String,
    fault_zone_type: String,
    end_node_type: String,
}

impl Topology {
    /// The conventional `/zone/instance` topology.
    pub fn zones() -> Self {
        Self {
            path: "/zone/instance".to_owned(),
            fault_zone_type: "zone".to_owned(),
            end_node_type: "instance".to_owned(),
        }
    }

    /// Create a topology with explicit path and node types.
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

    fn into_low_level(self) -> CrushTopologySpec {
        CrushTopologySpec::new(self.path, self.fault_zone_type, self.end_node_type)
    }
}

/// Placement strategy for an application resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Placement {
    Crush,
    CrushWithTopology(Topology),
    SemiAuto(BTreeMap<String, Vec<String>>),
}

impl Placement {
    /// Begin a CRUSH placement builder.
    pub fn crush() -> PlacementBuilder {
        PlacementBuilder
    }

    /// Create semi-auto placement from explicit preference lists.
    pub fn semi_auto(preference_lists: BTreeMap<String, Vec<String>>) -> Self {
        Self::SemiAuto(preference_lists)
    }

    fn into_low_level(self) -> LowLevelPlacementSpec {
        match self {
            Self::Crush => LowLevelPlacementSpec::Crush,
            Self::CrushWithTopology(topology) => LowLevelPlacementSpec::CrushWithTopology {
                topology: topology.into_low_level(),
            },
            Self::SemiAuto(preference_lists) => {
                LowLevelPlacementSpec::SemiAuto { preference_lists }
            }
        }
    }
}

/// Builder for CRUSH placement variants.
#[derive(Clone, Copy, Debug, Default)]
pub struct PlacementBuilder;

impl PlacementBuilder {
    /// Use CRUSH without an explicit topology.
    pub fn build(self) -> Placement {
        Placement::Crush
    }

    /// Use topology-aware CRUSH placement.
    pub fn topology(self, topology: Topology) -> Placement {
        Placement::CrushWithTopology(topology)
    }
}

/// A resource declaration in a cluster specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceSpec {
    name: String,
    partitions: usize,
    replicas: usize,
    state_model: StateModel,
    placement: Placement,
}

impl ResourceSpec {
    /// Declare a Leader/Standby resource.
    pub fn leader_standby(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partitions: 0,
            replicas: 0,
            state_model: StateModel::LeaderStandby,
            placement: Placement::Crush,
        }
    }

    /// Set the number of partitions.
    pub fn partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }

    /// Set the replication factor.
    pub fn replicas(mut self, replicas: usize) -> Self {
        self.replicas = replicas;
        self
    }

    /// Set the placement strategy.
    pub fn placement(mut self, placement: Placement) -> Self {
        self.placement = placement;
        self
    }

    fn into_low_level(self) -> crate::admin::ResourceSpec {
        crate::admin::ResourceSpec {
            name: self.name,
            partitions: self.partitions,
            replicas: self.replicas,
            state_model: match self.state_model {
                StateModel::LeaderStandby => "LeaderStandby".to_owned(),
            },
            placement: self.placement.into_low_level(),
        }
    }
}

/// A declarative, non-destructive cluster specification.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterSpec {
    instances: Vec<InstanceSpec>,
    resources: Vec<ResourceSpec>,
}

/// The class of transitions counted by a [`TransitionLimit`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionType {
    /// Count all state transitions.
    Any,
    /// Count recovery-balance transitions.
    RecoveryBalance,
    /// Count load-balance transitions.
    LoadBalance,
}

/// A typed limit on state-transition concurrency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionLimit {
    target: TransitionLimitTargetSpec,
    transition_type: TransitionType,
    max_in_flight: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TransitionLimitTargetSpec {
    Cluster,
    Resource(String),
    Instance(String),
}

impl TransitionLimit {
    /// Limit all state transitions across the cluster.
    pub fn cluster(max_in_flight: usize) -> Self {
        Self {
            target: TransitionLimitTargetSpec::Cluster,
            transition_type: TransitionType::Any,
            max_in_flight,
        }
    }

    /// Limit all state transitions for one resource.
    pub fn resource(resource: impl Into<String>, max_in_flight: usize) -> Self {
        Self {
            target: TransitionLimitTargetSpec::Resource(resource.into()),
            transition_type: TransitionType::Any,
            max_in_flight,
        }
    }

    /// Limit all state transitions for one instance.
    pub fn instance(instance: impl Into<String>, max_in_flight: usize) -> Self {
        Self {
            target: TransitionLimitTargetSpec::Instance(instance.into()),
            transition_type: TransitionType::Any,
            max_in_flight,
        }
    }

    /// Count only one class of state transitions.
    pub fn transition_type(mut self, transition_type: TransitionType) -> Self {
        self.transition_type = transition_type;
        self
    }

    fn into_low_level(self) -> Result<TransitionLimitSpec, ApplicationError> {
        if self.max_in_flight == 0 {
            return Err(ApplicationError::Spec(
                "transition limit must be greater than zero".to_owned(),
            ));
        }
        let target = match self.target {
            TransitionLimitTargetSpec::Cluster => TransitionLimitTarget::Cluster,
            TransitionLimitTargetSpec::Resource(resource) => TransitionLimitTarget::Resource(
                ResourceId::new(resource)
                    .map_err(|error| ApplicationError::Spec(error.to_string()))?,
            ),
            TransitionLimitTargetSpec::Instance(instance) => TransitionLimitTarget::Instance(
                InstanceId::new(instance)
                    .map_err(|error| ApplicationError::Spec(error.to_string()))?,
            ),
        };
        let rebalance_type = match self.transition_type {
            TransitionType::Any => TransitionRebalanceType::Any,
            TransitionType::RecoveryBalance => TransitionRebalanceType::RecoveryBalance,
            TransitionType::LoadBalance => TransitionRebalanceType::LoadBalance,
        };
        Ok(TransitionLimitSpec {
            target,
            rebalance_type,
            max_in_flight: self.max_in_flight,
        })
    }
}

impl ClusterSpec {
    /// Create an empty specification.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add instance declarations. Omitted existing instances are retained.
    pub fn instances<I, T>(mut self, instances: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<InstanceSpec>,
    {
        self.instances.extend(instances.into_iter().map(Into::into));
        self
    }

    /// Add a resource declaration.
    pub fn resource(mut self, resource: ResourceSpec) -> Self {
        self.resources.push(resource);
        self
    }

    fn validate(&self) -> Result<(), ApplicationError> {
        let mut instances = BTreeMap::new();
        for instance in &self.instances {
            let id = InstanceId::new(instance.instance_id.clone())
                .map_err(|error| ApplicationError::Spec(error.to_string()))?;
            if instance.zone.trim().is_empty() || instance.zone.as_bytes().contains(&0) {
                return Err(ApplicationError::Spec(format!(
                    "instance {id}: zone must not be empty or contain NUL"
                )));
            }
            if instances.insert(id.clone(), ()).is_some() {
                return Err(ApplicationError::Spec(format!(
                    "instance {id} is declared more than once"
                )));
            }
        }
        let mut resources = BTreeMap::new();
        for resource in &self.resources {
            let id = ResourceId::new(resource.name.clone())
                .map_err(|error| ApplicationError::Spec(error.to_string()))?;
            if resource.partitions == 0 || resource.replicas == 0 {
                return Err(ApplicationError::Spec(format!(
                    "resource {id}: partitions and replicas must be greater than zero"
                )));
            }
            if resources.insert(id.clone(), ()).is_some() {
                return Err(ApplicationError::Spec(format!(
                    "resource {id} is declared more than once"
                )));
            }
            if let Placement::SemiAuto(preference_lists) = &resource.placement {
                if preference_lists.len() != resource.partitions
                    || preference_lists
                        .values()
                        .any(|instances| instances.len() != resource.replicas)
                {
                    return Err(ApplicationError::Spec(format!(
                        "resource {id}: semi-auto lists must match partitions and replicas"
                    )));
                }
                if preference_lists.values().any(|instances| {
                    instances.iter().collect::<BTreeSet<_>>().len() != instances.len()
                }) {
                    return Err(ApplicationError::Spec(format!(
                        "resource {id}: semi-auto preference lists must not contain duplicate instances"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// A connected application-level cluster handle.
#[derive(Clone)]
pub struct Cluster {
    coordination: EtcdCoordination,
}

impl Cluster {
    /// Connect to the configured etcd coordination namespace.
    pub async fn connect(config: ClusterConfig) -> Result<Self, ApplicationError> {
        let config = config.validate().map_err(ApplicationError::Config)?;
        let coordination = EtcdCoordination::connect_endpoints_with_options(
            config.endpoints,
            config.namespace,
            config.cluster,
            config.connection_options,
        )
        .await?;
        ClusterAdmin::new(coordination.clone())
            .ensure_cluster(coordination.cluster())
            .await?;
        Ok(Self { coordination })
    }

    /// Return an application administrator.
    pub fn admin(&self) -> Admin {
        Admin {
            coordination: self.coordination.clone(),
        }
    }

    /// Build a controller process for one controller identity.
    pub fn controller(&self, controller_id: impl Into<String>) -> ControllerBuilder {
        ControllerBuilder {
            coordination: self.coordination.clone(),
            controller_id: controller_id.into(),
            lease_ttl: Duration::from_millis(DEFAULT_CONTROLLER_LEASE_TTL_MS),
            ready: None,
            event_hook: None,
        }
    }

    /// Build a participant process for one instance identity.
    pub fn participant(&self, instance_id: impl Into<String>) -> ParticipantBuilder {
        ParticipantBuilder {
            coordination: self.coordination.clone(),
            instance_id: instance_id.into(),
            handlers: Vec::new(),
            lease_ttl: Duration::from_secs(60),
            ready: None,
            event_hook: None,
        }
    }

    /// Return a read-only observer.
    pub fn observer(&self) -> Observer {
        Observer::new(self.coordination.clone())
    }
}

/// Application-facing administration facade.
#[derive(Clone)]
pub struct Admin {
    coordination: EtcdCoordination,
}

impl Admin {
    /// Apply a validated, idempotent, non-destructive specification.
    pub async fn apply(&self, spec: ClusterSpec) -> Result<(), ApplicationError> {
        spec.validate()?;
        let admin = ClusterAdmin::new(self.coordination.clone());
        admin.ensure_cluster(self.coordination.cluster()).await?;
        for instance in spec.instances {
            admin.put_instance(instance.into_low_level()).await?;
        }
        for resource in spec.resources {
            admin.put_resource(resource.into_low_level()).await?;
        }
        Ok(())
    }

    /// Explicitly remove one configured instance.
    pub async fn remove_instance(
        &self,
        instance_id: impl AsRef<str>,
    ) -> Result<(), ApplicationError> {
        ClusterAdmin::new(self.coordination.clone())
            .remove_instance(instance_id.as_ref())
            .await?;
        Ok(())
    }

    /// Explicitly remove one configured resource.
    pub async fn remove_resource(
        &self,
        resource_id: impl AsRef<str>,
    ) -> Result<(), ApplicationError> {
        ClusterAdmin::new(self.coordination.clone())
            .remove_resource(resource_id.as_ref())
            .await?;
        Ok(())
    }

    /// Replace the typed state-transition concurrency limits.
    pub async fn set_transition_limits<I>(&self, limits: I) -> Result<(), ApplicationError>
    where
        I: IntoIterator<Item = TransitionLimit>,
    {
        let limits = limits
            .into_iter()
            .map(TransitionLimit::into_low_level)
            .collect::<Result<Vec<_>, _>>()?;
        ClusterAdmin::new(self.coordination.clone())
            .put_transition_limits(limits)
            .await?;
        Ok(())
    }
}

/// Builder for a controller runtime.
pub struct ControllerBuilder {
    coordination: EtcdCoordination,
    controller_id: String,
    lease_ttl: Duration,
    ready: Option<Box<dyn FnOnce() -> Result<(), BoxError> + Send + Sync>>,
    event_hook: Option<RuntimeEventHook>,
}

impl ControllerBuilder {
    /// Set the controller election lease duration.
    pub fn lease_ttl(mut self, lease_ttl: Duration) -> Self {
        self.lease_ttl = lease_ttl;
        self
    }

    /// Set a one-shot callback invoked after the initial controller commit.
    pub fn on_ready<F, E>(mut self, callback: F) -> Self
    where
        F: FnOnce() -> Result<(), E> + Send + Sync + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.ready = Some(Box::new(move || {
            callback().map_err(|error| Box::new(error) as BoxError)
        }));
        self
    }

    /// Register a callback for controller lifecycle and recovery events.
    pub fn on_event<F>(mut self, callback: F) -> Self
    where
        F: Fn(RuntimeEvent) + Send + Sync + 'static,
    {
        self.event_hook = Some(std::sync::Arc::new(callback));
        self
    }

    /// Run until the supplied future resolves.
    pub async fn run_until<F>(self, shutdown: F) -> Result<(), ApplicationError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let cluster = self.coordination.cluster().to_owned();
        let mut runtime = ControllerRuntime::new(
            self.coordination,
            ControllerRuntimeConfig {
                cluster,
                controller_id: self.controller_id,
                lease_ttl_ms: lease_ttl_millis(self.lease_ttl)?,
            },
        )
        .await
        .map_err(ApplicationError::Controller)?;
        let callback_error = Arc::new(Mutex::new(None));
        if let Some(ready) = self.ready {
            let callback_error_for_runtime = callback_error.clone();
            runtime = runtime.on_ready(move || {
                ready().map_err(|error| {
                    *callback_error_for_runtime
                        .lock()
                        .expect("callback error mutex is not poisoned") = Some(error);
                    std::io::Error::other("application ready callback failed")
                })
            });
        }
        if let Some(event_hook) = self.event_hook {
            runtime = runtime.on_event(move |event| event_hook(event));
        }
        let runtime_result = runtime.run_until(shutdown).await;
        if let Some(error) = callback_error
            .lock()
            .expect("callback error mutex is not poisoned")
            .take()
        {
            return Err(ApplicationError::Callback(error));
        }
        runtime_result.map_err(ApplicationError::Controller)?;
        Ok(())
    }

    /// Run until Ctrl-C or SIGTERM.
    pub async fn run_until_signal(self) -> Result<(), ApplicationError> {
        #[cfg(unix)]
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(ApplicationError::Signal)?;
        let signal_error = Arc::new(Mutex::new(None));
        let signal_error_for_shutdown = signal_error.clone();
        let shutdown = async move {
            #[cfg(unix)]
            {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            *signal_error_for_shutdown
                                .lock()
                                .expect("signal error mutex is not poisoned") = Some(error);
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            #[cfg(not(unix))]
            {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    *signal_error_for_shutdown
                        .lock()
                        .expect("signal error mutex is not poisoned") = Some(error);
                }
            }
        };
        let result = self.run_until(shutdown).await;
        if let Some(error) = signal_error
            .lock()
            .expect("signal error mutex is not poisoned")
            .take()
        {
            return Err(ApplicationError::Signal(error));
        }
        result
    }
}

/// Builder for a resource-scoped participant runtime.
pub struct ParticipantBuilder {
    coordination: EtcdCoordination,
    instance_id: String,
    handlers: Vec<(String, Box<dyn crate::participant::ErasedResourceHandler>)>,
    lease_ttl: Duration,
    ready: Option<Box<dyn FnOnce() -> Result<(), BoxError> + Send>>,
    event_hook: Option<RuntimeEventHook>,
}

impl ParticipantBuilder {
    /// Register a transition handler for one resource.
    pub fn resource<H>(mut self, resource: impl Into<String>, handler: H) -> Self
    where
        H: ResourceHandler,
    {
        self.handlers
            .push((resource.into(), erase_resource_handler(handler)));
        self
    }

    /// Set the participant lease duration.
    pub fn lease_ttl(mut self, lease_ttl: Duration) -> Self {
        self.lease_ttl = lease_ttl;
        self
    }

    /// Set a one-shot callback invoked after registration and queued work.
    pub fn on_ready<F, E>(mut self, callback: F) -> Self
    where
        F: FnOnce() -> Result<(), E> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.ready = Some(Box::new(move || {
            callback().map_err(|error| Box::new(error) as BoxError)
        }));
        self
    }

    /// Register a callback for participant lifecycle and recovery events.
    pub fn on_event<F>(mut self, callback: F) -> Self
    where
        F: Fn(RuntimeEvent) + Send + Sync + 'static,
    {
        self.event_hook = Some(std::sync::Arc::new(callback));
        self
    }

    /// Run until the supplied future resolves.
    pub async fn run_until<F>(self, shutdown: F) -> Result<(), ApplicationError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let instance = InstanceId::new(self.instance_id)
            .map_err(|error| ApplicationError::Spec(error.to_string()))?;
        let mut handlers = BTreeMap::new();
        for (resource, handler) in self.handlers {
            let resource_id = ResourceId::new(resource)
                .map_err(|error| ApplicationError::Spec(error.to_string()))?;
            if handlers.insert(resource_id, handler).is_some() {
                return Err(ApplicationError::Spec(
                    "participant resource handler declared more than once".to_owned(),
                ));
            }
        }
        let mut runtime =
            ParticipantRuntime::<crate::participant::AsyncScopedResourceHandler>::new_async_scoped(
                self.coordination,
                instance,
                handlers,
            )
            .with_lease_ttl(self.lease_ttl)?;
        if let Some(event_hook) = self.event_hook {
            runtime = runtime.on_event(move |event| event_hook(event));
        }
        let callback_error = Arc::new(Mutex::new(None));
        let ready: Box<dyn FnOnce() -> Result<(), ParticipantRuntimeError> + Send> =
            match self.ready {
                Some(ready) => {
                    let callback_error_for_runtime = callback_error.clone();
                    Box::new(move || {
                        ready().map_err(|error| {
                            *callback_error_for_runtime
                                .lock()
                                .expect("callback error mutex is not poisoned") = Some(error);
                            ParticipantRuntimeError::Io(String::from(
                                "application ready callback failed",
                            ))
                        })
                    })
                }
                None => Box::new(|| Ok(())),
            };
        let runtime_result = runtime.run_until(shutdown, ready).await;
        if let Some(error) = callback_error
            .lock()
            .expect("callback error mutex is not poisoned")
            .take()
        {
            return Err(ApplicationError::Callback(error));
        }
        runtime_result?;
        Ok(())
    }

    /// Run until Ctrl-C or SIGTERM.
    pub async fn run_until_signal(self) -> Result<(), ApplicationError> {
        #[cfg(unix)]
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(ApplicationError::Signal)?;
        let signal_error = Arc::new(Mutex::new(None));
        let signal_error_for_shutdown = signal_error.clone();
        let shutdown = async move {
            #[cfg(unix)]
            {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => {
                        if let Err(error) = result {
                            *signal_error_for_shutdown
                                .lock()
                                .expect("signal error mutex is not poisoned") = Some(error);
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            #[cfg(not(unix))]
            {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    *signal_error_for_shutdown
                        .lock()
                        .expect("signal error mutex is not poisoned") = Some(error);
                }
            }
        };
        let result = self.run_until(shutdown).await;
        if let Some(error) = signal_error
            .lock()
            .expect("signal error mutex is not poisoned")
            .take()
        {
            return Err(ApplicationError::Signal(error));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::etcd::CoordinationError;
    use crate::observe::WaitError;
    use crate::routing::RoutingError;
    use std::collections::BTreeMap;
    use std::time::Duration;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn configuration_and_application_errors_have_operator_messages() {
        assert_eq!(
            ConfigError::Missing(String::from("CLUSTODIAN_CLUSTER")).to_string(),
            "missing configuration value CLUSTODIAN_CLUSTER"
        );
        assert_eq!(
            ConfigError::Invalid(String::from("bad configuration")).to_string(),
            "bad configuration"
        );

        let errors = [
            ApplicationError::Config(ConfigError::Invalid(String::from("config"))),
            ApplicationError::Coordination(CoordinationError::InvalidValue),
            ApplicationError::Runtime(crate::Error::new("runtime")),
            ApplicationError::Spec(String::from("spec")),
            ApplicationError::Participant(ParticipantRuntimeError::Io(String::from("participant"))),
            ApplicationError::Controller(crate::Error::new("controller")),
            ApplicationError::Callback(Box::new(std::io::Error::other("callback"))),
            ApplicationError::Signal(std::io::Error::other("signal")),
            ApplicationError::Routing(RoutingError::InvalidResource(String::from("resource"))),
            ApplicationError::Wait(WaitError::Timeout(Duration::from_secs(1))),
        ];
        let messages = errors.iter().map(ToString::to_string).collect::<Vec<_>>();
        assert_eq!(
            messages,
            vec![
                "config",
                "coordination value is invalid UTF-8",
                "runtime",
                "spec",
                "participant runtime error: participant",
                "controller runtime error: controller",
                "application callback failed: callback",
                "signal handler setup failed: signal",
                "invalid resource \"resource\"",
                "observation timed out after 1s",
            ]
        );
        let callback_error =
            ApplicationError::Callback(Box::new(std::io::Error::other("callback")));
        assert_eq!(
            std::error::Error::source(&callback_error)
                .expect("callback source is preserved")
                .to_string(),
            "callback"
        );

        let source_errors = [
            ApplicationError::Config(ConfigError::Invalid(String::from("config"))),
            ApplicationError::Coordination(CoordinationError::InvalidValue),
            ApplicationError::Runtime(crate::Error::new("runtime")),
            ApplicationError::Participant(ParticipantRuntimeError::Io(String::from("participant"))),
            ApplicationError::Controller(crate::Error::new("controller")),
            ApplicationError::Callback(Box::new(std::io::Error::other("callback"))),
            ApplicationError::Signal(std::io::Error::other("signal")),
            ApplicationError::Routing(RoutingError::InvalidResource(String::from("resource"))),
            ApplicationError::Wait(WaitError::Timeout(Duration::from_secs(1))),
        ];
        for error in source_errors {
            assert!(std::error::Error::source(&error).is_some());
        }
        assert!(std::error::Error::source(&ApplicationError::Spec(String::from("spec"))).is_none());
        assert!(matches!(
            ApplicationError::from(ConfigError::Invalid(String::from("config"))),
            ApplicationError::Config(_)
        ));
        assert!(matches!(
            ApplicationError::from(CoordinationError::InvalidValue),
            ApplicationError::Coordination(_)
        ));
        assert!(matches!(
            ApplicationError::from(crate::Error::new("runtime")),
            ApplicationError::Runtime(_)
        ));
        assert!(matches!(
            ApplicationError::from(ParticipantRuntimeError::InvalidMessage),
            ApplicationError::Participant(_)
        ));
        assert!(matches!(
            ApplicationError::from(RoutingError::InvalidResource(String::from("resource"))),
            ApplicationError::Routing(_)
        ));
        assert!(matches!(
            ApplicationError::from(WaitError::Timeout(Duration::from_secs(1))),
            ApplicationError::Wait(_)
        ));
    }

    #[test]
    fn controller_lease_ttl_conversion_is_checked() {
        assert_eq!(
            lease_ttl_millis(Duration::from_millis(1_500)).unwrap(),
            1_500
        );
        assert!(lease_ttl_millis(Duration::from_secs(u64::MAX)).is_err());
    }

    #[test]
    fn configuration_validation_rejects_empty_values() {
        let invalid = [
            ClusterConfig {
                cluster: String::new(),
                endpoints: vec![String::from("http://etcd")],
                namespace: String::from("/namespace"),
                connection_options: EtcdConnectionOptions::new(),
            },
            ClusterConfig {
                cluster: String::from("cluster"),
                endpoints: Vec::new(),
                namespace: String::from("/namespace"),
                connection_options: EtcdConnectionOptions::new(),
            },
            ClusterConfig {
                cluster: String::from("cluster"),
                endpoints: vec![String::from(" ")],
                namespace: String::from("/namespace"),
                connection_options: EtcdConnectionOptions::new(),
            },
            ClusterConfig {
                cluster: String::from("cluster"),
                endpoints: vec![String::from("http://etcd")],
                namespace: String::new(),
                connection_options: EtcdConnectionOptions::new(),
            },
        ];
        for config in invalid {
            assert!(config.validate().is_err());
        }

        assert!(ClusterConfig {
            cluster: String::from("bad\0cluster"),
            endpoints: vec![String::from("http://etcd")],
            namespace: String::from("/namespace"),
            connection_options: EtcdConnectionOptions::new(),
        }
        .validate()
        .is_err());
        assert!(ClusterConfig {
            cluster: String::from("cluster"),
            endpoints: vec![String::from("http://etcd")],
            namespace: String::from("bad\0namespace"),
            connection_options: EtcdConnectionOptions::new(),
        }
        .validate()
        .is_err());
    }

    #[test]
    fn application_builders_convert_all_supported_variants() {
        let instance: InstanceSpec = "node-a".into();
        assert_eq!(instance.zone, DEFAULT_ZONE);
        let instance: InstanceSpec = String::from("node-b").into();
        assert_eq!(instance.zone, DEFAULT_ZONE);
        assert_eq!(instance.zone(String::from("zone-b")).zone, "zone-b");

        assert_eq!(Topology::zones().path, "/zone/instance");
        let topology = Topology::new("/rack/instance", "rack", "instance");
        let low_level = topology.clone().into_low_level();
        assert_eq!(low_level.path, "/rack/instance");
        assert_eq!(low_level.fault_zone_type, "rack");
        assert_eq!(low_level.end_node_type, "instance");

        assert_eq!(Placement::crush().build(), Placement::Crush);
        assert_eq!(
            Placement::crush().topology(topology),
            Placement::CrushWithTopology(Topology::new("/rack/instance", "rack", "instance"))
        );
        let preferences = BTreeMap::from([(String::from("p0"), vec![String::from("node-a")])]);
        assert_eq!(
            Placement::semi_auto(preferences.clone()).into_low_level(),
            LowLevelPlacementSpec::SemiAuto {
                preference_lists: preferences,
            }
        );
        assert_eq!(
            Placement::CrushWithTopology(Topology::zones()).into_low_level(),
            LowLevelPlacementSpec::CrushWithTopology {
                topology: CrushTopologySpec::new("/zone/instance", "zone", "instance"),
            }
        );

        let resource = ResourceSpec::leader_standby("documents")
            .partitions(2)
            .replicas(2);
        let low_level = resource.clone().into_low_level();
        assert_eq!(low_level.name, "documents");
        assert_eq!(low_level.partitions, 2);
        assert_eq!(low_level.replicas, 2);
        assert_eq!(low_level.state_model, "LeaderStandby");
        assert_eq!(resource.state_model, StateModel::LeaderStandby);
    }

    #[test]
    fn typed_transition_limits_convert_without_stringly_inputs() {
        let cluster = TransitionLimit::cluster(10).into_low_level().unwrap();
        assert_eq!(cluster.target, crate::admin::TransitionLimitTarget::Cluster);
        assert_eq!(
            cluster.rebalance_type,
            crate::admin::TransitionRebalanceType::Any
        );
        assert_eq!(cluster.max_in_flight, 10);

        let resource = TransitionLimit::resource("database", 2)
            .transition_type(TransitionType::LoadBalance)
            .into_low_level()
            .unwrap();
        assert_eq!(
            resource.target,
            crate::admin::TransitionLimitTarget::Resource(ResourceId::new("database").unwrap())
        );
        assert_eq!(
            resource.rebalance_type,
            crate::admin::TransitionRebalanceType::LoadBalance
        );
        assert!(TransitionLimit::resource("", 2).into_low_level().is_err());
        assert!(TransitionLimit::cluster(0).into_low_level().is_err());
    }

    #[test]
    fn config_builder_has_explicit_values() {
        let config = ClusterConfig::new("cache")
            .etcd_endpoints(["http://one:2379", "http://two:2379"])
            .namespace("/example/cache");
        assert_eq!(config.cluster(), "cache");
        assert_eq!(config.endpoints().len(), 2);
        assert_eq!(config.namespace_value(), "/example/cache");
        assert!(config.clone().validate().is_ok());
        assert_eq!(ClusterSpec::new(), ClusterSpec::default());
    }

    #[test]
    fn loads_configuration_from_environment() {
        let _lock = ENV_LOCK
            .lock()
            .expect("environment test lock is not poisoned");
        let previous = [
            (
                "CLUSTODIAN_CLUSTER",
                std::env::var("CLUSTODIAN_CLUSTER").ok(),
            ),
            (
                "CLUSTODIAN_ETCD_ENDPOINTS",
                std::env::var("CLUSTODIAN_ETCD_ENDPOINTS").ok(),
            ),
            (
                "CLUSTODIAN_NAMESPACE",
                std::env::var("CLUSTODIAN_NAMESPACE").ok(),
            ),
            (
                "CLUSTODIAN_ETCD_USERNAME",
                std::env::var("CLUSTODIAN_ETCD_USERNAME").ok(),
            ),
            (
                "CLUSTODIAN_ETCD_PASSWORD",
                std::env::var("CLUSTODIAN_ETCD_PASSWORD").ok(),
            ),
            (
                "CLUSTODIAN_ETCD_CA_CERT_FILE",
                std::env::var("CLUSTODIAN_ETCD_CA_CERT_FILE").ok(),
            ),
            (
                "CLUSTODIAN_ETCD_CLIENT_CERT_FILE",
                std::env::var("CLUSTODIAN_ETCD_CLIENT_CERT_FILE").ok(),
            ),
            (
                "CLUSTODIAN_ETCD_CLIENT_KEY_FILE",
                std::env::var("CLUSTODIAN_ETCD_CLIENT_KEY_FILE").ok(),
            ),
            (
                "CLUSTODIAN_ETCD_TLS_DOMAIN_NAME",
                std::env::var("CLUSTODIAN_ETCD_TLS_DOMAIN_NAME").ok(),
            ),
        ];
        std::env::set_var("CLUSTODIAN_CLUSTER", "from-env");
        std::env::set_var("CLUSTODIAN_ETCD_ENDPOINTS", " http://one ,http://two ");
        std::env::set_var("CLUSTODIAN_NAMESPACE", "/custom/namespace");
        let config = ClusterConfig::from_env().expect("environment configuration is valid");
        assert_eq!(config.cluster(), "from-env");
        assert_eq!(config.endpoints(), ["http://one", "http://two"]);
        assert_eq!(config.namespace_value(), "/custom/namespace");
        for (name, value) in previous {
            if let Some(value) = value {
                std::env::set_var(name, value);
            } else {
                std::env::remove_var(name);
            }
        }
    }

    #[test]
    fn environment_configuration_uses_defaults_and_rejects_invalid_values() {
        let _lock = ENV_LOCK
            .lock()
            .expect("environment test lock is not poisoned");
        let names = [
            "CLUSTODIAN_CLUSTER",
            "CLUSTODIAN_ETCD_ENDPOINTS",
            "CLUSTODIAN_NAMESPACE",
            "CLUSTODIAN_ETCD_USERNAME",
            "CLUSTODIAN_ETCD_PASSWORD",
            "CLUSTODIAN_ETCD_CA_CERT_FILE",
            "CLUSTODIAN_ETCD_CLIENT_CERT_FILE",
            "CLUSTODIAN_ETCD_CLIENT_KEY_FILE",
            "CLUSTODIAN_ETCD_TLS_DOMAIN_NAME",
        ];
        let previous = names
            .iter()
            .map(|name| (*name, std::env::var(name).ok()))
            .collect::<Vec<_>>();
        for name in names {
            std::env::remove_var(name);
        }
        let defaults = ClusterConfig::from_env().expect("defaults are valid");
        assert_eq!(defaults.cluster(), DEFAULT_CLUSTER);
        assert_eq!(defaults.endpoints(), [DEFAULT_ENDPOINT.to_owned()]);
        assert_eq!(defaults.namespace_value(), "/clustodian/default");

        std::env::set_var("CLUSTODIAN_ETCD_ENDPOINTS", "http://one,,http://two");
        assert!(ClusterConfig::from_env().is_err());
        assert!(ClusterConfig {
            cluster: String::from("cluster"),
            endpoints: vec![String::from("http://etcd")],
            namespace: String::from("bad\0namespace"),
            connection_options: EtcdConnectionOptions::new(),
        }
        .validate()
        .is_err());
        assert!(ClusterConfig {
            cluster: String::from("bad\0cluster"),
            endpoints: vec![String::from("http://etcd")],
            namespace: String::from("/namespace"),
            connection_options: EtcdConnectionOptions::new(),
        }
        .validate()
        .is_err());
        for (name, value) in previous {
            if let Some(value) = value {
                std::env::set_var(name, value);
            } else {
                std::env::remove_var(name);
            }
        }
    }

    #[test]
    fn cluster_spec_rejects_duplicate_declarations() {
        let spec = ClusterSpec::new()
            .instances([InstanceSpec::new("node-a"), InstanceSpec::new("node-a")])
            .resource(
                ResourceSpec::leader_standby("cache")
                    .partitions(1)
                    .replicas(1),
            );
        assert!(spec.validate().is_err());

        let duplicate_replica = ClusterSpec::new().resource(
            ResourceSpec::leader_standby("cache")
                .partitions(1)
                .replicas(2)
                .placement(Placement::semi_auto(BTreeMap::from([(
                    String::from("cache_0"),
                    vec![String::from("node-a"), String::from("node-a")],
                )]))),
        );
        assert!(duplicate_replica.validate().is_err());
    }

    #[test]
    fn cluster_spec_validates_instance_and_resource_shapes() {
        let invalid_instances = [
            ClusterSpec::new().instances([InstanceSpec {
                instance_id: String::new(),
                zone: String::from("zone-a"),
            }]),
            ClusterSpec::new().instances([InstanceSpec {
                instance_id: String::from("node-a"),
                zone: String::new(),
            }]),
            ClusterSpec::new().instances([InstanceSpec {
                instance_id: String::from("node-a"),
                zone: String::from("bad\0zone"),
            }]),
        ];
        for spec in invalid_instances {
            assert!(spec.validate().is_err());
        }

        for resource in [
            ResourceSpec::leader_standby("documents"),
            ResourceSpec::leader_standby("documents").partitions(1),
            ResourceSpec::leader_standby("documents").replicas(1),
        ] {
            assert!(ClusterSpec::new().resource(resource).validate().is_err());
        }
        assert!(ClusterSpec::new()
            .resource(
                ResourceSpec::leader_standby("documents")
                    .partitions(1)
                    .replicas(1)
            )
            .resource(
                ResourceSpec::leader_standby("documents")
                    .partitions(1)
                    .replicas(1)
            )
            .validate()
            .is_err());
        assert!(ClusterSpec::new()
            .resource(
                ResourceSpec::leader_standby("documents")
                    .partitions(2)
                    .replicas(1)
                    .placement(Placement::semi_auto(BTreeMap::from([(
                        String::from("p0"),
                        vec![String::from("node-a")],
                    )])))
            )
            .validate()
            .is_err());
        assert!(ClusterSpec::new()
            .resource(
                ResourceSpec::leader_standby("documents")
                    .partitions(1)
                    .replicas(2)
                    .placement(Placement::semi_auto(BTreeMap::from([(
                        String::from("p0"),
                        vec![String::from("node-a")],
                    )])))
            )
            .validate()
            .is_err());
        assert!(ClusterSpec::new()
            .instances([InstanceSpec::new("node-a")])
            .resource(
                ResourceSpec::leader_standby("documents")
                    .partitions(1)
                    .replicas(1)
                    .placement(Placement::semi_auto(BTreeMap::from([(
                        String::from("documents_0"),
                        vec![String::from("node-a")],
                    )])))
            )
            .validate()
            .is_ok());
    }

    #[test]
    fn topology_builder_converts_to_application_placement() {
        let resource = ResourceSpec::leader_standby("database")
            .partitions(1)
            .replicas(2)
            .placement(Placement::crush().topology(Topology::zones()));
        assert!(matches!(
            resource.placement,
            Placement::CrushWithTopology(_)
        ));
    }
}
