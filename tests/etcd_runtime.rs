#![cfg(not(feature = "shuttle"))]

use clustodian::admin::{
    ClusterAdmin, CrushTopologySpec, InstanceSpec as AdminInstanceSpec,
    PlacementSpec as AdminPlacementSpec, ResourceSpec as AdminResourceSpec, ThrottleSpec,
};
use clustodian::coordination::etcd::{
    EtcdCoordination, EtcdCoordinationConfig, RegistrationOptions, Revision, WatchError,
    WatchEventKind, WatchRecovery,
};
use clustodian::election::{ControllerElection, ControllerElectionConfig};
use clustodian::model::leader_standby;
use clustodian::model::{InstanceId, PartitionId, ResourceId, SessionId, State};
use clustodian::observe::{ClusterObserver, Observer};
use clustodian::participant::{
    processed_revision_key, ParticipantRuntime, ParticipantRuntimeError, TransitionExecution,
    TransitionHandler, TransitionHandlerError,
};
use clustodian::runtime::{ControllerRuntime, ControllerRuntimeConfig};
use clustodian::transition::TransitionMessage;
use clustodian::RuntimeEvent;
use clustodian::{
    ApplicationError, Cluster, ClusterConfig, ClusterSpec, InstanceSpec, Placement,
    ResourceHandler, ResourceSpec, ResourceTransition, TransitionContext, TransitionError,
    TransitionLimit, TransitionType,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::Barrier;

mod support;

use support::EtcdFixture;

fn instance(name: &str) -> InstanceId {
    InstanceId::new(name).expect("test instance is valid")
}

fn resource(name: &str) -> ResourceId {
    ResourceId::new(name).expect("test resource is valid")
}

fn partition(name: &str) -> PartitionId {
    PartitionId::new(name).expect("test partition is valid")
}

fn state(name: &str) -> State {
    State::try_from(name).expect("test state is valid")
}

fn states(entries: &[(&str, &str)]) -> BTreeMap<PartitionId, State> {
    entries
        .iter()
        .map(|(partition_id, state_name)| (partition(partition_id), state(state_name)))
        .collect()
}

fn encoded_test_segment(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn lease_ttl(endpoint: &str, lease_id: i64) -> i64 {
    let client = etcd_client::Client::connect([endpoint], None)
        .await
        .expect("etcd lease TTL client connects");
    client
        .lease_client()
        .time_to_live(lease_id, None)
        .await
        .expect("etcd returns lease TTL")
        .ttl()
}

#[tokio::test]
async fn coordination_connection_rejects_invalid_inputs_before_network() {
    let invalid_configs = [
        EtcdCoordinationConfig {
            endpoint: String::from("http://127.0.0.1:1"),
            prefix: String::new(),
            cluster: String::from("test"),
        },
        EtcdCoordinationConfig {
            endpoint: String::from("http://127.0.0.1:1"),
            prefix: String::from("/clustodian/test"),
            cluster: String::new(),
        },
        EtcdCoordinationConfig {
            endpoint: String::from("http://127.0.0.1:1"),
            prefix: String::from("/clustodian/test"),
            cluster: String::from("bad\0cluster"),
        },
        EtcdCoordinationConfig {
            endpoint: String::from("http://127.0.0.1:1"),
            prefix: String::from("/clustodian/test"),
            cluster: String::from("test"),
        },
    ];
    for (index, config) in invalid_configs.into_iter().enumerate() {
        let result = if index == 3 {
            EtcdCoordination::connect_endpoints(
                std::iter::empty::<&str>(),
                config.prefix,
                config.cluster,
            )
            .await
        } else {
            EtcdCoordination::connect(config).await
        };
        assert!(matches!(
            result,
            Err(clustodian::coordination::etcd::CoordinationError::InvalidPrefix)
                | Err(clustodian::coordination::etcd::CoordinationError::InvalidCluster)
                | Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
        ));
    }
}

#[tokio::test]
async fn public_cluster_facade_rejects_invalid_configuration_before_connecting() {
    let invalid_configs = [
        ClusterConfig::new("").etcd_endpoints(["http://127.0.0.1:1"]),
        ClusterConfig::new("test").etcd_endpoints(Vec::<String>::new()),
        ClusterConfig::new("test").etcd_endpoints([" "]),
        ClusterConfig::new("test").namespace(""),
        ClusterConfig::new("bad\0cluster"),
        ClusterConfig::new("test").namespace("bad\0namespace"),
    ];
    for config in invalid_configs {
        assert!(matches!(
            Cluster::connect(config).await,
            Err(ApplicationError::Config(_))
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_cluster_facade_applies_spec_and_runs_both_runtime_builders() {
    let fixture = EtcdFixture::new().expect("etcd is required for facade integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .unwrap();
    cluster.admin().apply(ClusterSpec::new()).await.unwrap();

    let spec = ClusterSpec::new()
        .instances([InstanceSpec::new("node-a").zone("zone-a")])
        .resource(
            ResourceSpec::leader_standby("documents")
                .partitions(1)
                .replicas(1)
                .placement(Placement::semi_auto(BTreeMap::from([(
                    String::from("documents_0"),
                    vec![String::from("node-a")],
                )]))),
        );
    cluster.admin().apply(spec.clone()).await.unwrap();
    cluster.admin().apply(spec).await.unwrap();
    cluster
        .admin()
        .set_transition_limits([
            TransitionLimit::cluster(10),
            TransitionLimit::resource("documents", 2).transition_type(TransitionType::LoadBalance),
        ])
        .await
        .unwrap();
    let transition_limits: Value = serde_json::from_str(
        backend
            .get_metadata("controller/throttles")
            .await
            .unwrap()
            .unwrap()
            .value()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(transition_limits[0]["scope"], "CLUSTER");
    assert_eq!(transition_limits[0]["max_in_flight"], 10);
    assert_eq!(transition_limits[1]["scope"], "RESOURCE");
    assert_eq!(transition_limits[1]["resource"], "documents");
    assert_eq!(transition_limits[1]["rebalance_type"], "LOAD_BALANCE");
    assert_eq!(
        backend
            .get_metadata("controller/cluster")
            .await
            .unwrap()
            .and_then(|entry| entry.value().map(str::to_owned)),
        Some(String::from("test"))
    );
    assert!(!cluster
        .observer()
        .snapshot()
        .await
        .unwrap()
        .instances()
        .is_live("node-a"));
    cluster.admin().remove_instance("node-a").await.unwrap();
    assert_eq!(
        serde_json::from_str::<Vec<Value>>(
            backend
                .get_metadata("controller/instance-configs")
                .await
                .unwrap()
                .unwrap()
                .value()
                .unwrap(),
        )
        .unwrap(),
        Vec::<Value>::new()
    );

    let controller_ready = Arc::new(tokio::sync::Notify::new());
    let controller_ready_for_runtime = controller_ready.clone();
    let controller_ready_calls = Arc::new(AtomicUsize::new(0));
    let controller_ready_calls_for_runtime = controller_ready_calls.clone();
    let (controller_shutdown_tx, controller_shutdown_rx) = tokio::sync::oneshot::channel();
    let controller_task = tokio::spawn(
        cluster
            .controller("facade-controller")
            .lease_ttl(Duration::from_secs(1))
            .on_ready(move || {
                controller_ready_calls_for_runtime.fetch_add(1, Ordering::SeqCst);
                controller_ready_for_runtime.notify_one();
                Ok::<_, std::io::Error>(())
            })
            .run_until(async move {
                let _ = controller_shutdown_rx.await;
            }),
    );
    tokio::time::timeout(Duration::from_secs(5), controller_ready.notified())
        .await
        .expect("facade controller becomes ready");
    controller_shutdown_tx.send(()).unwrap();
    controller_task.await.unwrap().unwrap();
    assert_eq!(controller_ready_calls.load(Ordering::SeqCst), 1);
    cluster
        .observer()
        .wait_until(Duration::from_secs(5), |snapshot| {
            snapshot.controllers().active().is_none()
        })
        .await
        .expect("facade controller relinquishes authority");

    let participant_ready = Arc::new(tokio::sync::Notify::new());
    let participant_ready_for_runtime = participant_ready.clone();
    let participant_ready_calls = Arc::new(AtomicUsize::new(0));
    let participant_ready_calls_for_runtime = participant_ready_calls.clone();
    let (participant_shutdown_tx, participant_shutdown_rx) = tokio::sync::oneshot::channel();
    let participant_task = tokio::spawn(
        cluster
            .participant("node-a")
            .resource("documents", RecordingParticipantHandler::new())
            .lease_ttl(Duration::from_secs(1))
            .on_ready(move || {
                participant_ready_calls_for_runtime.fetch_add(1, Ordering::SeqCst);
                participant_ready_for_runtime.notify_one();
                Ok::<_, std::io::Error>(())
            })
            .run_until(async move {
                let _ = participant_shutdown_rx.await;
            }),
    );
    tokio::time::timeout(Duration::from_secs(5), participant_ready.notified())
        .await
        .expect("facade participant becomes ready");
    participant_shutdown_tx.send(()).unwrap();
    participant_task.await.unwrap().unwrap();
    assert_eq!(participant_ready_calls.load(Ordering::SeqCst), 1);
    assert!(!cluster
        .observer()
        .snapshot()
        .await
        .unwrap()
        .instances()
        .is_live("node-a"));

    assert!(matches!(
        cluster.participant("").run_until(async {}).await,
        Err(ApplicationError::Spec(_))
    ));
    assert!(matches!(
        cluster
            .participant("node-a")
            .resource("documents", RecordingParticipantHandler::new())
            .resource("documents", RecordingParticipantHandler::new())
            .run_until(async {})
            .await,
        Err(ApplicationError::Spec(message)) if message.contains("declared more than once")
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_admin_apply_reports_spec_errors_before_writing() {
    let fixture = EtcdFixture::new().expect("etcd is required for facade integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .unwrap();

    let invalid_specs = [
        ClusterSpec::new().instances([InstanceSpec::new("")]),
        ClusterSpec::new().instances([InstanceSpec::new("node-a").zone("")]),
        ClusterSpec::new().resource(ResourceSpec::leader_standby("documents")),
        ClusterSpec::new().resource(
            ResourceSpec::leader_standby("documents")
                .partitions(1)
                .replicas(2)
                .placement(Placement::semi_auto(BTreeMap::from([(
                    String::from("documents_0"),
                    vec![String::from("node-a")],
                )]))),
        ),
    ];
    for spec in invalid_specs {
        assert!(matches!(
            cluster.admin().apply(spec).await,
            Err(ApplicationError::Spec(_))
        ));
    }
    assert!(backend
        .get_metadata("controller/instance-configs")
        .await
        .unwrap()
        .is_none());
    assert!(backend
        .get_metadata("controller/resources/documents")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_builders_report_callback_and_builder_failures() {
    let fixture = EtcdFixture::new().expect("etcd is required for facade integration tests");
    let _backend = fixture.connect().await.expect("etcd becomes ready");
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .unwrap();

    let controller_result = cluster
        .controller("callback-controller")
        .lease_ttl(Duration::from_secs(1))
        .on_ready(|| Err::<(), _>(std::io::Error::other("controller callback failed")))
        .run_until(std::future::pending::<()>())
        .await;
    assert!(matches!(
        controller_result,
        Err(ApplicationError::Callback(error)) if error.to_string() == "controller callback failed"
    ));

    let participant_result = cluster
        .participant("node-a")
        .lease_ttl(Duration::from_secs(1))
        .on_ready(|| Err::<(), _>(std::io::Error::other("participant callback failed")))
        .run_until(async {})
        .await;
    assert!(matches!(
        participant_result,
        Err(ApplicationError::Callback(error)) if error.to_string() == "participant callback failed"
    ));
    assert!(!cluster
        .observer()
        .snapshot()
        .await
        .unwrap()
        .instances()
        .is_live("node-a"));

    assert!(matches!(
        cluster
            .participant("node-a")
            .resource("", RecordingParticipantHandler::new())
            .run_until(async {})
            .await,
        Err(ApplicationError::Spec(_))
    ));
    assert!(matches!(
        cluster
            .participant("node-a")
            .lease_ttl(Duration::ZERO)
            .run_until(async {})
            .await,
        Err(ApplicationError::Participant(_))
    ));
    assert!(matches!(
        cluster
            .controller("invalid-ttl-controller")
            .lease_ttl(Duration::ZERO)
            .run_until(async {})
            .await,
        Err(ApplicationError::Controller(_))
    ));
    assert!(matches!(
        cluster.controller("").run_until(async {}).await,
        Err(ApplicationError::Controller(_))
    ));
    assert!(matches!(
        cluster
            .controller("oversized-ttl-controller")
            .lease_ttl(Duration::from_secs(u64::MAX))
            .run_until(async {})
            .await,
        Err(ApplicationError::Config(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_validates_and_publishes_all_configuration_variants() {
    let fixture = EtcdFixture::new().expect("etcd is required for admin integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let admin = ClusterAdmin::new(backend.clone());

    assert!(matches!(
        admin.ensure_cluster("other").await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidCluster)
    ));
    admin.ensure_cluster("test").await.unwrap();
    admin.ensure_cluster("test").await.unwrap();
    backend
        .put_metadata("controller/cluster", "wrong")
        .await
        .unwrap();
    assert!(matches!(
        admin.ensure_cluster("test").await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidCluster)
    ));
    backend
        .put_metadata("controller/cluster", "test")
        .await
        .unwrap();
    admin
        .remove_instance("missing-before-config")
        .await
        .unwrap();
    assert!(matches!(
        admin
            .put_instance(AdminInstanceSpec {
                instance_id: String::new(),
                zone: String::from("zone-a"),
            })
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidKey)
    ));
    for zone in [String::new(), String::from("bad\0zone")] {
        assert!(matches!(
            admin
                .put_instance(AdminInstanceSpec {
                    instance_id: String::from("node-a"),
                    zone,
                })
                .await,
            Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
        ));
    }
    admin
        .put_instance(AdminInstanceSpec {
            instance_id: String::from("node-a"),
            zone: String::from("zone-a"),
        })
        .await
        .unwrap();
    admin
        .put_instance(AdminInstanceSpec {
            instance_id: String::from("node-a"),
            zone: String::from("zone-a"),
        })
        .await
        .unwrap();
    assert!(matches!(
        admin.remove_instance("").await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidKey)
    ));
    admin.remove_instance("missing").await.unwrap();

    for spec in [
        AdminResourceSpec {
            name: String::new(),
            partitions: 1,
            replicas: 1,
            state_model: String::from("LeaderStandby"),
            placement: AdminPlacementSpec::Crush,
        },
        AdminResourceSpec {
            name: String::from("documents"),
            partitions: 0,
            replicas: 1,
            state_model: String::from("LeaderStandby"),
            placement: AdminPlacementSpec::Crush,
        },
        AdminResourceSpec {
            name: String::from("documents"),
            partitions: 1,
            replicas: 1,
            state_model: String::from("Other"),
            placement: AdminPlacementSpec::Crush,
        },
    ] {
        assert!(matches!(
            admin.put_resource(spec).await,
            Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
                | Err(clustodian::coordination::etcd::CoordinationError::InvalidKey)
        ));
    }
    admin
        .put_resource(AdminResourceSpec {
            name: String::from("documents"),
            partitions: 2,
            replicas: 1,
            state_model: String::from("LeaderStandby"),
            placement: AdminPlacementSpec::Crush,
        })
        .await
        .unwrap();
    admin
        .put_resource(AdminResourceSpec {
            name: String::from("semi-auto"),
            partitions: 1,
            replicas: 1,
            state_model: String::from("LeaderStandby"),
            placement: AdminPlacementSpec::SemiAuto {
                preference_lists: BTreeMap::from([(
                    String::from("p0"),
                    vec![String::from("node-a")],
                )]),
            },
        })
        .await
        .unwrap();
    admin
        .put_resource(AdminResourceSpec {
            name: String::from("profiles"),
            partitions: 1,
            replicas: 1,
            state_model: String::from("LeaderStandby"),
            placement: AdminPlacementSpec::CrushWithTopology {
                topology: CrushTopologySpec::new("/zone/instance", "zone", "instance"),
            },
        })
        .await
        .unwrap();
    let crush = backend
        .get_metadata("controller/resources/documents")
        .await
        .unwrap()
        .unwrap();
    let crush: Value = serde_json::from_str(crush.value().unwrap()).unwrap();
    assert_eq!(crush["name"], "documents");
    assert_eq!(crush["placement"]["kind"], "CRUSH");
    assert_eq!(
        crush["placement"]["partitions"].as_array().unwrap().len(),
        2
    );

    let topology = backend
        .get_metadata("controller/resources/profiles")
        .await
        .unwrap()
        .unwrap();
    let topology: Value = serde_json::from_str(topology.value().unwrap()).unwrap();
    assert_eq!(topology["placement"]["topology"]["path"], "/zone/instance");
    assert_eq!(topology["placement"]["topology"]["fault_zone_type"], "zone");

    for preference_lists in [
        BTreeMap::new(),
        BTreeMap::from([(String::from(""), vec![String::from("node-a")])]),
        BTreeMap::from([(String::from("p0"), Vec::new())]),
        BTreeMap::from([(String::from("p0"), vec![String::new()])]),
    ] {
        assert!(matches!(
            admin
                .put_resource(AdminResourceSpec {
                    name: String::from("invalid-resource"),
                    partitions: 1,
                    replicas: 1,
                    state_model: String::from("LeaderStandby"),
                    placement: AdminPlacementSpec::SemiAuto { preference_lists },
                })
                .await,
            Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
                | Err(clustodian::coordination::etcd::CoordinationError::InvalidKey)
        ));
    }
    admin
        .put_resource(AdminResourceSpec {
            name: String::from("semi-auto"),
            partitions: 1,
            replicas: 1,
            state_model: String::from("LeaderStandby"),
            placement: AdminPlacementSpec::SemiAuto {
                preference_lists: BTreeMap::from([(
                    String::from("p0"),
                    vec![String::from("node-a")],
                )]),
            },
        })
        .await
        .unwrap();
    admin
        .put_throttles(vec![ThrottleSpec {
            scope: String::from("CLUSTER"),
            rebalance_type: String::from("ANY"),
            max_in_flight: 1,
        }])
        .await
        .unwrap();
    let throttles = backend
        .get_metadata("controller/throttles")
        .await
        .unwrap()
        .unwrap();
    let throttles: Value = serde_json::from_str(throttles.value().unwrap()).unwrap();
    assert_eq!(throttles[0]["scope"], "CLUSTER");
    assert_eq!(throttles[0]["rebalance_type"], "ANY");
    assert_eq!(throttles[0]["max_in_flight"], 1);
    for throttle in [
        ThrottleSpec {
            scope: String::new(),
            rebalance_type: String::from("ANY"),
            max_in_flight: 1,
        },
        ThrottleSpec {
            scope: String::from("CLUSTER"),
            rebalance_type: String::from("ANY"),
            max_in_flight: 0,
        },
    ] {
        assert!(matches!(
            admin.put_throttles(vec![throttle]).await,
            Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observers_wait_for_controller_idle_and_current_state() {
    let fixture = EtcdFixture::new().expect("etcd is required for observer integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let admin = ClusterAdmin::new(backend.clone());
    admin.ensure_cluster("test").await.unwrap();

    let observer = clustodian::Observer::new(backend.clone());
    let initial = observer
        .wait_until(Duration::from_secs(1), |_| true)
        .await
        .unwrap();
    assert_eq!(initial.controllers().active(), None);
    assert!(matches!(
        observer
            .wait_until(Duration::from_millis(20), |_| false)
            .await,
        Err(clustodian::observe::WaitError::Timeout(_))
    ));
    let observer_waiting = observer.clone();
    let observer_wait_task = tokio::spawn(async move {
        observer_waiting
            .wait_for_controller(Duration::from_secs(2))
            .await
    });
    let cluster_observer = ClusterObserver::new(backend.clone());
    assert_eq!(
        cluster_observer
            .snapshot()
            .await
            .unwrap()
            .controllers
            .active(),
        None
    );
    let cluster_waiting = cluster_observer.clone();
    let cluster_wait_task = tokio::spawn(async move {
        cluster_waiting
            .wait_for_controller(Duration::from_secs(2))
            .await
    });
    let at_revision = backend
        .put_metadata("observer-probe", "present")
        .await
        .unwrap();
    assert!(cluster_observer
        .snapshot_at_revision(at_revision)
        .await
        .is_ok());
    let historical = observer.snapshot_at_revision(at_revision).await.unwrap();
    assert!(historical.observer_revision().value() >= at_revision.value());

    let election = ControllerElection::new(
        backend.clone(),
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("observer-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    let leadership = election.acquire().await.unwrap();
    let controller = observer
        .wait_for_controller(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(
        controller.controllers().active(),
        Some("observer-controller")
    );
    assert_eq!(
        observer_wait_task
            .await
            .unwrap()
            .unwrap()
            .controllers()
            .active(),
        Some("observer-controller")
    );
    assert_eq!(
        cluster_wait_task
            .await
            .unwrap()
            .unwrap()
            .controllers
            .active(),
        Some("observer-controller")
    );
    backend
        .put_metadata("controller/output/processed-revision", "999")
        .await
        .unwrap();
    let idle = observer
        .wait_for_idle(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(idle.processed_revision().unwrap().value(), 999);

    assert!(matches!(
        cluster_observer
            .wait_until(Duration::from_millis(20), |_| false)
            .await,
        Err(clustodian::observe::WaitError::Timeout(_))
    ));
    let immediate = cluster_observer
        .wait_until(Duration::from_secs(1), |_| true)
        .await
        .unwrap();
    assert_eq!(immediate.controllers.active(), Some("observer-controller"));
    let waited = cluster_observer
        .wait_for_controller(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(waited.controllers.active(), Some("observer-controller"));
    assert!(cluster_observer
        .wait_for_idle(Duration::from_secs(1))
        .await
        .is_ok());
    drop(leadership);

    assert!(matches!(
        cluster_observer.session_current_state_exists("", "1").await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidKey)
    ));
    assert!(matches!(
        cluster_observer
            .session_current_state_exists("node-a", "not-a-session")
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidSession)
    ));
    let session = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(session.instance_id().as_str(), "node-a");
    assert!(session.registration_revision().value() > 0);
    session.keep_alive().await.unwrap();
    assert!(matches!(
        backend
            .inject_session_current_state(
                &instance("node-a"),
                clustodian::model::SessionId::from_wire_value(
                    session.session_id().wire_value() + 100
                ),
                resource("documents"),
                states(&[("documents_0", "OFFLINE")]),
            )
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::UnknownSession(_))
    ));
    assert!(!cluster_observer
        .session_current_state_exists("node-a", &session.session_id().wire_value().to_string())
        .await
        .unwrap());
    backend
        .publish_current_state_for_session(
            &instance("node-a"),
            session.session_id(),
            resource("documents"),
            states(&[("documents_0", "OFFLINE")]),
        )
        .await
        .unwrap();
    assert!(cluster_observer
        .session_current_state_exists("node-a", &session.session_id().wire_value().to_string())
        .await
        .unwrap());
    session.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observer_rejects_malformed_published_metadata() {
    let fixture = EtcdFixture::new().expect("etcd is required for observer integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let observer = ClusterObserver::new(backend.clone());

    backend
        .put_metadata("controller/output/processed-revision", "not-a-revision")
        .await
        .unwrap();
    assert!(matches!(
        observer.snapshot().await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
    ));
    backend
        .put_metadata("controller/output/processed-revision", "1")
        .await
        .unwrap();
    backend
        .put_metadata("controller/output/external-view", "not-json")
        .await
        .unwrap();
    assert!(matches!(
        observer.snapshot().await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
    ));
    backend
        .put_metadata("controller/output/external-view", "{}")
        .await
        .unwrap();
    backend
        .put_metadata("controller/output/pending-transitions", "not-json")
        .await
        .unwrap();
    assert!(matches!(
        observer.snapshot().await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordination_snapshot_rejects_malformed_election_and_metadata_records() {
    let fixture = EtcdFixture::new().expect("etcd is required for coordination tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let client = etcd_client::Client::connect([fixture.endpoint.as_str()], None)
        .await
        .unwrap();

    client
        .kv_client()
        .put(
            format!("{}/controller/election/active", fixture.prefix()),
            "controller-a",
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        backend.controller_snapshot().await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
    ));

    client
        .kv_client()
        .delete(
            format!("{}/controller/election/active", fixture.prefix()),
            None,
        )
        .await
        .unwrap();
    client
        .kv_client()
        .put(
            format!(
                "{}/controller/election/candidates/636f6e74726f6c6c65722d61",
                fixture.prefix()
            ),
            "controller-a",
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        backend.controller_snapshot().await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
    ));

    client
        .kv_client()
        .delete(
            format!(
                "{}/controller/election/candidates/636f6e74726f6c6c65722d61",
                fixture.prefix()
            ),
            None,
        )
        .await
        .unwrap();
    client
        .kv_client()
        .put(
            format!("{}/metadata/626164", fixture.prefix()),
            vec![0xff],
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        backend.controller_snapshot().await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
    ));
}

async fn output(backend: &EtcdCoordination, key: &str) -> Option<Value> {
    backend
        .get_metadata(key)
        .await
        .expect("output metadata read succeeds")
        .and_then(|entry| {
            entry
                .value()
                .map(|value| serde_json::from_str(value).unwrap())
        })
}

async fn wait_for_output(backend: &EtcdCoordination, key: &str) -> Value {
    for _ in 0..200 {
        if let Some(value) = output(backend, key).await {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for controller output {key}");
}

async fn wait_for_nonempty_pending(backend: &EtcdCoordination) -> Value {
    for _ in 0..200 {
        if let Some(value) = output(backend, "controller/output/pending-transitions").await {
            if value.as_array().is_some_and(|items| !items.is_empty()) {
                return value;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for pending controller transitions");
}

async fn wait_for_pending_instance_state(
    backend: &EtcdCoordination,
    instance_name: &str,
    from: &str,
    to: &str,
) -> Value {
    for _ in 0..200 {
        if let Some(value) = output(backend, "controller/output/pending-transitions").await {
            if value.as_array().is_some_and(|items| {
                items.iter().any(|item| {
                    item["instance"] == instance_name && item["from"] == from && item["to"] == to
                })
            }) {
                return value;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for pending transition {instance_name}: {from} -> {to}");
}

async fn wait_for_external_state(
    backend: &EtcdCoordination,
    resource_name: &str,
    partition_name: &str,
    instance_name: &str,
    expected: &str,
) -> Value {
    for _ in 0..200 {
        if let Some(value) = output(backend, "controller/output/external-view").await {
            if value[resource_name][partition_name][instance_name] == expected {
                return value;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for ExternalView state");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn natural_lease_expiry_advances_controller_progress() {
    let fixture = EtcdFixture::new().expect("etcd is required for lease expiry tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let admin = ClusterAdmin::new(backend.clone());
    admin
        .put_instance(AdminInstanceSpec {
            instance_id: String::from("node-a"),
            zone: String::from("zone-a"),
        })
        .await
        .unwrap();
    admin
        .put_resource(AdminResourceSpec {
            name: String::from("documents"),
            partitions: 1,
            replicas: 1,
            state_model: String::from("LeaderStandby"),
            placement: AdminPlacementSpec::Crush,
        })
        .await
        .unwrap();

    let before_registration = backend.controller_snapshot().await.unwrap();
    let mut deletion_watch = backend
        .watch_namespace_from(before_registration.revision().next().unwrap())
        .await
        .unwrap();
    let participant = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap();
    participant
        .publish_current_state(resource("documents"), states(&[("documents_0", "OFFLINE")]))
        .await
        .unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_runtime = events.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("natural-expiry-controller"),
            lease_ttl_ms: 1_500,
        },
    )
    .await
    .unwrap()
    .on_event(move |event| events_for_runtime.lock().unwrap().push(event));
    let task = tokio::spawn(runtime.run_until(async move {
        let _ = shutdown_rx.await;
    }));

    wait_for_external_state(&backend, "documents", "documents_0", "node-a", "OFFLINE").await;
    let deletion_revision = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = deletion_watch.next().await.unwrap();
            if event.kind() == WatchEventKind::Delete && event.key().starts_with("live/") {
                break event.revision();
            }
        }
    })
    .await
    .expect("natural lease expiry produces a live-instance delete");

    let idle = ClusterObserver::new(backend.clone())
        .wait_for_idle(Duration::from_secs(5))
        .await
        .unwrap();
    assert!(!idle.instances().is_live("node-a"));
    assert!(idle.external_view["documents"]["documents_0"]
        .get("node-a")
        .is_none());
    assert!(idle
        .processed_revision
        .is_some_and(|revision| revision.value() >= deletion_revision.value()));

    tokio::time::sleep(Duration::from_millis(750)).await;
    assert!(
        !task.is_finished(),
        "controller remains alive after lease expiry"
    );
    let events = events.lock().unwrap();
    let publication_rejections = events
        .iter()
        .filter(|event| matches!(event, RuntimeEvent::PublicationRejected { .. }))
        .count();
    assert!(publication_rejections <= 1);
    assert!(!events.iter().any(|event| {
        matches!(event, RuntimeEvent::FatalRuntimeError { message, .. } if message.contains("contention"))
    }));
    drop(events);

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("controller shuts down")
        .unwrap()
        .is_ok());
}

#[cfg(feature = "m13-failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_reclassifies_authority_loss_during_output_publication() {
    let fixture = EtcdFixture::new().expect("etcd is required for authority tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/instance-configs",
            r#"[{"name":"node-a","zone":"zone-a"}]"#,
        )
        .await
        .unwrap();
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":1,"preference_lists":{"documents_0":["node-a"]}}}"#,
        )
        .await
        .unwrap();
    let participant = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    participant
        .publish_current_state(resource("documents"), states(&[("documents_0", "OFFLINE")]))
        .await
        .unwrap();

    fail::cfg("controller_before_output_txn", "sleep(1000)").unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_runtime = events.clone();
    let authority_ready = Arc::new(tokio::sync::Notify::new());
    let authority_ready_for_runtime = authority_ready.clone();
    let controller_a = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("controller-a"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap()
    .on_authority_ready(move |_| authority_ready_for_runtime.notify_one())
    .on_event(move |event| events_for_runtime.lock().unwrap().push(event));
    let task_a = tokio::spawn(controller_a.run());
    tokio::time::timeout(Duration::from_secs(2), authority_ready.notified())
        .await
        .expect("controller A acquires authority");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let lease_a = backend
        .controller_snapshot()
        .await
        .unwrap()
        .controller_election()
        .active()
        .expect("controller A is active")
        .1;
    let election_b = ControllerElection::new(
        backend.clone(),
        ControllerElectionConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("controller-b"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    let acquire_b = tokio::spawn(async move { election_b.acquire().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let client = etcd_client::Client::connect([fixture.endpoint.as_str()], None)
        .await
        .unwrap();
    client.lease_client().revoke(lease_a).await.unwrap();
    let leader_b = tokio::time::timeout(Duration::from_secs(5), acquire_b)
        .await
        .expect("controller B acquires authority")
        .unwrap()
        .unwrap();

    fail::remove("controller_before_output_txn");
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(!task_a.is_finished(), "controller A keeps campaigning");
    let events = events.lock().unwrap();
    assert!(!events.iter().any(|event| {
        matches!(event, RuntimeEvent::FatalRuntimeError { message, .. } if message.contains("contention"))
    }));
    drop(events);

    task_a.abort();
    let _ = task_a.await;
    drop(leader_b);
    participant.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crush_controller_survives_live_instance_removal_attempt() {
    let fixture = EtcdFixture::new().expect("etcd is required for CRUSH tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let admin = ClusterAdmin::new(backend.clone());
    for (instance_id, zone) in [("node-a", "zone-a"), ("node-b", "zone-b")] {
        admin
            .put_instance(AdminInstanceSpec {
                instance_id: String::from(instance_id),
                zone: String::from(zone),
            })
            .await
            .unwrap();
    }
    admin
        .put_resource(AdminResourceSpec {
            name: String::from("documents"),
            partitions: 1,
            replicas: 1,
            state_model: String::from("LeaderStandby"),
            placement: AdminPlacementSpec::Crush,
        })
        .await
        .unwrap();
    let participant = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(600)).unwrap(),
        )
        .await
        .unwrap();
    participant
        .publish_current_state(resource("documents"), states(&[("documents_0", "OFFLINE")]))
        .await
        .unwrap();

    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("crush-removal-controller"),
            lease_ttl_ms: 1_500,
        },
    )
    .await
    .unwrap();
    let task = tokio::spawn(runtime.run());
    wait_for_external_state(&backend, "documents", "documents_0", "node-a", "OFFLINE").await;

    assert!(matches!(
        admin.remove_instance("node-a").await,
        Err(clustodian::coordination::etcd::CoordinationError::InstanceStillLive(id))
            if id == instance("node-a")
    ));
    tokio::time::sleep(Duration::from_millis(750)).await;
    assert!(
        !task.is_finished(),
        "CRUSH controller survives rejected removal"
    );

    participant.revoke().await.unwrap();
    admin.remove_instance("node-a").await.unwrap();
    tokio::time::sleep(Duration::from_millis(750)).await;
    assert!(
        !task.is_finished(),
        "CRUSH controller survives completed removal"
    );

    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_cas_and_gap_free_watch() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    assert!(backend.get_metadata("missing").await.unwrap().is_none());

    let first_revision = backend.put_metadata("alpha", "one").await.unwrap();
    let first = backend.get_metadata("alpha").await.unwrap().unwrap();
    assert_eq!(first.value(), Some("one"));
    assert_eq!(first.revision(), first_revision);

    let stale = backend
        .compare_and_put_metadata(
            "alpha",
            Revision::new(first_revision.value() + 1).unwrap(),
            "stale",
        )
        .await
        .unwrap();
    assert!(!stale.applied());
    let updated = backend
        .compare_and_put_metadata("alpha", first_revision, "two")
        .await
        .unwrap();
    assert!(updated.applied());
    assert!(updated.revision() > first_revision);
    assert!(backend
        .put_metadata_if_absent("only-once", "first")
        .await
        .unwrap());
    assert!(!backend
        .put_metadata_if_absent("only-once", "second")
        .await
        .unwrap());
    assert_eq!(
        backend
            .get_metadata("only-once")
            .await
            .unwrap()
            .unwrap()
            .value(),
        Some("first")
    );

    let (entry, mut watch) = backend.snapshot_and_watch_metadata("alpha").await.unwrap();
    assert_eq!(entry.value(), Some("two"));
    backend.put_metadata("alpha", "three").await.unwrap();
    let event = watch.next().await.unwrap();
    assert_eq!(event.key(), "alpha");
    assert_eq!(event.value(), Some("three"));
    assert_eq!(event.kind(), WatchEventKind::Put);

    watch.disconnect();
    backend.put_metadata("alpha", "four").await.unwrap();
    assert!(matches!(watch.next().await, Err(WatchError::Disconnected)));
    watch.resume().await.unwrap();
    assert_eq!(watch.next().await.unwrap().value(), Some("four"));

    let client = etcd_client::Client::connect([fixture.endpoint.as_str()], None)
        .await
        .unwrap();
    client
        .kv_client()
        .delete(format!("{}/metadata/616c706861", fixture.prefix()), None)
        .await
        .unwrap();
    let deleted = watch.next().await.unwrap();
    assert_eq!(deleted.key(), "alpha");
    assert_eq!(deleted.value(), None);
    assert_eq!(deleted.kind(), WatchEventKind::Delete);

    let (empty, mut missing_watch) = backend
        .snapshot_and_watch_metadata("new-key")
        .await
        .unwrap();
    assert_eq!(empty.value(), None);
    backend.put_metadata("new-key", "value").await.unwrap();
    assert_eq!(missing_watch.next().await.unwrap().value(), Some("value"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_wait_until_survives_etcd_restart() {
    let mut fixture = EtcdFixture::new_isolated().expect("etcd is required for observer tests");
    if !fixture.can_control_process() {
        return;
    }
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let node_a = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    let observer = Observer::new(backend.clone());
    let wait = tokio::spawn(async move {
        observer
            .wait_until(Duration::from_secs(8), |snapshot| {
                snapshot.instances().is_live("node-b")
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(fixture.stop_process());
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert!(fixture.restart_process().await, "local etcd restarts");
    let restarted = fixture.connect().await.expect("etcd reconnects");
    let node_b = restarted
        .register(
            instance("node-b"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("typed observer wait remains alive")
        .expect("typed observer wait task does not panic")
        .expect("typed observer sees the post-restart registration");
    node_a.revoke().await.unwrap();
    node_b.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_observer_wait_until_survives_etcd_restart() {
    let mut fixture = EtcdFixture::new_isolated().expect("etcd is required for observer tests");
    if !fixture.can_control_process() {
        return;
    }
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let node_a = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    let observer = ClusterObserver::new(backend.clone());
    let wait = tokio::spawn(async move {
        observer
            .wait_until(Duration::from_secs(8), |snapshot| {
                snapshot.live_instances.contains_key("node-c")
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(fixture.stop_process());
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert!(fixture.restart_process().await, "local etcd restarts");
    let restarted = fixture.connect().await.expect("etcd reconnects");
    let node_c = restarted
        .register(
            instance("node-c"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("cluster observer wait remains alive")
        .expect("cluster observer wait task does not panic")
        .expect("cluster observer sees the post-restart registration");
    if let Err(error) = node_a.revoke().await {
        assert!(matches!(
            error,
            clustodian::coordination::etcd::CoordinationError::Etcd(_)
        ));
    }
    node_c.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_replaces_and_removes_existing_instance_configuration() {
    let fixture = EtcdFixture::new().expect("etcd is required for admin integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let admin = ClusterAdmin::new(backend.clone());

    assert!(matches!(
        admin
            .put_instance(AdminInstanceSpec {
                instance_id: String::from("node-invalid"),
                zone: String::from("   "),
            })
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
    ));
    admin
        .put_instance(AdminInstanceSpec {
            instance_id: String::from("node-b"),
            zone: String::from("zone-b"),
        })
        .await
        .unwrap();
    admin
        .put_instance(AdminInstanceSpec {
            instance_id: String::from("node-a"),
            zone: String::from("zone-a"),
        })
        .await
        .unwrap();
    admin
        .put_instance(AdminInstanceSpec {
            instance_id: String::from("node-a"),
            zone: String::from("zone-a2"),
        })
        .await
        .unwrap();

    let instances: Vec<Value> = serde_json::from_str(
        backend
            .get_metadata("controller/instance-configs")
            .await
            .unwrap()
            .unwrap()
            .value()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(instances.len(), 2);
    assert_eq!(instances[0]["name"], "node-a");
    assert_eq!(instances[0]["zone"], "zone-a2");
    assert_eq!(instances[1]["name"], "node-b");

    admin.remove_instance("node-b").await.unwrap();
    let instances: Vec<Value> = serde_json::from_str(
        backend
            .get_metadata("controller/instance-configs")
            .await
            .unwrap()
            .unwrap()
            .value()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        instances,
        vec![json!({"name": "node-a", "zone": "zone-a2"})]
    );

    admin.remove_instance("node-a").await.unwrap();
    assert_eq!(
        backend
            .get_metadata("controller/instance-configs")
            .await
            .unwrap()
            .unwrap()
            .value(),
        Some("[]")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_fences_live_instance_removal_and_disables_resource_deletion() {
    let fixture = EtcdFixture::new().expect("etcd is required for admin integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let admin = ClusterAdmin::new(backend.clone());
    admin.ensure_cluster("test").await.unwrap();
    admin
        .put_instance(AdminInstanceSpec {
            instance_id: String::from("node-a"),
            zone: String::from("zone-a"),
        })
        .await
        .unwrap();
    admin
        .put_resource(AdminResourceSpec {
            name: String::from("documents"),
            partitions: 1,
            replicas: 1,
            state_model: String::from("LeaderStandby"),
            placement: AdminPlacementSpec::Crush,
        })
        .await
        .unwrap();

    let configured_before = backend
        .get_metadata("controller/instance-configs")
        .await
        .unwrap();
    let session = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        admin.remove_instance("node-a").await,
        Err(clustodian::coordination::etcd::CoordinationError::InstanceStillLive(id))
            if id == instance("node-a")
    ));
    assert_eq!(
        backend
            .get_metadata("controller/instance-configs")
            .await
            .unwrap(),
        configured_before
    );

    session
        .publish_current_state(resource("documents"), states(&[("documents_0", "OFFLINE")]))
        .await
        .unwrap();
    let message = transition_message(
        "resource-delete-disabled",
        session.session_id().wire_value(),
        "OFFLINE",
        "STANDBY",
    );
    backend.inject_pending_transition(&message).await.unwrap();
    let resource_before = backend
        .get_metadata("controller/resources/documents")
        .await
        .unwrap();
    let pending_before = backend
        .get_metadata("controller/output/pending-transitions")
        .await
        .unwrap();
    let current_before = backend.participant_snapshot().await.unwrap();
    assert!(matches!(
        admin.remove_resource("documents").await,
        Err(clustodian::coordination::etcd::CoordinationError::ResourceDeletionDisabled)
    ));
    assert_eq!(
        backend
            .get_metadata("controller/resources/documents")
            .await
            .unwrap(),
        resource_before
    );
    assert_eq!(
        backend
            .get_metadata("controller/output/pending-transitions")
            .await
            .unwrap(),
        pending_before
    );
    assert_eq!(
        backend.participant_snapshot().await.unwrap(),
        current_before
    );

    session.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_session_accessors_and_stale_operations_are_fenced() {
    let fixture = EtcdFixture::new().expect("etcd is required for session integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("session-node");
    let session = backend
        .register(
            instance_id.clone(),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(session.instance_id(), &instance_id);
    assert_eq!(
        backend.live_session(&instance_id).await.unwrap(),
        Some(session.session_id())
    );
    assert!(session.registration_revision().value() > 0);
    session
        .publish_current_state(resource("documents"), states(&[("documents_0", "STANDBY")]))
        .await
        .unwrap();

    let unknown_session = SessionId::from_wire_value(session.session_id().wire_value() + 1);
    assert!(matches!(
        backend
            .publish_current_state_for_session(
                &instance_id,
                unknown_session,
                resource("documents"),
                states(&[("documents_0", "LEADER")]),
            )
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleSession(id))
            if id == instance_id
    ));
    assert!(matches!(
        backend
            .inject_session_current_state(
                &instance_id,
                unknown_session,
                resource("documents"),
                states(&[("documents_0", "LEADER")]),
            )
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::UnknownSession(id))
            if id == unknown_session
    ));
    assert!(matches!(
        backend.revoke_live(&instance_id, unknown_session).await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleSession(id))
            if id == instance_id
    ));

    backend
        .revoke_live(&instance_id, session.session_id())
        .await
        .unwrap();
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
    assert!(matches!(
        session
            .publish_current_state(resource("documents"), states(&[("documents_0", "LEADER")]))
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleSession(id))
            if id == instance_id
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordination_public_api_snapshot_watch_and_error_paths() {
    let fixture = EtcdFixture::new().expect("etcd is required for coordination tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    assert_eq!(backend.cluster(), "test");
    assert!(format!("{backend:?}").contains("EtcdCoordination"));

    let initial = backend.controller_snapshot().await.unwrap();
    let historical = backend
        .controller_snapshot_at_revision(initial.revision())
        .await
        .unwrap();
    assert_eq!(historical.revision(), initial.revision());

    let mut namespace_watch = backend
        .watch_namespace_from(initial.revision().next().unwrap())
        .await
        .unwrap();
    backend.put_metadata("api-smoke", "one").await.unwrap();
    assert!(namespace_watch
        .next()
        .await
        .unwrap()
        .key()
        .starts_with("metadata/"));
    namespace_watch.disconnect();

    let (snapshot, mut replacement_watch) = backend.snapshot_and_watch_namespace().await.unwrap();
    assert_eq!(snapshot.metadata()["api-smoke"].value(), Some("one"));
    backend.put_metadata("api-smoke", "two").await.unwrap();
    assert_eq!(replacement_watch.next().await.unwrap().value(), Some("two"));
    replacement_watch.disconnect();

    let snapshot = backend.controller_snapshot().await.unwrap();
    let mut metadata_watch = backend
        .watch_metadata_from("direct-watch", snapshot.revision().next().unwrap())
        .await
        .unwrap();
    backend.put_metadata("direct-watch", "value").await.unwrap();
    assert_eq!(metadata_watch.next().await.unwrap().value(), Some("value"));
    metadata_watch.disconnect();

    backend
        .compact(backend.controller_snapshot().await.unwrap().revision())
        .await
        .unwrap();

    let errors = [
        clustodian::coordination::etcd::CoordinationError::InvalidPrefix,
        clustodian::coordination::etcd::CoordinationError::InvalidCluster,
        clustodian::coordination::etcd::CoordinationError::InvalidKey,
        clustodian::coordination::etcd::CoordinationError::InvalidValue,
        clustodian::coordination::etcd::CoordinationError::InvalidLeaseTtl,
        clustodian::coordination::etcd::CoordinationError::LeaseExpired,
        clustodian::coordination::etcd::CoordinationError::InvalidRevision(2),
        clustodian::coordination::etcd::CoordinationError::RevisionExhausted,
        clustodian::coordination::etcd::CoordinationError::MissingRevision,
        clustodian::coordination::etcd::CoordinationError::OutsidePrefix,
        clustodian::coordination::etcd::CoordinationError::InvalidSession,
        clustodian::coordination::etcd::CoordinationError::UnknownSession(
            SessionId::from_wire_value(2),
        ),
        clustodian::coordination::etcd::CoordinationError::RegistrationLost,
        clustodian::coordination::etcd::CoordinationError::Contention,
        clustodian::coordination::etcd::CoordinationError::StaleSession(instance("node-a")),
        clustodian::coordination::etcd::CoordinationError::DuplicateMessage(String::from("m1")),
        clustodian::coordination::etcd::CoordinationError::StaleController,
        clustodian::coordination::etcd::CoordinationError::StaleRevision,
    ];
    assert!(errors.iter().all(|error| !error.to_string().is_empty()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_watermarks_and_pending_queue_cover_edge_cases() {
    let fixture = EtcdFixture::new().expect("etcd is required for coordination tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");

    let high = Revision::new(7).unwrap();
    assert_eq!(
        backend
            .advance_metadata_revision("watermark", high)
            .await
            .unwrap(),
        high
    );
    assert_eq!(
        backend
            .advance_metadata_revision("watermark", Revision::new(6).unwrap())
            .await
            .unwrap(),
        high
    );
    assert_eq!(
        backend
            .advance_metadata_revision("watermark", high)
            .await
            .unwrap(),
        high
    );
    backend
        .put_metadata("invalid-watermark", "not-a-revision")
        .await
        .unwrap();
    assert!(matches!(
        backend
            .advance_metadata_revision("invalid-watermark", high)
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
    ));

    let empty = transition_message("", 1, "OFFLINE", "STANDBY");
    assert!(matches!(
        backend.inject_pending_transition(&empty).await,
        Err(clustodian::coordination::etcd::CoordinationError::InvalidValue)
    ));

    let message = transition_message("queue-edge", 1, "OFFLINE", "STANDBY");
    let queue_revision = backend.inject_pending_transition(&message).await.unwrap();
    assert!(matches!(
        backend.inject_pending_transition(&message).await,
        Err(clustodian::coordination::etcd::CoordinationError::DuplicateMessage(id))
            if id == "queue-edge"
    ));
    let other_message = transition_message("queue-edge-other", 1, "OFFLINE", "STANDBY");
    backend
        .inject_pending_transition(&other_message)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .remove_pending_transition(queue_revision, "queue-edge")
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleRevision)
    ));
    assert!(!backend
        .remove_pending_transition(queue_revision, "missing")
        .await
        .unwrap());
    let current_queue_revision = backend
        .get_metadata("controller/output/pending-transitions")
        .await
        .unwrap()
        .unwrap()
        .revision();
    assert!(backend
        .remove_pending_transition(current_queue_revision, "queue-edge")
        .await
        .unwrap());
    assert!(!backend
        .remove_pending_transition(current_queue_revision, "queue-edge")
        .await
        .unwrap());

    let session = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    let completion_message = transition_message(
        "complete-edge",
        session.session_id().wire_value(),
        "OFFLINE",
        "STANDBY",
    );
    let completion_revision = backend
        .inject_pending_transition(&completion_message)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .complete_pending_transition(
                &instance("node-a"),
                session.session_id(),
                completion_revision,
                &completion_message,
                Some(&state("STANDBY")),
            )
            .await,
        Ok(clustodian::coordination::etcd::CompletionResult::Applied(_))
    ));
    assert!(matches!(
        backend
            .complete_pending_transition(
                &instance("node-a"),
                session.session_id(),
                completion_revision,
                &completion_message,
                None,
            )
            .await,
        Ok(clustodian::coordination::etcd::CompletionResult::AlreadyCompleted)
    ));

    let dropped_message = transition_message(
        "complete-dropped",
        session.session_id().wire_value(),
        "STANDBY",
        "DROPPED",
    );
    let dropped_revision = backend
        .inject_pending_transition(&dropped_message)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .complete_pending_transition(
                &instance("node-a"),
                session.session_id(),
                dropped_revision,
                &dropped_message,
                None,
            )
            .await,
        Ok(clustodian::coordination::etcd::CompletionResult::Applied(_))
    ));

    session.revoke().await.unwrap();
    let lost_message = transition_message(
        "complete-session-lost",
        session.session_id().wire_value(),
        "OFFLINE",
        "STANDBY",
    );
    let lost_revision = backend
        .inject_pending_transition(&lost_message)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .complete_pending_transition(
                &instance("node-a"),
                session.session_id(),
                lost_revision,
                &lost_message,
                Some(&state("STANDBY")),
            )
            .await,
        Ok(clustodian::coordination::etcd::CompletionResult::SessionLost)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_sessions_are_lease_backed_and_fenced() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    assert!(RegistrationOptions::new(Duration::ZERO).is_err());
    let options = RegistrationOptions::new(Duration::from_secs(5))
        .unwrap()
        .with_default_initial_state(state("OFFLINE"))
        .with_resource_initial_state(resource("documents"), state("OFFLINE"));
    let node_a = instance("node-a");
    let first = backend
        .register(node_a.clone(), options.clone())
        .await
        .unwrap();
    assert_eq!(first.instance_id(), &node_a);
    assert_eq!(
        backend.live_session(&node_a).await.unwrap(),
        Some(first.session_id())
    );
    first.keep_alive().await.unwrap();
    assert!(backend
        .participant_snapshot()
        .await
        .unwrap()
        .active_current_state()
        .contains_key(&node_a));

    first
        .publish_current_state(resource("documents"), states(&[("documents_0", "STANDBY")]))
        .await
        .unwrap();
    let active = backend.participant_snapshot().await.unwrap();
    let current = active
        .active_current_state()
        .get(&node_a)
        .unwrap()
        .resources()
        .get(&resource("documents"))
        .unwrap();
    assert_eq!(
        current
            .state(&partition("documents_0"), &node_a)
            .map(State::as_str),
        Some("STANDBY")
    );

    let second_attempt = backend.register(node_a.clone(), options.clone()).await;
    assert!(matches!(
        second_attempt,
        Err(clustodian::coordination::etcd::CoordinationError::RegistrationLost)
    ));
    assert_eq!(
        backend.live_session(&node_a).await.unwrap(),
        Some(first.session_id())
    );

    assert!(matches!(
        backend
            .revoke_live(
                &node_a,
                clustodian::model::SessionId::from_wire_value(first.session_id().wire_value() + 1),
            )
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleSession(_))
    ));
    first.revoke().await.unwrap();
    assert_eq!(backend.live_session(&node_a).await.unwrap(), None);
    assert!(matches!(
        backend.revoke_live(&node_a, first.session_id()).await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleSession(_))
    ));
    assert!(backend
        .participant_snapshot()
        .await
        .unwrap()
        .active_current_state()
        .is_empty());

    let replacement_options = RegistrationOptions::new(Duration::from_secs(5))
        .unwrap()
        .with_default_initial_state(state("ERROR"))
        .with_resource_initial_state(resource("documents"), state("OFFLINE"));
    let replacement = backend
        .register(node_a.clone(), replacement_options)
        .await
        .unwrap();
    assert!(replacement.session_id() > first.session_id());
    let replacement_snapshot = backend.participant_snapshot().await.unwrap();
    let carried = replacement_snapshot
        .active_current_state()
        .get(&node_a)
        .unwrap()
        .resources()
        .get(&resource("documents"))
        .unwrap();
    assert_eq!(
        carried
            .state(&partition("documents_0"), &node_a)
            .map(State::as_str),
        Some("OFFLINE")
    );
    assert!(matches!(
        first
            .publish_current_state(resource("documents"), states(&[("documents_0", "LEADER")]))
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleSession(_))
    ));
    replacement
        .publish_current_state(resource("documents"), states(&[("documents_0", "STANDBY")]))
        .await
        .unwrap();
    replacement.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_election_fences_a_replaced_lease() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let election_a = ControllerElection::new(
        backend.clone(),
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("controller-a"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    let first = election_a.acquire().await.unwrap();
    let old_authority = first.authority();
    backend
        .put_controller_owned(&old_authority, "probe", b"first".to_vec())
        .await
        .unwrap();
    drop(first);
    // Simulate controller A stopping before its lease expires. Its lease is
    // intentionally left for etcd to expire so B exercises the replacement
    // path rather than an explicit relinquish.

    let election_b = ControllerElection::new(
        backend.clone(),
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("controller-b"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(8), election_b.acquire())
        .await
        .expect("replacement controller is elected")
        .unwrap();
    backend
        .put_controller_owned(&second.authority(), "probe", b"second".to_vec())
        .await
        .unwrap();

    let other_backend = fixture
        .connect_namespace(&format!("{}/other", fixture.prefix()), "test")
        .await
        .expect("other namespace becomes ready");
    let other_election = ControllerElection::new(
        other_backend,
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("other-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    let other_leadership = other_election.acquire().await.unwrap();
    assert!(matches!(
        backend
            .put_controller_owned(&other_leadership.authority(), "probe", b"other".to_vec())
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleController)
    ));
    drop(other_leadership);
    assert!(matches!(
        backend
            .put_controller_owned(&old_authority, "probe", b"stale".to_vec())
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleController)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_election_validates_candidates_and_can_cancel_behind_a_leader() {
    let fixture = EtcdFixture::new().expect("etcd is required for election integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    for config in [
        ControllerElectionConfig {
            cluster: String::from("other-cluster"),
            controller_id: String::from("controller"),
            lease_ttl_ms: 1_000,
        },
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::new(),
            lease_ttl_ms: 1_000,
        },
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("bad\0controller"),
            lease_ttl_ms: 1_000,
        },
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("controller"),
            lease_ttl_ms: 0,
        },
    ] {
        assert!(
            clustodian::election::ControllerElection::new(backend.clone(), config)
                .await
                .is_err()
        );
    }

    let leader = ControllerElection::new(
        backend.clone(),
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("existing-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap()
    .acquire()
    .await
    .unwrap();
    let contender_backend = backend.clone();
    let contender = ControllerElection::new(
        contender_backend.clone(),
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("waiting-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    let mut shutdown = Box::pin(async {
        tokio::time::sleep(Duration::from_millis(150)).await;
    });
    assert!(contender
        .acquire_until(shutdown.as_mut())
        .await
        .unwrap()
        .is_none());

    let duplicate_candidate = ControllerElection::new(
        contender_backend,
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("existing-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await;
    let duplicate_candidate = duplicate_candidate.unwrap();
    let mut duplicate_shutdown = Box::pin(async {
        tokio::time::sleep(Duration::from_millis(75)).await;
    });
    assert!(duplicate_candidate
        .acquire_until(duplicate_shutdown.as_mut())
        .await
        .unwrap()
        .is_none());
    drop(leader);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_publication_rejects_stale_output_inputs() {
    let fixture = EtcdFixture::new().expect("etcd is required for controller integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let election = ControllerElection::new(
        backend.clone(),
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("publication-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    let leadership = election.acquire().await.unwrap();

    let observed = backend.controller_snapshot().await.unwrap().revision();
    backend
        .put_metadata(
            "controller/output/processed-revision",
            &(observed.value() + 100).to_string(),
        )
        .await
        .unwrap();
    assert!(matches!(
        backend
            .publish_controller_outputs(&leadership.authority(), "{}", "[]", observed, None,)
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleRevision)
    ));

    backend
        .put_metadata("controller/output/processed-revision", "1")
        .await
        .unwrap();
    let pending_revision = backend
        .put_metadata("controller/output/pending-transitions", "[]")
        .await
        .unwrap();
    let observed = backend.controller_snapshot().await.unwrap().revision();
    assert!(matches!(
        backend
            .publish_controller_outputs(
                &leadership.authority(),
                "{}",
                "[]",
                observed,
                Some(Revision::new(pending_revision.value() + 1).unwrap()),
            )
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleRevision)
    ));
    drop(leadership);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_completion_rejects_unrelated_queue_mutation() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let session = backend
        .register(
            instance_id.clone(),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    let first = transition_message(
        "fence-M1",
        session.session_id().wire_value(),
        "OFFLINE",
        "STANDBY",
    );
    let first_revision = backend.inject_pending_transition(&first).await.unwrap();
    let second = transition_message(
        "fence-M2",
        session.session_id().wire_value(),
        "OFFLINE",
        "STANDBY",
    );
    backend.inject_pending_transition(&second).await.unwrap();

    assert!(matches!(
        backend
            .complete_pending_transition(
                &instance_id,
                session.session_id(),
                first_revision,
                &first,
                Some(&state("STANDBY")),
            )
            .await,
        Err(clustodian::coordination::etcd::CoordinationError::StaleRevision)
    ));
    let pending: Vec<TransitionMessage> = serde_json::from_str(
        backend
            .get_metadata("controller/output/pending-transitions")
            .await
            .unwrap()
            .and_then(|entry| entry.value().map(str::to_owned))
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(pending, vec![first, second]);
    session.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observer_membership_and_state_share_one_revision() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let election = ControllerElection::new(
        backend.clone(),
        ControllerElectionConfig {
            cluster: String::from("test"),
            controller_id: String::from("controller-a"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    let leadership = election.acquire().await.unwrap();
    backend
        .put_metadata("observer-probe", "present")
        .await
        .unwrap();

    let snapshot = ClusterObserver::new(backend).snapshot().await.unwrap();
    assert!(snapshot.observer_revision.value() > 0);
    assert_eq!(
        snapshot.controllers.active,
        vec![String::from("controller-a")]
    );
    assert!(snapshot.controllers.standby.is_empty());
    drop(leadership);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_cluster_has_no_semantic_writes_for_thirty_seconds() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let admin = ClusterAdmin::new(backend.clone());
    admin.ensure_cluster("test").await.unwrap();
    let ready = Arc::new(tokio::sync::Notify::new());
    let ready_for_runtime = ready.clone();
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: String::from("test"),
            controller_id: String::from("idle-controller"),
            lease_ttl_ms: 1_500,
        },
    )
    .await
    .unwrap()
    .on_ready(move || {
        ready_for_runtime.notify_one();
        Ok::<_, std::io::Error>(())
    });
    let task = tokio::spawn(runtime.run());
    tokio::time::timeout(Duration::from_secs(5), ready.notified())
        .await
        .expect("controller becomes ready");
    let observer = ClusterObserver::new(backend.clone());
    let before = observer.snapshot().await.unwrap();
    assert_eq!(before.controllers.active.len(), 1);
    tokio::time::sleep(Duration::from_secs(30)).await;
    let after = observer.snapshot().await.unwrap();
    task.abort();
    let _ = task.await;

    assert_eq!(after.observer_revision, before.observer_revision);
    assert_eq!(after.pending_transitions, before.pending_transitions);
    assert_eq!(after.external_view, before.external_view);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_admin_instance_updates_preserve_both_writers() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata("controller/instance-configs", "[]")
        .await
        .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let admin_a = ClusterAdmin::new(backend.clone());
    let admin_b = admin_a.clone();
    let barrier_a = barrier.clone();
    let update_a = tokio::spawn(async move {
        barrier_a.wait().await;
        admin_a
            .put_instance(AdminInstanceSpec {
                instance_id: String::from("node-a"),
                zone: String::from("zone-a"),
            })
            .await
    });
    let barrier_b = barrier.clone();
    let update_b = tokio::spawn(async move {
        barrier_b.wait().await;
        admin_b
            .put_instance(AdminInstanceSpec {
                instance_id: String::from("node-b"),
                zone: String::from("zone-b"),
            })
            .await
    });
    barrier.wait().await;
    update_a.await.unwrap().unwrap();
    update_b.await.unwrap().unwrap();

    let instances: Vec<Value> = serde_json::from_str(
        backend
            .get_metadata("controller/instance-configs")
            .await
            .unwrap()
            .unwrap()
            .value()
            .unwrap(),
    )
    .unwrap();
    let names = instances
        .iter()
        .map(|instance| instance["name"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(names, BTreeSet::from(["node-a", "node-b"]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namespace_watch_recovers_after_compaction() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let ready_revision = backend.put_metadata("watch-ready", "yes").await.unwrap();
    backend.compact(ready_revision).await.unwrap();
    let mut old_watch = backend
        .watch_namespace_from(Revision::new(1).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        old_watch.next().await,
        Err(WatchError::Compacted { .. })
    ));
    assert!(matches!(
        old_watch.recover_compaction().await.unwrap(),
        WatchRecovery::Namespace(snapshot)
            if snapshot.metadata().contains_key("watch-ready")
    ));
    backend.put_metadata("after-recovery", "two").await.unwrap();
    let recovered = old_watch.next().await.unwrap();
    assert_eq!(recovered.key(), "metadata/61667465722d7265636f76657279");

    let metadata_revision = backend.put_metadata("compact-me", "value").await.unwrap();
    backend.compact(metadata_revision).await.unwrap();
    let mut metadata_watch = backend
        .watch_metadata_from("compact-me", Revision::new(1).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        metadata_watch.next().await,
        Err(WatchError::Compacted { .. })
    ));
    assert!(matches!(
        metadata_watch.recover_compaction().await.unwrap(),
        WatchRecovery::Metadata(Some(entry))
            if entry.value() == Some("value")
    ));
    assert!(format!("{metadata_watch:?}").contains("compact-me"));

    let mut missing_metadata_watch = backend
        .watch_metadata_from("never-created", Revision::new(1).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        missing_metadata_watch.next().await,
        Err(WatchError::Compacted { .. })
    ));
    assert!(matches!(
        missing_metadata_watch.recover_compaction().await.unwrap(),
        WatchRecovery::Metadata(None)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn namespace_ranges_do_not_include_sibling_prefixes() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let _backend = fixture.connect().await.expect("etcd becomes ready");
    let first = EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint: fixture.endpoint.clone(),
        prefix: String::from("/clustodian/cluster-1"),
        cluster: String::from("test"),
    })
    .await
    .unwrap();
    let sibling = EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint: fixture.endpoint.clone(),
        prefix: String::from("/clustodian/cluster-10"),
        cluster: String::from("test"),
    })
    .await
    .unwrap();
    first.put_metadata("owned", "one").await.unwrap();
    sibling.put_metadata("sibling", "two").await.unwrap();

    let snapshot = first.controller_snapshot().await.unwrap();
    assert!(snapshot.metadata().contains_key("owned"));
    assert!(!snapshot.metadata().contains_key("sibling"));

    let mut watch = first
        .watch_namespace_from(snapshot.revision().next().unwrap())
        .await
        .unwrap();
    sibling
        .put_metadata("sibling-later", "three")
        .await
        .unwrap();
    first.put_metadata("owned-later", "four").await.unwrap();
    let event = watch.next().await.unwrap();
    assert_eq!(event.key(), "metadata/6f776e65642d6c61746572");
    assert_eq!(event.value(), Some("four"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_runtime_publishes_actual_state_and_reacts_to_watches() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/instance-configs",
            &serde_json::to_string(&json!([
                {"name": "node-a", "zone": "zone-a"},
                {"name": "node-b", "zone": "zone-b"}
            ]))
            .unwrap(),
        )
        .await
        .unwrap();
    backend
        .put_metadata(
            "controller/resources/documents",
            &serde_json::to_string(&json!({
                "name": "documents",
                "state_model": "LeaderStandby",
                "placement": {
                    "kind": "SEMI_AUTO",
                    "replicas": 2,
                    "preference_lists": {"documents_0": ["node-a", "node-b"]}
                }
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    backend
        .put_metadata(
            "controller/throttles",
            "[{\"scope\":\"CLUSTER\",\"rebalance_type\":\"ANY\",\"max_in_flight\":0}]",
        )
        .await
        .unwrap();
    let options = RegistrationOptions::new(Duration::from_secs(5)).unwrap();
    let node_a = backend
        .register(instance("node-a"), options.clone())
        .await
        .unwrap();
    let node_b = backend.register(instance("node-b"), options).await.unwrap();
    node_a
        .publish_current_state(resource("documents"), states(&[("documents_0", "OFFLINE")]))
        .await
        .unwrap();
    node_b
        .publish_current_state(resource("documents"), states(&[("documents_0", "OFFLINE")]))
        .await
        .unwrap();

    let ready_dir = tempdir().unwrap();
    let ready_file = ready_dir.path().join("ready");
    let ready_file_for_runtime = ready_file.clone();
    let authority_ready = Arc::new(tokio::sync::Notify::new());
    let authority_ready_for_runtime = authority_ready.clone();
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("m10-test-controller"),
            // This test exercises watch-driven reconciliation, not lease
            // expiry. Keep the controller lease above the concurrent etcd
            // integration-suite scheduling noise.
            lease_ttl_ms: 10_000,
        },
    )
    .await
    .unwrap()
    .on_authority_ready(move |_| authority_ready_for_runtime.notify_one())
    .on_ready(move || std::fs::write(ready_file_for_runtime, b"ready\n"));
    let task = tokio::spawn(runtime.run());
    tokio::time::timeout(Duration::from_secs(5), authority_ready.notified())
        .await
        .expect("controller authority becomes ready");
    for _ in 0..200 {
        if ready_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(ready_file.exists());
    let pending = wait_for_output(&backend, "controller/output/pending-transitions").await;
    assert!(pending.as_array().is_some_and(|items| !items.is_empty()));
    backend
        .put_metadata("controller/throttles", "[]")
        .await
        .unwrap();
    let pending = wait_for_nonempty_pending(&backend).await;
    assert!(pending.as_array().is_some_and(|items| !items.is_empty()));
    assert_eq!(
        output(&backend, "controller/output/external-view")
            .await
            .unwrap()["documents"]["documents_0"]["node-a"],
        "OFFLINE"
    );

    node_a
        .publish_current_state(resource("documents"), states(&[("documents_0", "STANDBY")]))
        .await
        .unwrap();
    let external =
        wait_for_external_state(&backend, "documents", "documents_0", "node-a", "STANDBY").await;
    assert_eq!(external["documents"]["documents_0"]["node-a"], "STANDBY");

    node_a.revoke().await.unwrap();
    assert_eq!(
        backend.live_session(&instance("node-a")).await.unwrap(),
        None
    );
    task.abort();
    let _ = task.await;
}

#[cfg(feature = "m13-failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_does_not_publish_plan_from_invalidated_input_snapshot() {
    let fixture = EtcdFixture::new().expect("etcd is required for controller tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/instance-configs",
            r#"[{"name":"node-a","zone":"zone-a"}]"#,
        )
        .await
        .unwrap();
    let participant = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    participant
        .publish_current_state(resource("documents"), states(&[("documents_0", "OFFLINE")]))
        .await
        .unwrap();
    let (_, mut output_watch) = backend
        .snapshot_and_watch_metadata("controller/output/external-view")
        .await
        .unwrap();
    fail::cfg("controller_before_output_txn", "sleep(1000)").unwrap();
    let authority_ready = Arc::new(tokio::sync::Notify::new());
    let authority_ready_for_runtime = authority_ready.clone();
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("publication-race-controller"),
            lease_ttl_ms: 1_500,
        },
    )
    .await
    .unwrap()
    .on_authority_ready(move |_| authority_ready_for_runtime.notify_one());
    let task = tokio::spawn(runtime.run());
    tokio::time::timeout(Duration::from_secs(2), authority_ready.notified())
        .await
        .expect("controller authority becomes ready");
    tokio::time::sleep(Duration::from_millis(100)).await;

    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":1,"preference_lists":{"documents_0":["node-a"]}}}"#,
        )
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(3), output_watch.next())
        .await
        .expect("controller publishes output after retrying stale plan")
        .unwrap();
    assert!(event
        .value()
        .is_some_and(|value| value.contains("documents")));
    fail::remove("controller_before_output_txn");
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_runtime_replans_bootstrap_after_session_replacement() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/instance-configs",
            &serde_json::to_string(&json!([
                {"name": "node-a", "zone": "zone-a"},
                {"name": "node-b", "zone": "zone-b"}
            ]))
            .unwrap(),
        )
        .await
        .unwrap();
    backend
        .put_metadata(
            "controller/resources/documents",
            &serde_json::to_string(&json!({
                "name": "documents",
                "state_model": "LeaderStandby",
                "placement": {
                    "kind": "SEMI_AUTO",
                    "replicas": 2,
                    "preference_lists": {"documents_0": ["node-a", "node-b"]}
                }
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    let options = RegistrationOptions::new(Duration::from_secs(600)).unwrap();
    let node_a = backend
        .register(instance("node-a"), options.clone())
        .await
        .unwrap();
    let node_b = backend.register(instance("node-b"), options).await.unwrap();
    node_a
        .publish_current_state(resource("documents"), states(&[("documents_0", "LEADER")]))
        .await
        .unwrap();
    node_b
        .publish_current_state(resource("documents"), states(&[("documents_0", "STANDBY")]))
        .await
        .unwrap();

    let ready_dir = tempdir().unwrap();
    let ready_file = ready_dir.path().join("ready");
    let ready_file_for_runtime = ready_file.clone();
    let task = tokio::spawn(
        ControllerRuntime::new(
            backend.clone(),
            ControllerRuntimeConfig {
                cluster: backend.cluster().to_owned(),
                controller_id: String::from("m10-test-controller"),
                lease_ttl_ms: 1_500,
            },
        )
        .await
        .unwrap()
        .on_ready(move || std::fs::write(ready_file_for_runtime, b"ready\n"))
        .run(),
    );
    for _ in 0..200 {
        if ready_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(ready_file.exists());
    assert_eq!(
        output(&backend, "controller/output/pending-transitions")
            .await
            .unwrap()
            .as_array()
            .map(Vec::len),
        Some(0)
    );

    node_a.revoke().await.unwrap();
    let replacement = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(600)).unwrap(),
        )
        .await
        .unwrap();
    assert!(replacement.session_id() > node_a.session_id());

    let pending = wait_for_pending_instance_state(&backend, "node-a", "OFFLINE", "STANDBY").await;
    assert!(pending.as_array().unwrap().iter().any(|item| {
        item["instance"] == "node-b" && item["from"] == "STANDBY" && item["to"] == "LEADER"
    }));

    replacement.revoke().await.unwrap();
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_runtime_cancels_a_campaign_behind_an_existing_leader() {
    let fixture = EtcdFixture::new().expect("etcd is required for runtime integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let _leader = ControllerElection::new(
        backend.clone(),
        ControllerElectionConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("existing-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap()
    .acquire()
    .await
    .unwrap();

    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("cancelled-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();

    assert!(runtime.run_until(async {}).await.is_ok());
    assert_eq!(
        backend
            .controller_snapshot()
            .await
            .unwrap()
            .controller_election()
            .active()
            .map(|(controller, _)| controller),
        Some("existing-controller"),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_runtime_returns_election_errors() {
    let mut fixture =
        EtcdFixture::new_isolated().expect("etcd is required for runtime integration tests");
    if !fixture.can_control_process() {
        return;
    }
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("unavailable-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    assert!(fixture.stop_process());

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(runtime.run_until(async move {
        let _ = shutdown_rx.await;
    }));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !task.is_finished(),
        "transport errors are retried while etcd is down"
    );
    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("controller cancellation is not blocked by recovery")
        .unwrap()
        .is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_runtime_retries_after_a_candidate_lease_expires() {
    let fixture = EtcdFixture::new().expect("etcd is required for runtime integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let leader = ControllerElection::new(
        backend.clone(),
        ControllerElectionConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("existing-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap()
    .acquire()
    .await
    .unwrap();

    let controller_id = String::from("retry-controller");
    let encoded_controller_id = controller_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let candidate_key = format!(
        "{}/controller/election/candidates/{}",
        fixture.prefix(),
        encoded_controller_id,
    );
    let client = etcd_client::Client::connect([fixture.endpoint.as_str()], None)
        .await
        .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id,
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap();
    let task = tokio::spawn(runtime.run_until(async move {
        let _ = shutdown_rx.await;
    }));

    let first_lease = loop {
        if let Some(kv) = client
            .kv_client()
            .get(candidate_key.clone(), None)
            .await
            .unwrap()
            .kvs()
            .first()
        {
            break kv.lease();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_ne!(first_lease, 0);
    client.lease_client().revoke(first_lease).await.unwrap();

    let second_lease = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(kv) = client
                .kv_client()
                .get(candidate_key.clone(), None)
                .await
                .unwrap()
                .kvs()
                .first()
            {
                if kv.lease() != first_lease {
                    break kv.lease();
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("runtime retries after the candidate lease expires");
    assert_ne!(second_lease, first_lease);

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("runtime shuts down after retrying")
        .unwrap()
        .is_ok());
    drop(leader);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_runtime_reacquires_after_leadership_is_lost() {
    let fixture = EtcdFixture::new().expect("etcd is required for runtime integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":1,"preference_lists":{"documents_0":["node-a"]}}}"#,
        )
        .await
        .unwrap();
    let participant = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(600)).unwrap(),
        )
        .await
        .unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("reacquiring-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap()
    .on_ready(move || {
        ready_tx
            .send(())
            .map_err(|_| std::io::Error::other("ready receiver dropped"))
    });
    let task = tokio::spawn(runtime.run());
    match tokio::time::timeout(Duration::from_secs(5), ready_rx).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("controller runtime exited before ready: {:?}", task.await),
        Err(_) if task.is_finished() => {
            panic!("controller runtime exited before ready: {:?}", task.await)
        }
        Err(_) => panic!("controller did not become ready"),
    }

    let old_lease = backend
        .controller_snapshot()
        .await
        .unwrap()
        .controller_election()
        .active()
        .expect("controller is active")
        .1;
    let client = etcd_client::Client::connect([fixture.endpoint.as_str()], None)
        .await
        .unwrap();
    client.lease_client().revoke(old_lease).await.unwrap();

    let new_lease = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some((controller, lease)) = backend
                .controller_snapshot()
                .await
                .unwrap()
                .controller_election()
                .active()
            {
                if controller == "reacquiring-controller" && lease != old_lease {
                    break lease;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("controller reacquires after losing leadership");
    assert_ne!(new_lease, old_lease);

    task.abort();
    let _ = task.await;
    participant.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_survives_etcd_restart_while_leader() {
    let mut fixture = EtcdFixture::new_isolated().expect("etcd is required for controller tests");
    if !fixture.can_control_process() {
        return;
    }
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let authority_ready = Arc::new(tokio::sync::Notify::new());
    let authority_ready_for_runtime = authority_ready.clone();
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("restart-survivor-controller"),
            lease_ttl_ms: 1_000,
        },
    )
    .await
    .unwrap()
    .on_authority_ready(move |_| authority_ready_for_runtime.notify_one());
    let task = tokio::spawn(runtime.run());
    tokio::time::timeout(Duration::from_secs(5), authority_ready.notified())
        .await
        .expect("controller acquires authority");

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(fixture.stop_process());
    // This exceeds the one-second lease TTL, so successful recovery requires
    // the controller to campaign again rather than relying on its old lease.
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    assert!(fixture.restart_process().await, "local etcd restarts");
    assert!(!task.is_finished(), "controller task survives the outage");

    let restarted = fixture.connect().await.expect("etcd becomes ready again");
    let participant = restarted
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap();
    restarted
        .put_metadata(
            "controller/instance-configs",
            r#"[{"name":"node-a","zone":"zone-a"}]"#,
        )
        .await
        .unwrap();
    restarted
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":1,"preference_lists":{"documents_0":["node-a"]}}}"#,
        )
        .await
        .unwrap();
    let pending = tokio::time::timeout(
        Duration::from_secs(8),
        wait_for_nonempty_pending(&restarted),
    )
    .await
    .expect("controller reconciles a post-restart metadata mutation");
    assert!(pending.as_array().is_some_and(|items| !items.is_empty()));

    task.abort();
    let _ = task.await;
    participant.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_recovers_from_brief_keepalive_failure_before_lease_expiry() {
    let mut fixture = EtcdFixture::new_isolated().expect("etcd is required for controller tests");
    if !fixture.can_control_process() {
        return;
    }
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let authority_ready = Arc::new(tokio::sync::Notify::new());
    let authority_ready_for_runtime = authority_ready.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        ControllerRuntime::new(
            backend.clone(),
            ControllerRuntimeConfig {
                cluster: backend.cluster().to_owned(),
                controller_id: String::from("brief-outage-controller"),
                lease_ttl_ms: 10_000,
            },
        )
        .await
        .unwrap()
        .on_authority_ready(move |_| authority_ready_for_runtime.notify_one())
        .run_until(async move {
            let _ = shutdown_rx.await;
        }),
    );
    tokio::time::timeout(Duration::from_secs(5), authority_ready.notified())
        .await
        .expect("controller acquires authority");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let old_lease = backend
        .controller_snapshot()
        .await
        .unwrap()
        .controller_election()
        .active()
        .expect("controller is active")
        .1;

    assert!(fixture.stop_process());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(fixture.restart_process().await, "local etcd restarts");
    let restarted = fixture.connect().await.expect("etcd becomes ready again");
    let active_lease = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some((controller, lease)) = restarted
                .controller_snapshot()
                .await
                .unwrap()
                .controller_election()
                .active()
            {
                if controller == "brief-outage-controller" {
                    break lease;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("controller recovers before the ten-second lease expires");
    assert_eq!(active_lease, old_lease);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        lease_ttl(&fixture.endpoint, old_lease).await >= 7,
        "controller keepalive recovers the original lease before its ten-second TTL expires"
    );
    assert!(
        !task.is_finished(),
        "controller task survives the brief outage"
    );

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("controller shuts down")
        .unwrap()
        .is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_cluster_throttle_limits_real_inflight_work() {
    let fixture = EtcdFixture::new().expect("etcd is required for throttle tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/instance-configs",
            r#"[{"name":"node-a","zone":"zone-a"}]"#,
        )
        .await
        .unwrap();
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .unwrap();
    let handler = ThrottleHandler::new();
    let (participant_ready_tx, participant_ready_rx) = tokio::sync::oneshot::channel();
    let (participant_shutdown_tx, participant_shutdown_rx) = tokio::sync::oneshot::channel();
    let participant_task = tokio::spawn(
        cluster
            .participant("node-a")
            .resource("documents", handler.clone())
            .lease_ttl(Duration::from_secs(5))
            .on_ready(move || {
                participant_ready_tx
                    .send(())
                    .map_err(|_| std::io::Error::other("participant readiness dropped"))
            })
            .run_until(async move {
                let _ = participant_shutdown_rx.await;
            }),
    );
    participant_ready_rx.await.unwrap();

    let (controller_ready_tx, controller_ready_rx) = tokio::sync::oneshot::channel();
    let (controller_shutdown_tx, controller_shutdown_rx) = tokio::sync::oneshot::channel();
    let controller = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("throttle-controller"),
            lease_ttl_ms: 1_500,
        },
    )
    .await
    .unwrap()
    .on_ready(move || {
        controller_ready_tx
            .send(())
            .map_err(|_| std::io::Error::other("controller readiness dropped"))
    });
    let controller_task = tokio::spawn(controller.run_until(async move {
        let _ = controller_shutdown_rx.await;
    }));
    controller_ready_rx.await.unwrap();
    backend
        .put_metadata(
            "controller/throttles",
            r#"[{"scope":"CLUSTER","rebalance_type":"ANY","max_in_flight":1}]"#,
        )
        .await
        .unwrap();
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":1,"preference_lists":{"documents_0":["node-a"],"documents_1":["node-a"]}}}"#,
        )
        .await
        .unwrap();
    wait_for_throttle_calls(&handler, 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        handler.calls().len(),
        1,
        "the cluster quota admits one transition"
    );
    assert_eq!(handler.max_active.load(Ordering::Acquire), 1);
    handler.release();
    wait_for_throttle_calls(&handler, 2).await;
    assert_eq!(handler.max_active.load(Ordering::Acquire), 1);

    controller_shutdown_tx.send(()).unwrap();
    participant_shutdown_tx.send(()).unwrap();
    let _ = controller_task.await;
    let _ = participant_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_cluster_throttle_dynamic_limit_admits_new_work() {
    let fixture = EtcdFixture::new().expect("etcd is required for throttle tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/instance-configs",
            r#"[{"name":"node-a","zone":"zone-a"}]"#,
        )
        .await
        .unwrap();
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .unwrap();
    let handler = ThrottleHandler::new();
    let (participant_ready_tx, participant_ready_rx) = tokio::sync::oneshot::channel();
    let (participant_shutdown_tx, participant_shutdown_rx) = tokio::sync::oneshot::channel();
    let participant_task = tokio::spawn(
        cluster
            .participant("node-a")
            .resource("documents", handler.clone())
            .lease_ttl(Duration::from_secs(5))
            .on_ready(move || {
                participant_ready_tx
                    .send(())
                    .map_err(|_| std::io::Error::other("participant readiness dropped"))
            })
            .run_until(async move {
                let _ = participant_shutdown_rx.await;
            }),
    );
    participant_ready_rx.await.unwrap();
    let (controller_ready_tx, controller_ready_rx) = tokio::sync::oneshot::channel();
    let (controller_shutdown_tx, controller_shutdown_rx) = tokio::sync::oneshot::channel();
    let controller = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("dynamic-throttle-controller"),
            lease_ttl_ms: 1_500,
        },
    )
    .await
    .unwrap()
    .on_ready(move || {
        controller_ready_tx
            .send(())
            .map_err(|_| std::io::Error::other("controller readiness dropped"))
    });
    let controller_task = tokio::spawn(controller.run_until(async move {
        let _ = controller_shutdown_rx.await;
    }));
    controller_ready_rx.await.unwrap();
    backend
        .put_metadata(
            "controller/throttles",
            r#"[{"scope":"CLUSTER","rebalance_type":"ANY","max_in_flight":1}]"#,
        )
        .await
        .unwrap();
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":1,"preference_lists":{"documents_0":["node-a"],"documents_1":["node-a"]}}}"#,
        )
        .await
        .unwrap();
    wait_for_throttle_calls(&handler, 1).await;
    backend
        .put_metadata(
            "controller/throttles",
            r#"[{"scope":"CLUSTER","rebalance_type":"ANY","max_in_flight":2}]"#,
        )
        .await
        .unwrap();
    wait_for_throttle_calls(&handler, 2).await;
    assert_eq!(handler.max_active.load(Ordering::Acquire), 2);
    handler.release();
    wait_for_empty_queue(&backend).await;
    controller_shutdown_tx.send(()).unwrap();
    participant_shutdown_tx.send(()).unwrap();
    let _ = controller_task.await;
    let _ = participant_task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_runtime_returns_startup_metadata_errors() {
    let fixture = EtcdFixture::new().expect("etcd is required for runtime integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":1,"preference_lists":"not-a-map"}}"#,
        )
        .await
        .unwrap();
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("malformed-metadata-controller"),
            lease_ttl_ms: 10_000,
        },
    )
    .await
    .unwrap();

    let result = runtime.run().await;
    assert!(result.is_err());
    assert_eq!(
        backend
            .controller_snapshot()
            .await
            .unwrap()
            .controller_election()
            .active(),
        None,
        "fatal reconciler errors relinquish controller authority promptly"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_runtime_recovers_a_compacted_namespace_watch() {
    let fixture = EtcdFixture::new().expect("etcd is required for runtime integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/instance-configs",
            r#"[{"name":"node-a","zone":"zone-a"}]"#,
        )
        .await
        .unwrap();
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby","placement":{"kind":"SEMI_AUTO","replicas":1,"preference_lists":{"documents_0":["node-a"]}}}"#,
        )
        .await
        .unwrap();
    let participant = backend
        .register(
            instance("node-a"),
            RegistrationOptions::new(Duration::from_secs(600)).unwrap(),
        )
        .await
        .unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ControllerRuntime::new(
        backend.clone(),
        ControllerRuntimeConfig {
            cluster: backend.cluster().to_owned(),
            controller_id: String::from("compaction-controller"),
            // This test exercises watch recovery, not lease expiry. Keep the
            // controller lease comfortably above the startup budget so a
            // busy integration-test host cannot expire it before readiness.
            lease_ttl_ms: 10_000,
        },
    )
    .await
    .unwrap()
    .on_ready(move || {
        ready_tx
            .send(())
            .map_err(|_| std::io::Error::other("ready receiver dropped"))
    });
    let task = tokio::spawn(runtime.run_until(async move {
        let _ = shutdown_rx.await;
    }));
    tokio::time::timeout(Duration::from_secs(10), ready_rx)
        .await
        .expect("controller becomes ready")
        .expect("controller ready callback receiver remains live");

    let barrier = backend
        .put_metadata("controller/watch-compaction-barrier", "ready")
        .await
        .unwrap();
    backend.compact(barrier).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    backend
        .put_metadata("controller/watch-compaction-follow-up", "ready")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !task.is_finished(),
        "controller exits during watch recovery"
    );

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("controller shuts down after watch recovery")
        .unwrap()
        .is_ok());
    participant.revoke().await.unwrap();
}

#[derive(Clone)]
struct RecordingParticipantHandler {
    calls: Arc<Mutex<Vec<String>>>,
    fail: Arc<AtomicBool>,
}

impl RecordingParticipantHandler {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            fail: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_failure(&self, fail: bool) {
        self.fail.store(fail, Ordering::Release);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl TransitionHandler for RecordingParticipantHandler {
    fn handle(&self, execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        self.calls.lock().unwrap().push(format!(
            "{}:{}:{}->{}",
            execution.resource(),
            execution.partition(),
            execution.source_state(),
            execution.target_state()
        ));
        if self.fail.load(Ordering::Acquire) {
            Err(TransitionHandlerError::new("handler failed"))
        } else {
            Ok(())
        }
    }
}

impl ResourceHandler for RecordingParticipantHandler {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        self.calls.lock().unwrap().push(format!(
            "documents:{}:{}->{}",
            transition.partition(),
            transition.source(),
            transition.target()
        ));
        if self.fail.load(Ordering::Acquire) {
            Err(TransitionError::new("handler failed"))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AsyncTransitionRecord {
    partition: String,
    source: String,
    target: String,
    attempt_id: String,
}

#[derive(Clone)]
struct AsyncHandlerHarness {
    records: Arc<Mutex<Vec<AsyncTransitionRecord>>>,
    entered: Arc<tokio::sync::Notify>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
    wait_for_cancellation: Arc<AtomicBool>,
    cancelled: Arc<AtomicUsize>,
}

impl AsyncHandlerHarness {
    fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
            entered: Arc::new(tokio::sync::Notify::new()),
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
            fail: Arc::new(AtomicBool::new(false)),
            wait_for_cancellation: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn records(&self) -> Vec<AsyncTransitionRecord> {
        self.records.lock().unwrap().clone()
    }

    fn fail(&self, value: bool) {
        self.fail.store(value, Ordering::Release);
    }

    fn wait_for_cancellation(&self, value: bool) {
        self.wait_for_cancellation.store(value, Ordering::Release);
    }
}

impl ResourceHandler for AsyncHandlerHarness {
    async fn transition(
        &self,
        transition: ResourceTransition,
        context: TransitionContext,
    ) -> Result<(), TransitionError> {
        self.records.lock().unwrap().push(AsyncTransitionRecord {
            partition: transition.partition().to_string(),
            source: transition.source().to_string(),
            target: transition.target().to_string(),
            attempt_id: transition.attempt_id().to_string(),
        });
        self.entered.notify_waiters();

        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active.fetch_max(active, Ordering::AcqRel);
        let result = if self.wait_for_cancellation.load(Ordering::Acquire) {
            tokio::select! {
                () = context.cancellation().cancelled() => {
                    self.cancelled.fetch_add(1, Ordering::AcqRel);
                    Err(TransitionError::cancelled())
                }
                () = tokio::time::sleep(Duration::from_secs(30)) => Ok(()),
            }
        } else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if self.fail.load(Ordering::Acquire) {
                Err(TransitionError::new("async handler failed"))
            } else {
                Ok(())
            }
        };
        self.active.fetch_sub(1, Ordering::AcqRel);
        result
    }
}

#[derive(Clone)]
struct SessionRecoveryHandler {
    records: Arc<Mutex<Vec<AsyncTransitionRecord>>>,
    entered: Arc<tokio::sync::Notify>,
    invocations: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
}

impl SessionRecoveryHandler {
    fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
            entered: Arc::new(tokio::sync::Notify::new()),
            invocations: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn records(&self) -> Vec<AsyncTransitionRecord> {
        self.records.lock().unwrap().clone()
    }

    fn was_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire) == 1
    }
}

impl ResourceHandler for SessionRecoveryHandler {
    async fn transition(
        &self,
        transition: ResourceTransition,
        context: TransitionContext,
    ) -> Result<(), TransitionError> {
        self.records.lock().unwrap().push(AsyncTransitionRecord {
            partition: transition.partition().to_string(),
            source: transition.source().to_string(),
            target: transition.target().to_string(),
            attempt_id: transition.attempt_id().to_string(),
        });
        self.entered.notify_waiters();
        if self.invocations.fetch_add(1, Ordering::AcqRel) == 0 {
            tokio::select! {
                () = context.cancellation().cancelled() => {
                    self.cancelled.fetch_add(1, Ordering::AcqRel);
                    Err(TransitionError::cancelled())
                }
                () = tokio::time::sleep(Duration::from_secs(30)) => Ok(()),
            }
        } else {
            Ok(())
        }
    }
}

async fn wait_for_recovery_handler_records(handler: &SessionRecoveryHandler, expected: usize) {
    for _ in 0..300 {
        if handler.records().len() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {expected} recovery handler records");
}

async fn wait_for_async_handler_records(handler: &AsyncHandlerHarness, expected: usize) {
    for _ in 0..200 {
        if handler.records().len() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {expected} async handler records");
}

#[derive(Clone)]
struct ThrottleHandler {
    calls: Arc<Mutex<Vec<String>>>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    released: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl ThrottleHandler {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            released: Arc::new(AtomicBool::new(false)),
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release.notify_waiters();
    }
}

impl ResourceHandler for ThrottleHandler {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        self.calls
            .lock()
            .unwrap()
            .push(transition.partition().to_string());
        self.entered.notify_waiters();
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active.fetch_max(active, Ordering::AcqRel);
        if !self.released.load(Ordering::Acquire) {
            self.release.notified().await;
        }
        self.active.fetch_sub(1, Ordering::AcqRel);
        Ok(())
    }
}

async fn wait_for_throttle_calls(handler: &ThrottleHandler, expected: usize) {
    for _ in 0..240 {
        if handler.calls().len() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {expected} throttled handler calls");
}

fn transition_message(
    message_id: &str,
    target_session: u64,
    from: &str,
    to: &str,
) -> TransitionMessage {
    transition_message_for_partition(message_id, "documents_0", target_session, from, to)
}

fn transition_message_for_partition(
    message_id: &str,
    partition_name: &str,
    target_session: u64,
    from: &str,
    to: &str,
) -> TransitionMessage {
    TransitionMessage {
        message_id: message_id.to_owned(),
        resource: String::from("documents"),
        partition: partition_name.to_owned(),
        instance: String::from("node-a"),
        target_session,
        from: from.to_owned(),
        to: to.to_owned(),
        message_type: String::from("STATE_TRANSITION"),
    }
}

async fn wait_for_participant_state(
    backend: &EtcdCoordination,
    instance_id: &InstanceId,
    resource_id: &ResourceId,
    partition_id: &PartitionId,
    expected: &str,
) {
    for _ in 0..200 {
        if let Some(state) = backend
            .participant_snapshot()
            .await
            .unwrap()
            .active_current_state()
            .get(instance_id)
            .and_then(|active| active.resources().get(resource_id))
            .and_then(|current| current.state(partition_id, instance_id))
        {
            if state.as_str() == expected {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for participant state {expected}");
}

async fn wait_for_handler_calls(handler: &RecordingParticipantHandler, expected: usize) {
    for _ in 0..200 {
        if handler.calls().len() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {expected} participant handler calls");
}

#[derive(Clone)]
struct SupersessionHandler {
    calls: Arc<Mutex<Vec<String>>>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl SupersessionHandler {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            entered: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn release(&self) {
        let (released, notify) = &*self.release;
        *released.lock().unwrap() = true;
        notify.notify_all();
    }
}

impl TransitionHandler for SupersessionHandler {
    fn handle(&self, execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        self.calls
            .lock()
            .unwrap()
            .push(execution.transition_id().to_owned());
        self.entered.notify_waiters();
        if execution.transition_id() == "M1" {
            let (released, notify) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = notify.wait(released).unwrap();
            }
        }
        Ok(())
    }
}

async fn wait_for_supersession_call(handler: &SupersessionHandler, expected: usize) {
    for _ in 0..200 {
        if handler.calls().len() >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {expected} supersession handler calls");
}

#[cfg(feature = "m13-failpoints")]
#[derive(Clone)]
struct PausedFailpoint {
    entered: Arc<AtomicBool>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

#[cfg(feature = "m13-failpoints")]
impl PausedFailpoint {
    fn install(name: &str) -> Self {
        let failpoint = Self {
            entered: Arc::new(AtomicBool::new(false)),
            release: Arc::new((Mutex::new(false), Condvar::new())),
        };
        let entered = failpoint.entered.clone();
        let release = failpoint.release.clone();
        fail::cfg_callback(name, move || {
            entered.store(true, Ordering::Release);
            let (released, notify) = &*release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = notify.wait(released).unwrap();
            }
        })
        .unwrap();
        failpoint
    }

    async fn wait_until_entered(&self) {
        for _ in 0..200 {
            if self.entered.load(Ordering::Acquire) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for failpoint callback");
    }

    fn release(&self) {
        let (released, notify) = &*self.release;
        *released.lock().unwrap() = true;
        notify.notify_all();
    }
}

#[cfg(feature = "m13-failpoints")]
impl Drop for PausedFailpoint {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone)]
struct BlockingParticipantHandler {
    entered: Arc<Mutex<BTreeSet<String>>>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl BlockingParticipantHandler {
    fn new() -> Self {
        Self {
            entered: Arc::new(Mutex::new(BTreeSet::new())),
            release: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn has_entered(&self, partition_name: &str) -> bool {
        self.entered.lock().unwrap().contains(partition_name)
    }

    fn release_blocked(&self) {
        let (released, notify) = &*self.release;
        *released.lock().unwrap() = true;
        notify.notify_all();
    }
}

impl TransitionHandler for BlockingParticipantHandler {
    fn handle(&self, execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        self.entered
            .lock()
            .unwrap()
            .insert(execution.partition().to_string());
        if execution.partition().as_str() == "documents_0" {
            let (released, notify) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = notify.wait(released).unwrap();
            }
        }
        Ok(())
    }
}

impl ResourceHandler for BlockingParticipantHandler {
    async fn transition(
        &self,
        transition: ResourceTransition,
        _context: TransitionContext,
    ) -> Result<(), TransitionError> {
        self.entered
            .lock()
            .unwrap()
            .insert(transition.partition().to_string());
        Ok(())
    }
}

async fn wait_for_partition_entry(handler: &BlockingParticipantHandler, partition_name: &str) {
    for _ in 0..200 {
        if handler.has_entered(partition_name) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for handler entry on {partition_name}");
}

async fn wait_for_empty_queue(backend: &EtcdCoordination) {
    for _ in 0..200 {
        if let Some(entry) = backend
            .get_metadata("controller/output/pending-transitions")
            .await
            .unwrap()
        {
            if serde_json::from_str::<Vec<Value>>(entry.value().unwrap_or("[]"))
                .unwrap()
                .is_empty()
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for an empty participant queue");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_runtime_executes_fences_and_reports_failures() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let resource_id = resource("documents");
    let partition_id = partition("documents_0");
    let handler = RecordingParticipantHandler::new();
    let ready_handler = handler.clone();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    );
    assert!(runtime.processed_revision_key().contains("node-a"));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(runtime.run(move || {
        ready_tx
            .send(())
            .map_err(|_| ParticipantRuntimeError::Io(String::from("ready receiver dropped")))
    }));
    ready_rx.await.expect("participant runtime becomes ready");

    let session = backend
        .live_session(&instance_id)
        .await
        .unwrap()
        .expect("runtime registered a live session");
    backend
        .put_metadata("participant-unrelated", "value")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    backend
        .inject_pending_transition(&transition_message(
            "promote",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_handler_calls(&ready_handler, 1).await;
    wait_for_participant_state(
        &backend,
        &instance_id,
        &resource_id,
        &partition_id,
        "STANDBY",
    )
    .await;
    wait_for_empty_queue(&backend).await;
    assert_eq!(
        ready_handler.calls(),
        vec!["documents:documents_0:OFFLINE->STANDBY"]
    );

    backend
        .inject_pending_transition(&transition_message(
            "current-state-mismatch",
            session.wire_value(),
            "OFFLINE",
            "LEADER",
        ))
        .await
        .unwrap();
    wait_for_empty_queue(&backend).await;
    assert_eq!(ready_handler.calls().len(), 1);

    backend
        .inject_pending_transition(&transition_message_for_partition(
            "non-adjacent-transition",
            "documents_non_adjacent",
            session.wire_value(),
            "OFFLINE",
            "LEADER",
        ))
        .await
        .unwrap();
    wait_for_empty_queue(&backend).await;
    assert_eq!(ready_handler.calls().len(), 1);

    backend
        .inject_pending_transition(&transition_message(
            "stale-target",
            session.wire_value() + 1,
            "STANDBY",
            "OFFLINE",
        ))
        .await
        .unwrap();
    wait_for_empty_queue(&backend).await;
    assert_eq!(ready_handler.calls().len(), 1);

    let mut invalid_type =
        transition_message("invalid-type", session.wire_value(), "STANDBY", "OFFLINE");
    invalid_type.message_type = String::from("NOT_A_STATE_TRANSITION");
    backend
        .inject_pending_transition(&invalid_type)
        .await
        .unwrap();
    wait_for_empty_queue(&backend).await;
    assert_eq!(ready_handler.calls().len(), 1);

    backend
        .inject_pending_transition(&transition_message_for_partition(
            "drop-instance",
            "documents_drop",
            session.wire_value(),
            "OFFLINE",
            "DROPPED",
        ))
        .await
        .unwrap();
    wait_for_empty_queue(&backend).await;
    assert!(!backend
        .participant_snapshot()
        .await
        .unwrap()
        .active_current_state()
        .get(&instance_id)
        .is_some_and(|active| {
            active.resources().get(&resource_id).is_some_and(|current| {
                current
                    .state(&partition("documents_drop"), &instance_id)
                    .is_some()
            })
        }));

    handler.set_failure(true);
    backend
        .inject_pending_transition(&transition_message(
            "failed-promotion",
            session.wire_value(),
            "STANDBY",
            "LEADER",
        ))
        .await
        .unwrap();
    wait_for_handler_calls(&ready_handler, 2).await;
    wait_for_participant_state(&backend, &instance_id, &resource_id, &partition_id, "ERROR").await;
    wait_for_empty_queue(&backend).await;

    backend.revoke_live(&instance_id, session).await.unwrap();
    let mut replacement_session = None;
    for _ in 0..200 {
        if let Some(replacement) = backend.live_session(&instance_id).await.unwrap() {
            if replacement != session {
                assert!(replacement > session);
                replacement_session = Some(replacement);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(replacement_session.is_some());

    task.abort();
    let _ = task.await;
}

struct PanickingParticipantHandler;

impl TransitionHandler for PanickingParticipantHandler {
    fn handle(&self, _execution: &TransitionExecution) -> Result<(), TransitionHandlerError> {
        panic!("participant handler panicked");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_runtime_reports_panicking_handlers_and_revokes_session() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        PanickingParticipantHandler,
    );
    let task = tokio::spawn(runtime.run(|| Ok(())));
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    backend
        .inject_pending_transition(&transition_message(
            "panic-handler",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant reports a panicking handler")
        .unwrap();
    assert!(matches!(
        result,
        Err(ParticipantRuntimeError::HandlerJoin(_))
    ));
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_runtime_retries_registration_until_previous_lease_is_gone() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let previous = backend
        .register(
            instance_id.clone(),
            RegistrationOptions::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        RecordingParticipantHandler::new(),
    )
    .with_lease_ttl(Duration::from_secs(1))
    .unwrap();
    let task = tokio::spawn(runtime.run(move || {
        ready_tx
            .send(())
            .map_err(|_| ParticipantRuntimeError::Io(String::from("ready receiver dropped")))
    }));

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        backend.live_session(&instance_id).await.unwrap(),
        Some(previous.session_id())
    );
    previous.revoke().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), ready_rx)
        .await
        .expect("replacement registration becomes ready")
        .expect("participant runtime sends ready");

    task.abort();
    let _ = task.await;
    if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
        backend.revoke_live(&instance_id, session).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_runtime_can_cancel_while_waiting_for_a_previous_session() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let previous = backend
        .register(
            instance_id.clone(),
            RegistrationOptions::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        RecordingParticipantHandler::new(),
    );
    let task = tokio::spawn(runtime.run_until(
        async move {
            let _ = shutdown_rx.await;
        },
        || Ok(()),
    ));
    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("participant cancellation completes")
        .unwrap()
        .is_ok());
    previous.revoke().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_runtime_revokes_session_when_ready_fails() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        RecordingParticipantHandler::new(),
    );

    let result = runtime
        .run(|| {
            Err(ParticipantRuntimeError::Io(String::from(
                "application failed during startup",
            )))
        })
        .await;

    assert!(matches!(result, Err(ParticipantRuntimeError::Io(_))));
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_participant_rejects_missing_or_invalid_resource_metadata() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let mut handlers = BTreeMap::new();
    handlers.insert(
        resource("documents"),
        Box::new(RecordingParticipantHandler::new()) as Box<dyn TransitionHandler>,
    );
    let runtime =
        ParticipantRuntime::<clustodian::participant::ScopedTransitionHandler>::new_scoped(
            backend.clone(),
            instance_id.clone(),
            handlers,
        )
        .with_lease_ttl(Duration::from_secs(1))
        .unwrap();
    let task = tokio::spawn(runtime.run(|| Ok(())));
    for _ in 0..100 {
        if backend.live_session(&instance_id).await.unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    backend
        .inject_pending_transition(&transition_message(
            "missing-resource-metadata",
            backend
                .live_session(&instance_id)
                .await
                .unwrap()
                .expect("participant registered")
                .wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant reports invalid resource metadata")
        .unwrap();
    assert!(matches!(
        result,
        Err(ParticipantRuntimeError::InvalidMessage)
    ));
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_reports_malformed_queue_events() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        RecordingParticipantHandler::new(),
    );
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(runtime.run(move || {
        ready_tx
            .send(())
            .map_err(|_| ParticipantRuntimeError::Io(String::from("ready receiver dropped")))
    }));
    ready_rx
        .await
        .expect("participant is ready before malformed event");
    backend
        .put_metadata("controller/output/pending-transitions", "not-json")
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant reports malformed queue event")
        .unwrap();
    assert!(matches!(
        result,
        Err(ParticipantRuntimeError::InvalidMessage)
    ));
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_reports_invalid_transition_fields() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        RecordingParticipantHandler::new(),
    );
    let task = tokio::spawn(runtime.run(|| Ok(())));
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    let mut invalid = transition_message(
        "invalid-transition-field",
        session.wire_value(),
        "OFFLINE",
        "STANDBY",
    );
    invalid.from.clear();
    backend.inject_pending_transition(&invalid).await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant reports invalid transition fields")
        .unwrap();
    assert!(matches!(
        result,
        Err(ParticipantRuntimeError::InvalidMessage)
    ));
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_participant_rejects_unsupported_state_models() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"OnlineOffline"}"#,
        )
        .await
        .unwrap();
    let instance_id = instance("node-a");
    let runtime =
        ParticipantRuntime::<clustodian::participant::ScopedTransitionHandler>::new_scoped(
            backend.clone(),
            instance_id.clone(),
            BTreeMap::from([(
                resource("documents"),
                Box::new(RecordingParticipantHandler::new()) as Box<dyn TransitionHandler>,
            )]),
        );
    let task = tokio::spawn(runtime.run(|| Ok(())));
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    backend
        .inject_pending_transition(&transition_message(
            "unsupported-state-model",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant reports unsupported state model")
        .unwrap();
    assert!(matches!(
        result,
        Err(ParticipantRuntimeError::InvalidMessage)
    ));
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_participant_rejects_malformed_resource_metadata() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    backend
        .put_metadata("controller/resources/documents", "not-json")
        .await
        .unwrap();
    let instance_id = instance("node-a");
    let runtime =
        ParticipantRuntime::<clustodian::participant::ScopedTransitionHandler>::new_scoped(
            backend.clone(),
            instance_id.clone(),
            BTreeMap::from([(
                resource("documents"),
                Box::new(RecordingParticipantHandler::new()) as Box<dyn TransitionHandler>,
            )]),
        );
    let task = tokio::spawn(runtime.run(|| Ok(())));
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    backend
        .inject_pending_transition(&transition_message(
            "malformed-resource-metadata",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant reports malformed resource metadata")
        .unwrap();
    assert!(matches!(
        result,
        Err(ParticipantRuntimeError::InvalidMessage)
    ));
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_ignores_messages_for_other_instances() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let handler = RecordingParticipantHandler::new();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    );
    let task = tokio::spawn(runtime.run(|| Ok(())));
    for _ in 0..100 {
        if backend.live_session(&instance_id).await.unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut other_instance = transition_message("other-instance", 1, "OFFLINE", "STANDBY");
    other_instance.instance = String::from("node-b");
    backend
        .put_metadata(
            "controller/output/pending-transitions",
            &serde_json::to_string(&[other_instance]).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(handler.calls().is_empty());
    backend
        .put_metadata("controller/output/pending-transitions", "[]")
        .await
        .unwrap();
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_recovers_a_compacted_pending_queue_watch() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let handler = RecordingParticipantHandler::new();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    );
    let task = tokio::spawn(runtime.run_until(
        async move {
            let _ = shutdown_rx.await;
        },
        move || {
            ready_tx
                .send(())
                .map_err(|_| ParticipantRuntimeError::Io(String::from("ready receiver dropped")))
        },
    ));
    ready_rx.await.expect("participant watch is established");
    let barrier = backend
        .put_metadata("participant-compaction-barrier", "ready")
        .await
        .unwrap();
    backend.compact(barrier).await.unwrap();
    let session = backend
        .live_session(&instance_id)
        .await
        .unwrap()
        .expect("participant remains live");
    backend
        .inject_pending_transition(&transition_message(
            "after-compaction",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_handler_calls(&handler, 1).await;
    wait_for_participant_state(
        &backend,
        &instance_id,
        &resource("documents"),
        &partition("documents_0"),
        "STANDBY",
    )
    .await;
    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant shuts down")
        .unwrap()
        .is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_resumes_pending_queue_watch_after_etcd_restart() {
    let mut fixture = EtcdFixture::new_isolated().expect("etcd is required for participant tests");
    if !fixture.can_control_process() {
        return;
    }
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let handler = RecordingParticipantHandler::new();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    )
    .with_lease_ttl(Duration::from_secs(5))
    .unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(runtime.run(move || {
        ready_tx
            .send(())
            .map_err(|_| ParticipantRuntimeError::Io(String::from("ready receiver dropped")))
    }));
    ready_rx.await.expect("participant watch is established");
    for _ in 0..100 {
        if backend.live_session(&instance_id).await.unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(fixture.stop_process());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(fixture.restart_process().await, "local etcd restarts");
    tokio::time::sleep(Duration::from_millis(500)).await;
    for _ in 0..8 {
        if task.is_finished() {
            let result = task.await.expect("participant task panicked");
            panic!("participant exits during watch recovery: {result:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(handler.calls().is_empty());

    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn participant_recovers_from_brief_keepalive_failure_before_lease_expiry() {
    let mut fixture = EtcdFixture::new_isolated().expect("etcd is required for participant tests");
    if !fixture.can_control_process() {
        return;
    }
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("brief-outage-node");
    let handler = RecordingParticipantHandler::new();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    )
    .with_lease_ttl(Duration::from_secs(10))
    .unwrap();
    let task = tokio::spawn(runtime.run_until(
        async move {
            let _ = shutdown_rx.await;
        },
        move || {
            ready_tx
                .send(())
                .map_err(|_| ParticipantRuntimeError::Io(String::from("ready receiver dropped")))
        },
    ));
    ready_rx.await.expect("participant becomes ready");
    let old_session = backend
        .live_session(&instance_id)
        .await
        .unwrap()
        .expect("participant has a live session");
    let live_key = format!(
        "{}/live/{}",
        fixture.prefix(),
        encoded_test_segment(instance_id.as_str())
    );
    let client = etcd_client::Client::connect([fixture.endpoint.as_str()], None)
        .await
        .unwrap();
    let lease_id = client
        .kv_client()
        .get(live_key, None)
        .await
        .unwrap()
        .kvs()
        .first()
        .expect("live participant key exists")
        .lease();

    assert!(fixture.stop_process());
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(fixture.restart_process().await, "local etcd restarts");
    let restarted = fixture.connect().await.expect("etcd becomes ready again");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        restarted.live_session(&instance_id).await.unwrap(),
        Some(old_session)
    );
    assert!(
        lease_ttl(&fixture.endpoint, lease_id).await >= 8,
        "participant keepalive recovers the original lease before its ten-second TTL expires"
    );

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant shuts down")
        .unwrap()
        .is_ok());
}

#[cfg(feature = "m13-failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn participant_shutdown_during_watch_resume_drains_workers_before_revoke() {
    let mut fixture = EtcdFixture::new_isolated().expect("etcd is required for participant tests");
    if !fixture.can_control_process() {
        return;
    }
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .expect("cluster connects");
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby"}"#,
        )
        .await
        .unwrap();

    let handler = AsyncHandlerHarness::new();
    handler.wait_for_cancellation(true);
    let failpoint = PausedFailpoint::install("participant_before_watch_resume");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        cluster
            .participant("node-a")
            .resource("documents", handler.clone())
            .lease_ttl(Duration::from_secs(5))
            .run_until(async move {
                let _ = shutdown_rx.await;
            }),
    );
    let instance_id = instance("node-a");
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    backend
        .inject_pending_transition(&transition_message(
            "watch-resume-drain",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_async_handler_records(&handler, 1).await;

    assert!(fixture.stop_process());
    failpoint.wait_until_entered().await;
    assert!(fixture.restart_process().await, "local etcd restarts");
    shutdown_tx.send(()).unwrap();
    failpoint.release();
    assert!(tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("participant drains during watch recovery shutdown")
        .unwrap()
        .is_ok());
    assert_eq!(handler.cancelled.load(Ordering::Acquire), 1);
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
    fail::remove("participant_before_watch_resume");
}

#[cfg(feature = "m13-failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn participant_does_not_start_callback_after_session_is_revoked() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let handler = RecordingParticipantHandler::new();
    let failpoint = clustodian::test_support::AsyncPause::install(
        "participant_after_transition_delivery_before_callback",
    );
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    );
    let task = tokio::spawn(runtime.run(|| Ok(())));
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    backend
        .inject_pending_transition(&transition_message(
            "expired-before-callback",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    failpoint.wait_until_entered().await;
    backend.revoke_live(&instance_id, session).await.unwrap();
    failpoint.release();
    wait_for_empty_queue(&backend).await;
    assert!(handler.calls().is_empty());
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_fences_in_flight_transition_across_etcd_restart() {
    let mut fixture = EtcdFixture::new_isolated().expect("etcd is required for participant tests");
    if !fixture.can_control_process() {
        return;
    }
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .expect("cluster connects");
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby"}"#,
        )
        .await
        .unwrap();

    let handler = SessionRecoveryHandler::new();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        cluster
            .participant("node-a")
            .resource("documents", handler.clone())
            .lease_ttl(Duration::from_secs(1))
            .on_ready(move || {
                ready_tx
                    .send(())
                    .map_err(|_| std::io::Error::other("ready receiver dropped"))
            })
            .run_until(async move {
                let _ = shutdown_rx.await;
            }),
    );
    ready_rx.await.expect("participant becomes ready");
    let old_session = backend
        .live_session(&instance("node-a"))
        .await
        .unwrap()
        .expect("participant registered");
    backend
        .inject_pending_transition(&transition_message(
            "reconnect-fence",
            old_session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    for _ in 0..200 {
        if !handler.records().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(fixture.stop_process());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(fixture.restart_process().await, "local etcd restarts");
    backend
        .revoke_live(&instance("node-a"), old_session)
        .await
        .expect("old participant lease can be revoked after restart");
    for _ in 0..300 {
        if handler.was_cancelled() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        handler.was_cancelled(),
        "old transition observes session loss"
    );
    let mut new_session = None;
    for _ in 0..300 {
        if let Some(session) = backend.live_session(&instance("node-a")).await.unwrap() {
            if session > old_session {
                new_session = Some(session);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let new_session = new_session.expect("participant re-registers after restart");
    backend
        .inject_pending_transition(&transition_message(
            "reconnect-fence-new",
            new_session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_recovery_handler_records(&handler, 2).await;
    wait_for_participant_state(
        &backend,
        &instance("node-a"),
        &resource("documents"),
        &partition("documents_0"),
        "STANDBY",
    )
    .await;
    wait_for_empty_queue(&backend).await;
    assert_eq!(
        handler
            .records()
            .iter()
            .map(|record| record.attempt_id.as_str())
            .collect::<Vec<_>>(),
        vec!["reconnect-fence", "reconnect-fence-new"]
    );

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant shuts down")
        .unwrap()
        .is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_runtime_rejects_invalid_election_configuration() {
    let fixture = EtcdFixture::new().expect("etcd is required for runtime integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    for config in [
        ControllerRuntimeConfig {
            cluster: String::from("other"),
            controller_id: String::from("controller"),
            lease_ttl_ms: 1_000,
        },
        ControllerRuntimeConfig {
            cluster: String::from("test"),
            controller_id: String::new(),
            lease_ttl_ms: 1_000,
        },
        ControllerRuntimeConfig {
            cluster: String::from("test"),
            controller_id: String::from("controller"),
            lease_ttl_ms: 0,
        },
    ] {
        assert!(ControllerRuntime::new(backend.clone(), config)
            .await
            .is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_identity_is_atomic_and_enforced_on_connect() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let identity_prefix = format!("{}/identity", fixture.prefix());
    let backend_a = fixture
        .connect_namespace(&identity_prefix, "cluster-a")
        .await
        .expect("etcd becomes ready");
    let backend_b = fixture
        .connect_namespace(&identity_prefix, "cluster-b")
        .await
        .expect("etcd becomes ready");

    let admin_a = ClusterAdmin::new(backend_a);
    let admin_b = ClusterAdmin::new(backend_b);
    let (result_a, result_b) = tokio::join!(
        admin_a.ensure_cluster("cluster-a"),
        admin_b.ensure_cluster("cluster-b"),
    );
    assert_ne!(result_a.is_ok(), result_b.is_ok());

    let first = Cluster::connect(
        ClusterConfig::new("cluster-a")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(format!("{identity_prefix}/connect")),
    )
    .await;
    assert!(first.is_ok());
    let second = Cluster::connect(
        ClusterConfig::new("cluster-b")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(format!("{identity_prefix}/connect")),
    )
    .await;
    assert!(matches!(
        second,
        Err(ApplicationError::Coordination(
            clustodian::coordination::etcd::CoordinationError::InvalidCluster
        ))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_runtime_executes_distinct_partitions_concurrently() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let handler = BlockingParticipantHandler::new();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    );
    let task = tokio::spawn(runtime.run(|| Ok(())));
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };

    let second_revision = backend
        .inject_pending_transition(&transition_message_for_partition(
            "blocked",
            "documents_0",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_partition_entry(&handler, "documents_0").await;

    backend
        .inject_pending_transition(&transition_message_for_partition(
            "independent",
            "documents_1",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_partition_entry(&handler, "documents_1").await;
    let processed = backend
        .get_metadata(&processed_revision_key(&instance_id))
        .await
        .unwrap()
        .and_then(|entry| entry.value().map(str::to_owned));
    assert!(processed.map_or(true, |value| {
        value.parse::<i64>().expect("processed revision is numeric") < second_revision.value()
    }));

    handler.release_blocked();
    wait_for_participant_state(
        &backend,
        &instance_id,
        &resource("documents"),
        &partition("documents_0"),
        "STANDBY",
    )
    .await;
    wait_for_participant_state(
        &backend,
        &instance_id,
        &resource("documents"),
        &partition("documents_1"),
        "STANDBY",
    )
    .await;
    wait_for_empty_queue(&backend).await;

    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_replays_same_partition_replacement_after_old_worker_finishes() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let handler = SupersessionHandler::new();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    );
    let task = tokio::spawn(runtime.run_until(
        async move {
            let _ = shutdown_rx.await;
        },
        || Ok(()),
    ));
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let first_revision = backend
        .inject_pending_transition(&transition_message(
            "M1",
            session.wire_value(),
            "OFFLINE",
            "DROPPED",
        ))
        .await
        .unwrap();
    wait_for_supersession_call(&handler, 1).await;
    backend
        .remove_pending_transition(first_revision, "M1")
        .await
        .unwrap();
    backend
        .inject_pending_transition(&transition_message(
            "M2",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();

    handler.release();
    wait_for_supersession_call(&handler, 2).await;
    wait_for_participant_state(
        &backend,
        &instance_id,
        &resource("documents"),
        &partition("documents_0"),
        "STANDBY",
    )
    .await;
    wait_for_empty_queue(&backend).await;
    assert_eq!(handler.calls(), vec!["M1", "M2"]);

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant shuts down")
        .unwrap()
        .is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_coalesces_multiple_same_partition_replacements() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let handler = SupersessionHandler::new();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    );
    let task = tokio::spawn(runtime.run_until(
        async move {
            let _ = shutdown_rx.await;
        },
        || Ok(()),
    ));
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let first_revision = backend
        .inject_pending_transition(&transition_message(
            "M1",
            session.wire_value(),
            "OFFLINE",
            "DROPPED",
        ))
        .await
        .unwrap();
    wait_for_supersession_call(&handler, 1).await;
    backend
        .remove_pending_transition(first_revision, "M1")
        .await
        .unwrap();
    let second_revision = backend
        .inject_pending_transition(&transition_message(
            "M2",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    backend
        .remove_pending_transition(second_revision, "M2")
        .await
        .unwrap();
    backend
        .inject_pending_transition(&transition_message(
            "M3",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();

    handler.release();
    wait_for_supersession_call(&handler, 2).await;
    wait_for_participant_state(
        &backend,
        &instance_id,
        &resource("documents"),
        &partition("documents_0"),
        "STANDBY",
    )
    .await;
    wait_for_empty_queue(&backend).await;
    assert_eq!(handler.calls(), vec!["M1", "M3"]);

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant shuts down")
        .unwrap()
        .is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_runtime_keeps_session_alive_while_draining_shutdown() {
    let fixture = EtcdFixture::new().expect("etcd is required for Helix integration tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let handler = BlockingParticipantHandler::new();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    )
    .with_lease_ttl(Duration::from_secs(1))
    .unwrap();
    let task = tokio::spawn(runtime.run_until(
        async move {
            let _ = shutdown_rx.await;
        },
        || Ok(()),
    ));
    let session = loop {
        if let Some(session) = backend.live_session(&instance_id).await.unwrap() {
            break session;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    backend
        .inject_pending_transition(&transition_message(
            "shutdown-drain",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_partition_entry(&handler, "documents_0").await;

    shutdown_tx.send(()).unwrap();
    for _ in 0..15 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            backend.live_session(&instance_id).await.unwrap(),
            Some(session)
        );
    }

    handler.release_blocked();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant drains the blocked handler")
        .unwrap()
        .is_ok());
    assert_eq!(backend.live_session(&instance_id).await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_facade_participant_runs_resource_handler_lifecycle() {
    let fixture = EtcdFixture::new().expect("etcd is required for facade participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .expect("cluster connects");
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby"}"#,
        )
        .await
        .unwrap();

    let instance_id = instance("node-a");
    let handler = RecordingParticipantHandler::new();
    let ready_handler = handler.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        cluster
            .participant("node-a")
            .resource("documents", handler.clone())
            .on_ready(move || {
                ready_tx
                    .send(())
                    .map_err(|_| std::io::Error::other("ready receiver dropped"))
            })
            .run_until(async move {
                let _ = shutdown_rx.await;
            }),
    );
    ready_rx.await.expect("facade participant becomes ready");
    let session = backend
        .live_session(&instance_id)
        .await
        .unwrap()
        .expect("facade participant registered");
    backend
        .inject_pending_transition(&transition_message(
            "facade-promote",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_handler_calls(&ready_handler, 1).await;
    wait_for_participant_state(
        &backend,
        &instance_id,
        &resource("documents"),
        &partition("documents_0"),
        "STANDBY",
    )
    .await;

    for (message_id, from, to, expected_state) in [
        ("facade-elect", "STANDBY", "LEADER", "LEADER"),
        ("facade-demote", "LEADER", "STANDBY", "STANDBY"),
        ("facade-offline", "STANDBY", "OFFLINE", "OFFLINE"),
    ] {
        backend
            .inject_pending_transition(&transition_message(
                message_id,
                session.wire_value(),
                from,
                to,
            ))
            .await
            .unwrap();
        wait_for_handler_calls(&ready_handler, ready_handler.calls().len() + 1).await;
        wait_for_participant_state(
            &backend,
            &instance_id,
            &resource("documents"),
            &partition("documents_0"),
            expected_state,
        )
        .await;
        wait_for_empty_queue(&backend).await;
    }
    backend
        .inject_pending_transition(&transition_message_for_partition(
            "facade-drop",
            "documents_drop",
            session.wire_value(),
            "OFFLINE",
            "DROPPED",
        ))
        .await
        .unwrap();
    wait_for_handler_calls(&ready_handler, 5).await;
    wait_for_empty_queue(&backend).await;

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("facade participant shuts down")
        .unwrap()
        .is_ok());
    assert_eq!(
        handler.calls(),
        vec![
            "documents:documents_0:OFFLINE->STANDBY",
            "documents:documents_0:STANDBY->LEADER",
            "documents:documents_0:LEADER->STANDBY",
            "documents:documents_0:STANDBY->OFFLINE",
            "documents:documents_drop:OFFLINE->DROPPED",
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_facade_harness_runs_typed_concurrent_and_failed_transitions() {
    let fixture = EtcdFixture::new().expect("etcd is required for async facade tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .expect("cluster connects");
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby"}"#,
        )
        .await
        .unwrap();

    let handler = AsyncHandlerHarness::new();
    let ready_handler = handler.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        cluster
            .participant("node-a")
            .resource("documents", handler.clone())
            .on_ready(move || {
                ready_tx
                    .send(())
                    .map_err(|_| std::io::Error::other("ready receiver dropped"))
            })
            .run_until(async move {
                let _ = shutdown_rx.await;
            }),
    );
    ready_rx
        .await
        .expect("async facade participant becomes ready");
    let session = backend
        .live_session(&instance("node-a"))
        .await
        .unwrap()
        .expect("async facade participant registered");

    for (message_id, partition_name) in [("async-p0", "documents_0"), ("async-p1", "documents_1")] {
        backend
            .inject_pending_transition(&transition_message_for_partition(
                message_id,
                partition_name,
                session.wire_value(),
                "OFFLINE",
                "STANDBY",
            ))
            .await
            .unwrap();
    }
    wait_for_async_handler_records(&ready_handler, 2).await;
    wait_for_participant_state(
        &backend,
        &instance("node-a"),
        &resource("documents"),
        &partition("documents_0"),
        "STANDBY",
    )
    .await;
    wait_for_participant_state(
        &backend,
        &instance("node-a"),
        &resource("documents"),
        &partition("documents_1"),
        "STANDBY",
    )
    .await;

    assert!(ready_handler.max_active.load(Ordering::Acquire) >= 2);
    let records = ready_handler.records();
    assert_eq!(records.len(), 2);
    assert!(records.contains(&AsyncTransitionRecord {
        partition: String::from("documents_0"),
        source: String::from("OFFLINE"),
        target: String::from("STANDBY"),
        attempt_id: String::from("async-p0"),
    }));
    assert!(records.contains(&AsyncTransitionRecord {
        partition: String::from("documents_1"),
        source: String::from("OFFLINE"),
        target: String::from("STANDBY"),
        attempt_id: String::from("async-p1"),
    }));

    ready_handler.fail(true);
    backend
        .inject_pending_transition(&transition_message_for_partition(
            "async-failure",
            "documents_0",
            session.wire_value(),
            "STANDBY",
            "LEADER",
        ))
        .await
        .unwrap();
    wait_for_async_handler_records(&ready_handler, 3).await;
    wait_for_participant_state(
        &backend,
        &instance("node-a"),
        &resource("documents"),
        &partition("documents_0"),
        "ERROR",
    )
    .await;
    wait_for_empty_queue(&backend).await;

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("async facade participant shuts down")
        .unwrap()
        .is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_facade_harness_cancels_in_flight_transition_on_shutdown() {
    let fixture = EtcdFixture::new().expect("etcd is required for async facade tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let cluster = Cluster::connect(
        ClusterConfig::new("test")
            .etcd_endpoints([fixture.endpoint.clone()])
            .namespace(fixture.prefix().to_owned()),
    )
    .await
    .expect("cluster connects");
    backend
        .put_metadata(
            "controller/resources/documents",
            r#"{"name":"documents","state_model":"LeaderStandby"}"#,
        )
        .await
        .unwrap();

    let handler = AsyncHandlerHarness::new();
    handler.wait_for_cancellation(true);
    let ready_handler = handler.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        cluster
            .participant("node-a")
            .resource("documents", handler.clone())
            .on_ready(move || {
                ready_tx
                    .send(())
                    .map_err(|_| std::io::Error::other("ready receiver dropped"))
            })
            .run_until(async move {
                let _ = shutdown_rx.await;
            }),
    );
    ready_rx
        .await
        .expect("async facade participant becomes ready");
    let session = backend
        .live_session(&instance("node-a"))
        .await
        .unwrap()
        .expect("async facade participant registered");
    backend
        .inject_pending_transition(&transition_message(
            "async-cancel",
            session.wire_value(),
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_async_handler_records(&ready_handler, 1).await;

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("async facade participant cancels work")
        .unwrap()
        .is_ok());
    assert_eq!(ready_handler.cancelled.load(Ordering::Acquire), 1);
    assert_eq!(
        backend.live_session(&instance("node-a")).await.unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn participant_processes_a_preexisting_queue_and_cleans_non_transitions() {
    let fixture = EtcdFixture::new().expect("etcd is required for participant tests");
    let backend = fixture.connect().await.expect("etcd becomes ready");
    let instance_id = instance("node-a");
    let previous = backend
        .register(
            instance_id.clone(),
            RegistrationOptions::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap();
    let expected_session = previous.session_id().wire_value() + 1;
    backend
        .inject_pending_transition(&transition_message(
            "preexisting",
            expected_session,
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    previous.revoke().await.unwrap();

    let handler = RecordingParticipantHandler::new();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let runtime = ParticipantRuntime::new(
        backend.clone(),
        instance_id.clone(),
        leader_standby(),
        handler.clone(),
    )
    .with_lease_ttl(Duration::from_secs(1))
    .unwrap();
    let task = tokio::spawn(runtime.run_until(
        async move {
            let _ = shutdown_rx.await;
        },
        || Ok(()),
    ));
    wait_for_handler_calls(&handler, 1).await;
    wait_for_participant_state(
        &backend,
        &instance_id,
        &resource("documents"),
        &partition("documents_0"),
        "STANDBY",
    )
    .await;

    backend
        .inject_pending_transition(&transition_message(
            "already-applied",
            expected_session + 1,
            "OFFLINE",
            "STANDBY",
        ))
        .await
        .unwrap();
    wait_for_empty_queue(&backend).await;
    assert_eq!(handler.calls().len(), 1);

    backend
        .inject_pending_transition(&transition_message(
            "non-adjacent",
            backend
                .live_session(&instance_id)
                .await
                .unwrap()
                .expect("participant remains live")
                .wire_value(),
            "STANDBY",
            "DROPPED",
        ))
        .await
        .unwrap();
    wait_for_empty_queue(&backend).await;
    assert_eq!(handler.calls().len(), 1);

    shutdown_tx.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("participant shuts down")
        .unwrap()
        .is_ok());
}
