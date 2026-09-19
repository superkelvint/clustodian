use crate::coordination::etcd::{ParticipantCompletionFence, Revision, WatchCursor};
use crate::election::{ControllerAuthority, ControllerCommitFence};
use crate::participant::CompletionState;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

const DEFAULT_ITERATIONS: usize = 2_000;
const DEFAULT_PCT_DEPTH: usize = 3;

fn revision(value: i64) -> Revision {
    Revision::new(value).expect("test revision is positive")
}

fn run_async(future: impl Future<Output = ()> + 'static) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("Shuttle Tokio runtime builds");
    runtime.block_on(future);
}

fn iterations() -> usize {
    std::env::var("CLUSTODIAN_SHUTTLE_ITERATIONS")
        .ok()
        .map(|value| {
            value
                .parse()
                .expect("CLUSTODIAN_SHUTTLE_ITERATIONS must be a positive integer")
        })
        .unwrap_or(DEFAULT_ITERATIONS)
}

fn pct_depth() -> usize {
    std::env::var("CLUSTODIAN_SHUTTLE_PCT_DEPTH")
        .ok()
        .map(|value| {
            value
                .parse()
                .expect("CLUSTODIAN_SHUTTLE_PCT_DEPTH must be a positive integer")
        })
        .unwrap_or(DEFAULT_PCT_DEPTH)
}

fn seed() -> Option<u64> {
    std::env::var("SHUTTLE_RANDOM_SEED")
        .or_else(|_| std::env::var("SHUTTLE_PERSIST_SEED"))
        .ok()
        .map(|value| value.parse().expect("SHUTTLE_*_SEED must be a u64"))
}

fn replay_schedule(test: fn()) -> bool {
    let Ok(schedule) = std::env::var("SHUTTLE_SCHEDULE") else {
        return false;
    };
    shuttle::replay(test, &schedule);
    true
}

fn run_dfs(test: fn()) {
    if !replay_schedule(test) {
        shuttle::check_dfs(test, None);
    }
}

fn run_pct_and_random(test: fn()) {
    if replay_schedule(test) {
        return;
    }
    let cases = iterations();
    let depth = pct_depth();
    if let Some(seed) = seed() {
        let scheduler = shuttle::scheduler::PctScheduler::new_from_seed(seed, depth, cases);
        shuttle::Runner::new(scheduler, Default::default()).run(test);
        shuttle::check_random_with_seed(test, seed, cases);
    } else {
        shuttle::check_pct(test, cases, depth);
        shuttle::check_random(test, cases);
    }
}

fn participant_fence(
    instance: &str,
    session: u64,
    queue_revision: i64,
    message: &str,
) -> ParticipantCompletionFence {
    ParticipantCompletionFence::new(
        crate::model::InstanceId::new(instance).expect("test instance is valid"),
        crate::model::SessionId::from_wire_value(session),
        revision(queue_revision),
        message.to_owned(),
    )
}

#[derive(Clone)]
struct ParticipantStore {
    state: Arc<Mutex<ParticipantStoreState>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ParticipantEvent {
    Replacement,
    Completion { accepted: bool },
}

struct ParticipantStoreState {
    instance: crate::model::InstanceId,
    active_session: crate::model::SessionId,
    queue_revision: Revision,
    pending: BTreeSet<String>,
    published: BTreeMap<crate::model::SessionId, String>,
    frontiers: BTreeMap<crate::model::SessionId, Revision>,
    events: Vec<ParticipantEvent>,
}

impl ParticipantStore {
    fn new(session: u64, queue_revision: i64, message: &str) -> Self {
        Self {
            state: Arc::new(Mutex::new(ParticipantStoreState {
                instance: crate::model::InstanceId::new("node-a").expect("test instance is valid"),
                active_session: crate::model::SessionId::from_wire_value(session),
                queue_revision: revision(queue_revision),
                pending: BTreeSet::from([message.to_owned()]),
                published: BTreeMap::new(),
                frontiers: BTreeMap::new(),
                events: Vec::new(),
            })),
        }
    }

    async fn replace(&self, session: u64, queue_revision: i64, message: Option<&str>) {
        let mut state = self.state.lock().await;
        state.active_session = crate::model::SessionId::from_wire_value(session);
        state.queue_revision = revision(queue_revision);
        state.events.push(ParticipantEvent::Replacement);
        if let Some(message) = message {
            state.pending.insert(message.to_owned());
        }
    }

    async fn complete(&self, fence: &ParticipantCompletionFence, resulting_state: &str) -> bool {
        let mut state = self.state.lock().await;
        let accepted = fence.instance == state.instance
            && fence.queue_revision == state.queue_revision
            && fence.matches(
                state.active_session,
                state.pending.contains(&fence.message_id),
            );
        if accepted {
            state.pending.remove(&fence.message_id);
            state
                .published
                .insert(fence.session_id, resulting_state.to_owned());
            state
                .frontiers
                .insert(fence.session_id, fence.queue_revision);
        }
        state.events.push(ParticipantEvent::Completion { accepted });
        accepted
    }

    async fn snapshot(&self) -> ParticipantStoreSnapshot {
        let state = self.state.lock().await;
        ParticipantStoreSnapshot {
            active_session: state.active_session,
            pending: state.pending.clone(),
            published: state.published.clone(),
            frontiers: state.frontiers.clone(),
            events: state.events.clone(),
        }
    }
}

struct ParticipantStoreSnapshot {
    active_session: crate::model::SessionId,
    pending: BTreeSet<String>,
    published: BTreeMap<crate::model::SessionId, String>,
    frontiers: BTreeMap<crate::model::SessionId, Revision>,
    events: Vec<ParticipantEvent>,
}

fn shuttle_session_expiry_before_completion_scenario() {
    run_async(async {
        let store = ParticipantStore::new(1, 10, "M1");
        let fence = participant_fence("node-a", 1, 10, "M1");
        let completion_store = store.clone();
        let completion = tokio::spawn(async move {
            tokio::task::yield_now().await;
            completion_store.complete(&fence, "STANDBY").await
        });

        let replacement_store = store.clone();
        let replacement = tokio::spawn(async move {
            tokio::task::yield_now().await;
            replacement_store.replace(2, 10, None).await;
        });

        let completion_applied = completion.await.expect("completion task joins");
        replacement.await.expect("replacement task joins");
        let snapshot = store.snapshot().await;
        let session_two = crate::model::SessionId::from_wire_value(2);
        assert_eq!(snapshot.active_session, session_two);
        assert!(!snapshot.published.contains_key(&session_two));
        let replacement_position = snapshot
            .events
            .iter()
            .position(|event| matches!(event, &ParticipantEvent::Replacement));
        let completion_position = snapshot.events.iter().position(|event| {
            matches!(
                event,
                &ParticipantEvent::Completion { accepted }
                    if accepted == completion_applied
            )
        });
        if replacement_position.is_some_and(|replacement| {
            completion_position.is_some_and(|completion| replacement < completion)
        }) {
            assert!(!completion_applied);
        }
    });
}

#[test]
fn shuttle_session_expiry_before_completion_is_fenced() {
    run_dfs(shuttle_session_expiry_before_completion_scenario);
}

fn shuttle_completion_rejects_unrelated_queue_rewrite_scenario() {
    run_async(async {
        let store = ParticipantStore::new(1, 10, "M1");
        let fence = participant_fence("node-a", 1, 10, "M1");
        store.replace(1, 11, Some("M2")).await;
        assert!(!store.complete(&fence, "STANDBY").await);
        let snapshot = store.snapshot().await;
        assert_eq!(
            snapshot.pending,
            BTreeSet::from([String::from("M1"), String::from("M2")])
        );
        assert!(snapshot.published.is_empty());
    });
}

#[test]
fn shuttle_completion_matches_production_queue_revision_contract() {
    run_dfs(shuttle_completion_rejects_unrelated_queue_rewrite_scenario);
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ControllerEvent {
    Replacement,
    Publication { accepted: bool },
}

#[derive(Clone)]
struct ControllerStore {
    state: Arc<Mutex<ControllerStoreState>>,
}

struct ControllerStoreState {
    active_controller_id: String,
    active_lease_id: i64,
    input_revision: Revision,
    live_sessions: BTreeMap<crate::model::InstanceId, crate::model::SessionId>,
    events: Vec<ControllerEvent>,
}

impl ControllerStore {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ControllerStoreState {
                active_controller_id: "controller-a".to_owned(),
                active_lease_id: 1,
                input_revision: revision(20),
                live_sessions: BTreeMap::new(),
                events: Vec::new(),
            })),
        }
    }

    async fn replace(&self, controller_id: &str, lease_id: i64) {
        let mut state = self.state.lock().await;
        state.active_controller_id = controller_id.to_owned();
        state.active_lease_id = lease_id;
        state.events.push(ControllerEvent::Replacement);
    }

    async fn publish(&self, fence: &ControllerCommitFence) -> bool {
        let mut state = self.state.lock().await;
        let accepted = fence.matches(
            &state.active_controller_id,
            state.active_lease_id,
            state.input_revision,
            &state.live_sessions,
        );
        state.events.push(ControllerEvent::Publication { accepted });
        accepted
    }

    async fn events(&self) -> Vec<ControllerEvent> {
        self.state.lock().await.events.clone()
    }
}

fn authority(controller_id: &str, lease_id: i64) -> ControllerAuthority {
    ControllerAuthority {
        namespace: "shuttle-test".to_owned(),
        controller_id: controller_id.to_owned(),
        lease_id,
        lost: Arc::new(AtomicBool::new(false)),
    }
}

fn shuttle_controller_replacement_between_compute_and_publish_scenario() {
    run_async(async {
        let store = ControllerStore::new();
        let fence = authority("controller-a", 1).commit_fence(revision(20), BTreeMap::new());
        let publication_store = store.clone();
        let publication = tokio::spawn(async move {
            tokio::task::yield_now().await;
            publication_store.publish(&fence).await
        });

        let replacement_store = store.clone();
        let replacement = tokio::spawn(async move {
            tokio::task::yield_now().await;
            replacement_store.replace("controller-b", 2).await;
        });

        let publication_accepted = publication.await.expect("publication task joins");
        replacement.await.expect("replacement task joins");
        let events = store.events().await;
        let mut replaced = false;
        for event in events {
            match event {
                ControllerEvent::Replacement => replaced = true,
                ControllerEvent::Publication { accepted } if replaced => {
                    assert!(!accepted);
                }
                ControllerEvent::Publication { .. } => {}
            }
        }
        assert!(publication_accepted || events_contain_rejection(&store).await);
    });
}

async fn events_contain_rejection(store: &ControllerStore) -> bool {
    store
        .events()
        .await
        .into_iter()
        .any(|event| event == ControllerEvent::Publication { accepted: false })
}

#[test]
fn shuttle_controller_replacement_between_compute_and_publish_is_fenced() {
    run_pct_and_random(shuttle_controller_replacement_between_compute_and_publish_scenario);
}

fn shuttle_completion_frontier_scenario() {
    run_async(async {
        let state = Arc::new(Mutex::new(CompletionState::new()));
        let at_revision = vec!["M1".to_owned(), "M2".to_owned()];
        state.lock().await.observe(revision(30), &at_revision);

        let (tx, mut rx) = mpsc::channel(2);
        let mut releases = BTreeMap::new();
        let mut handles = Vec::new();
        for message_id in ["M1", "M2"] {
            let (release_tx, release_rx) = oneshot::channel();
            releases.insert(message_id, release_tx);
            let task_state = state.clone();
            let task_tx = tx.clone();
            handles.push(tokio::spawn(async move {
                let mut state = task_state.lock().await;
                let advanced = state.complete(message_id);
                let frontier = state.frontier();
                task_tx
                    .send((message_id, advanced, frontier))
                    .await
                    .expect("completion observer remains");
                release_rx.await.expect("completion release arrives");
            }));
        }
        drop(tx);

        let first = rx.recv().await.expect("first completion arrives");
        assert_eq!(first.1, None);
        assert_eq!(first.2, None);
        releases
            .remove(first.0)
            .expect("first completion has a release")
            .send(())
            .expect("first completion is waiting");

        let second = rx.recv().await.expect("second completion arrives");
        assert_eq!(second.1, Some(revision(30)));
        assert_eq!(second.2, Some(revision(30)));
        releases
            .remove(second.0)
            .expect("second completion has a release")
            .send(())
            .expect("second completion is waiting");
        for handle in handles {
            handle.await.expect("completion task joins");
        }

        let mut state = CompletionState::new();
        state.observe(revision(30), &["M1".to_owned(), "M2".to_owned()]);
        state.observe(revision(31), &["M3".to_owned()]);
        assert_eq!(state.complete("M3"), None);
        assert_eq!(state.frontier(), None);
        assert_eq!(state.complete("M1"), None);
        assert_eq!(state.frontier(), None);
        assert_eq!(state.complete("M2"), Some(revision(31)));
        assert_eq!(state.frontier(), Some(revision(31)));
    });
}

#[test]
fn shuttle_completion_frontier_waits_for_all_work_at_revision() {
    if !replay_schedule(shuttle_completion_frontier_scenario) {
        shuttle::check_dfs(shuttle_completion_frontier_scenario, Some(100));
    }
}

fn shuttle_old_callback_cannot_commit_after_participant_reconnect_scenario() {
    run_async(async {
        let store = ParticipantStore::new(1, 40, "M1");
        let fence = participant_fence("node-a", 1, 40, "M1");
        let (callback_started_tx, callback_started_rx) = oneshot::channel();

        let old_store = store.clone();
        let old_callback = tokio::spawn(async move {
            callback_started_tx
                .send(())
                .expect("callback-start observer remains");
            tokio::task::yield_now().await;
            old_store.complete(&fence, "STANDBY").await
        });

        callback_started_rx
            .await
            .expect("old callback starts before reconnect");
        let reconnect_store = store.clone();
        let reconnect = tokio::spawn(async move {
            tokio::task::yield_now().await;
            reconnect_store.replace(2, 41, Some("M2")).await;
        });

        old_callback.await.expect("old callback task joins");
        reconnect.await.expect("reconnect task joins");
        let snapshot = store.snapshot().await;
        let session_two = crate::model::SessionId::from_wire_value(2);
        assert_eq!(snapshot.active_session, session_two);
        assert!(snapshot.pending.contains("M2"));
        assert!(!snapshot.published.contains_key(&session_two));
        assert!(!snapshot.frontiers.contains_key(&session_two));
    });
}

#[test]
fn shuttle_old_callback_cannot_commit_after_participant_reconnect() {
    run_pct_and_random(shuttle_old_callback_cannot_commit_after_participant_reconnect_scenario);
}

#[derive(Clone)]
struct WatchModel {
    state: Arc<Mutex<WatchModelState>>,
}

struct WatchModelState {
    revision: Revision,
    value: String,
    history: BTreeMap<Revision, String>,
}

impl WatchModel {
    fn new() -> Self {
        let initial_revision = revision(50);
        Self {
            state: Arc::new(Mutex::new(WatchModelState {
                revision: initial_revision,
                value: "initial".to_owned(),
                history: BTreeMap::from([(initial_revision, "initial".to_owned())]),
            })),
        }
    }

    async fn snapshot(&self) -> (Revision, String) {
        let state = self.state.lock().await;
        (state.revision, state.value.clone())
    }

    async fn mutate(&self) -> Revision {
        let mut state = self.state.lock().await;
        let next = revision(state.revision.value() + 1);
        state.revision = next;
        state.value = "E".to_owned();
        state.history.insert(next, "E".to_owned());
        next
    }

    async fn events_from(&self, start: Revision) -> Vec<(Revision, String)> {
        self.state
            .lock()
            .await
            .history
            .range(start..)
            .map(|(revision, value)| (*revision, value.clone()))
            .collect()
    }
}

#[derive(Debug)]
struct RecoveryResult {
    cursor: WatchCursor,
    snapshot_revision: Revision,
    snapshot_value: String,
    applied: BTreeSet<String>,
}

fn shuttle_watch_compaction_recovery_scenario() {
    run_async(async {
        let model = WatchModel::new();
        let mut initial_cursor = WatchCursor::new(revision(50));
        assert!(initial_cursor.accept(revision(50), "initial"));
        let (recovery_tx, recovery_rx) = oneshot::channel();

        let recovery_model = model.clone();
        let recovery = tokio::spawn(async move {
            tokio::task::yield_now().await;
            let (snapshot_revision, snapshot_value) = recovery_model.snapshot().await;
            tokio::task::yield_now().await;
            let mut cursor = initial_cursor;
            let start_revision = cursor
                .reset_after_recovery(snapshot_revision)
                .expect("recovery revision increments");
            assert_eq!(cursor.start_revision(), start_revision);
            assert_eq!(start_revision, snapshot_revision.next().unwrap());
            tokio::task::yield_now().await;
            let mut applied = BTreeSet::new();
            if snapshot_value == "E" {
                applied.insert("E".to_owned());
            }
            for (event_revision, value) in recovery_model.events_from(start_revision).await {
                if cursor.accept(event_revision, "mutation") {
                    assert_eq!(value, "E");
                    assert!(applied.insert("E".to_owned()));
                }
            }
            recovery_tx
                .send(RecoveryResult {
                    cursor,
                    snapshot_revision,
                    snapshot_value,
                    applied,
                })
                .expect("recovery observer remains");
        });

        let mutation_model = model.clone();
        let mutation = tokio::spawn(async move {
            tokio::task::yield_now().await;
            mutation_model.mutate().await;
        });

        let mut result = recovery_rx.await.expect("recovery task reports");
        mutation.await.expect("mutation task joins");
        recovery.await.expect("recovery task joins");
        assert_eq!(
            result.cursor.start_revision(),
            result.snapshot_revision.next().unwrap()
        );
        if result.snapshot_value != "E" {
            for (event_revision, value) in model
                .events_from(result.snapshot_revision.next().unwrap())
                .await
            {
                if result.cursor.accept(event_revision, "mutation") {
                    assert_eq!(value, "E");
                    assert!(result.applied.insert("E".to_owned()));
                }
            }
        }
        assert_eq!(result.applied, BTreeSet::from(["E".to_owned()]));
    });
}

#[test]
fn shuttle_watch_compaction_recovery_has_no_gap_or_duplicate() {
    run_pct_and_random(shuttle_watch_compaction_recovery_scenario);
}

fn shuttle_same_controller_id_old_lease_scenario() {
    run_async(async {
        let store = ControllerStore::new();
        let old_fence = authority("controller-a", 1).commit_fence(revision(20), BTreeMap::new());
        let publication_store = store.clone();
        let publication = tokio::spawn(async move {
            tokio::task::yield_now().await;
            publication_store.publish(&old_fence).await
        });

        let replacement_store = store.clone();
        let replacement = tokio::spawn(async move {
            tokio::task::yield_now().await;
            replacement_store.replace("controller-a", 2).await;
        });

        let accepted = publication.await.expect("publication task joins");
        replacement.await.expect("replacement task joins");
        let events = store.events().await;
        assert!(events.contains(&ControllerEvent::Replacement));
        if events
            .iter()
            .position(|event| *event == ControllerEvent::Replacement)
            .is_some_and(|replacement_position| {
                events
                    .iter()
                    .position(|event| *event == ControllerEvent::Publication { accepted })
                    .is_some_and(|publication_position| replacement_position < publication_position)
            })
        {
            assert!(!accepted);
        }
        assert!(accepted || events.contains(&ControllerEvent::Publication { accepted: false }));
    });
}

#[test]
fn shuttle_same_controller_id_old_lease_cannot_publish() {
    run_dfs(shuttle_same_controller_id_old_lease_scenario);
}

#[test]
fn shuttle_shared_protocol_has_no_uncontrolled_nondeterminism() {
    if !replay_schedule(shuttle_session_expiry_before_completion_scenario) {
        shuttle::check_uncontrolled_nondeterminism(
            shuttle_session_expiry_before_completion_scenario,
            8,
        );
    }
}
