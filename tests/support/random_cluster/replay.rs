use super::action::ClusterAction;
use super::model::ClusterModel;
use clustodian::observe::ClusterSnapshot;

// Keep the report arguments explicit so each failure site can pass borrowed
// state without allocating a second context object.
#[allow(clippy::too_many_arguments)]
pub fn failure_report(
    seed: u64,
    steps: usize,
    step: usize,
    action: Option<&ClusterAction>,
    trace: &[ClusterAction],
    model: &ClusterModel,
    snapshot: Option<&ClusterSnapshot>,
    error: &str,
) -> String {
    let action_json =
        serde_json::to_string_pretty(trace).expect("random action trace is serializable");
    let model_json = serde_json::to_string_pretty(model).expect("random model is serializable");
    let snapshot_json = snapshot
        .map(|value| serde_json::to_string_pretty(value).expect("snapshot is serializable"))
        .unwrap_or_else(|| String::from("null"));
    format!(
        "randomized cluster state-machine failure: {error}\nseed: {seed}\nstep: {step}\ncurrent action: {action:?}\nreplay: CLUSTODIAN_RS_RANDOM_SEED={seed} CLUSTODIAN_RS_RANDOM_STEPS={steps} cargo test -p clustodian randomized_cluster_state_machine_smoke -- --nocapture\naction trace JSON:\n{action_json}\nreference model JSON:\n{model_json}\nlast observed cluster snapshot JSON:\n{snapshot_json}"
    )
}
