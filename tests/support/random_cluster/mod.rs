mod action;
mod invariants;
mod model;
mod replay;
mod runtime;

use crate::support::EtcdFixture;
use action::{next_action, valid_actions, ClusterAction};
use model::ClusterModel;
use rand_chacha::rand_core::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use replay::failure_report;
use runtime::RandomCluster;

const DEFAULT_SEEDS: &[u64] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0x1234_5678, 0xdead_beef];

pub async fn run_smoke() -> Result<(), String> {
    // Keep the ordinary default-feature regression suite bounded. Broader
    // randomized coverage is available through the environment overrides or
    // the ignored stress test below.
    run_lane(1, 8).await
}

pub async fn run_stress() -> Result<(), String> {
    run_lane(250, 200).await
}

async fn run_lane(default_cases: usize, default_steps: usize) -> Result<(), String> {
    let (seeds, steps) = configuration(default_cases, default_steps)?;
    let fixture = EtcdFixture::new().ok_or_else(|| {
        String::from("real etcd is required for randomized cluster state-machine tests")
    })?;
    for (case_index, seed) in seeds.into_iter().enumerate() {
        run_case(&fixture, seed, case_index, steps).await?;
    }
    Ok(())
}

fn configuration(default_cases: usize, default_steps: usize) -> Result<(Vec<u64>, usize), String> {
    let steps = environment_usize("CLUSTODIAN_RS_RANDOM_STEPS")?.unwrap_or(default_steps);
    let seeds = if let Some(seed) = environment_u64("CLUSTODIAN_RS_RANDOM_SEED")? {
        vec![seed]
    } else {
        let cases = environment_usize("CLUSTODIAN_RS_RANDOM_CASES")?.unwrap_or(default_cases);
        deterministic_seeds(cases)
    };
    Ok((seeds, steps))
}

fn deterministic_seeds(count: usize) -> Vec<u64> {
    let mut corpus = DEFAULT_SEEDS
        .iter()
        .copied()
        .take(count)
        .collect::<Vec<_>>();
    if corpus.len() < count {
        let mut rng = ChaCha8Rng::seed_from_u64(0x5eed_cafe_1234_5678);
        corpus.extend((corpus.len()..count).map(|_| rng.next_u64()));
    }
    corpus
}

async fn run_case(
    fixture: &EtcdFixture,
    seed: u64,
    case_index: usize,
    steps: usize,
) -> Result<(), String> {
    let prefix = format!(
        "clustodian-random-{}/case-{case_index}-seed-{seed:016x}",
        std::process::id()
    );
    let model = ClusterModel::initial();
    let mut cluster = match RandomCluster::new(fixture, seed, case_index).await {
        Ok(cluster) => cluster,
        Err(error) => {
            let _ = fixture.cleanup_namespace(&prefix).await;
            return Err(failure_report(
                seed,
                steps,
                0,
                None,
                &[],
                &model,
                None,
                &error,
            ));
        }
    };
    let result = execute_case(&mut cluster, seed, steps).await;
    let cleanup = cluster.shutdown().await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(format!("random case cleanup failed: {error}")),
        (Err(error), Err(cleanup_error)) => Err(format!(
            "{error}\nrandom case cleanup failed: {cleanup_error}"
        )),
    }
}

async fn execute_case(
    cluster: &mut RandomCluster<'_>,
    seed: u64,
    steps: usize,
) -> Result<(), String> {
    let mut model = ClusterModel::initial();
    let mut trace = Vec::with_capacity(steps);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    if let Err(error) = cluster.start_initial().await {
        return Err(failure_report(
            seed, steps, 0, None, &trace, &model, None, &error,
        ));
    }
    if let Err(error) = cluster.wait_until_initial_converged(&model).await {
        let snapshot = cluster.snapshot().await.ok();
        return Err(failure_report(
            seed,
            steps,
            0,
            None,
            &trace,
            &model,
            snapshot.as_ref(),
            &error,
        ));
    }
    cluster.reset_message_history();
    let initial_snapshot = cluster
        .snapshot()
        .await
        .map_err(|error| failure_report(seed, steps, 0, None, &trace, &model, None, &error))?;
    if let Err(error) = cluster.check_safety(&model, &initial_snapshot) {
        return Err(failure_report(
            seed,
            steps,
            0,
            None,
            &trace,
            &model,
            Some(&initial_snapshot),
            &error,
        ));
    }

    for step in 0..steps {
        let candidates = valid_actions(&model);
        if candidates.is_empty() {
            return Err(failure_report(
                seed,
                steps,
                step,
                None,
                &trace,
                &model,
                cluster.snapshot().await.ok().as_ref(),
                "the model produced no valid actions",
            ));
        }
        let action = next_action(&model, &mut rng);
        if !candidates.contains(&action) {
            return Err(failure_report(
                seed,
                steps,
                step,
                Some(&action),
                &trace,
                &model,
                cluster.snapshot().await.ok().as_ref(),
                "action generator returned an action outside the valid-action set",
            ));
        }
        trace.push(action.clone());
        if matches!(
            action,
            ClusterAction::StopController { .. } | ClusterAction::StartController { .. }
        ) {
            let before = cluster
                .wait_until_controller_caught_up()
                .await
                .map_err(|error| {
                    failure_report(
                        seed,
                        steps,
                        step,
                        Some(&action),
                        &trace,
                        &model,
                        None,
                        &error,
                    )
                })?;
            if let Err(error) = cluster.check_safety(&model, &before) {
                return Err(failure_report(
                    seed,
                    steps,
                    step,
                    Some(&action),
                    &trace,
                    &model,
                    Some(&before),
                    &error,
                ));
            }
            let authority_can_change = match &action {
                ClusterAction::StopController { controller } => before
                    .controllers
                    .active
                    .iter()
                    .any(|active| active == controller),
                ClusterAction::StartController { .. } => before.controllers.active.is_empty(),
                _ => false,
            };
            if authority_can_change {
                cluster.note_controller_failover();
            }
        }
        if let Err(error) = cluster.execute(&action).await {
            let snapshot = cluster.snapshot().await.ok();
            return Err(failure_report(
                seed,
                steps,
                step,
                Some(&action),
                &trace,
                &model,
                snapshot.as_ref(),
                &error,
            ));
        }
        if !cluster.action_rejected() {
            model.apply(&action);
        }
        let snapshot = match cluster.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Err(failure_report(
                    seed,
                    steps,
                    step,
                    Some(&action),
                    &trace,
                    &model,
                    None,
                    &error,
                ))
            }
        };
        if let Err(error) = cluster.check_safety(&model, &snapshot) {
            return Err(failure_report(
                seed,
                steps,
                step,
                Some(&action),
                &trace,
                &model,
                Some(&snapshot),
                &error,
            ));
        }

        if matches!(action, ClusterAction::Stabilize) || (step + 1) % 8 == 0 {
            if let Err(error) = cluster.wait_until_converged(&model).await {
                let snapshot = cluster.snapshot().await.ok();
                return Err(failure_report(
                    seed,
                    steps,
                    step,
                    Some(&action),
                    &trace,
                    &model,
                    snapshot.as_ref(),
                    &error,
                ));
            }
        }
    }

    let paused = model.paused_handlers.iter().cloned().collect::<Vec<_>>();
    for instance in paused {
        let action = ClusterAction::ResumeTransitions { instance };
        cluster.execute(&action).await.map_err(|error| {
            failure_report(
                seed,
                steps,
                steps,
                Some(&action),
                &trace,
                &model,
                None,
                &error,
            )
        })?;
        model.apply(&action);
    }
    if let Err(error) = cluster.wait_until_converged(&model).await {
        let snapshot = cluster.snapshot().await.ok();
        return Err(failure_report(
            seed,
            steps,
            steps,
            None,
            &trace,
            &model,
            snapshot.as_ref(),
            &error,
        ));
    }
    Ok(())
}

fn environment_usize(name: &str) -> Result<Option<usize>, String> {
    std::env::var(name)
        .map(|value| {
            value
                .parse::<usize>()
                .map(Some)
                .map_err(|error| format!("{name} must be a usize: {error}"))
        })
        .unwrap_or(Ok(None))
}

fn environment_u64(name: &str) -> Result<Option<u64>, String> {
    std::env::var(name)
        .map(|value| {
            value
                .parse::<u64>()
                .map(Some)
                .map_err(|error| format!("{name} must be a u64: {error}"))
        })
        .unwrap_or(Ok(None))
}
