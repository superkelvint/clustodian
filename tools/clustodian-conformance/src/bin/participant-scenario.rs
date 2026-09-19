#[path = "../m11.rs"]
mod m11;

use std::env;
use std::path::Path;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(mode) = args.next() else {
        eprintln!("usage: participant-scenario <runtime|prepare|run> <scenario.json> [state.json]");
        return ExitCode::FAILURE;
    };
    let Some(scenario) = args.next() else {
        eprintln!("usage: participant-scenario <runtime|prepare|run> <scenario.json> [state.json]");
        return ExitCode::FAILURE;
    };
    let state = args.next();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let result = runtime.block_on(async {
        match mode.as_str() {
            "runtime" => m11::run_runtime(Path::new(&scenario)).await.map(|_| None),
            "prepare" => {
                let state = state.as_deref().ok_or("prepare requires state.json")?;
                m11::prepare(Path::new(&scenario), Path::new(state))
                    .await
                    .map(|_| None)
            }
            "run" => {
                let state = state.as_deref().ok_or("run requires state.json")?;
                m11::run_driver(Path::new(&scenario), Path::new(state))
                    .await
                    .map(Some)
            }
            _ => Err("mode must be runtime, prepare, or run".into()),
        }
    });
    match result {
        Ok(Some(value)) => {
            println!("{value}");
            ExitCode::SUCCESS
        }
        Ok(None) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("participant scenario: {error}");
            ExitCode::FAILURE
        }
    }
}
