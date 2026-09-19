use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::runtime::{ControllerRuntime, ControllerRuntimeConfig};
use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let endpoint = match env::var("CLUSTODIAN_M10_ETCD_ENDPOINT") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("CLUSTODIAN_M10_ETCD_ENDPOINT is required");
            return ExitCode::from(2);
        }
    };
    let prefix = match env::var("CLUSTODIAN_M10_ETCD_PREFIX") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("CLUSTODIAN_M10_ETCD_PREFIX is required");
            return ExitCode::from(2);
        }
    };
    let ready_file = match env::var("CLUSTODIAN_M10_READY_FILE") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("CLUSTODIAN_M10_READY_FILE is required");
            return ExitCode::from(2);
        }
    };
    let controller_id =
        env::var("CLUSTODIAN_M10_CONTROLLER_ID").unwrap_or_else(|_| String::from("m10-controller"));

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to create runtime: {error}");
            return ExitCode::from(1);
        }
    };
    let result: Result<(), Box<dyn std::error::Error>> = runtime.block_on(async move {
        let backend = EtcdCoordination::connect(EtcdCoordinationConfig {
            endpoint,
            prefix,
            cluster: String::from("m10"),
        })
        .await
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?;
        ControllerRuntime::new(
            backend,
            ControllerRuntimeConfig {
                cluster: String::from("m10"),
                controller_id,
                lease_ttl_ms: 1_500,
            },
        )
        .await
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?
        .on_ready(move || std::fs::write(ready_file, b"ready\n"))
        .run()
        .await
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("controller runtime failed: {error}");
            ExitCode::from(1)
        }
    }
}
