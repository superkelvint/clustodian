use anyhow::{Context, Result};
use clustodian::coordination::etcd::{EtcdCoordination, EtcdCoordinationConfig};
use clustodian::model::{leader_standby, InstanceId};
use clustodian::participant::{
    ParticipantRuntime, TransitionExecution, TransitionHandler, TransitionHandlerError,
};
use std::{env, path::PathBuf, thread, time::Duration};

const PARTICIPANT_LEASE_TTL: Duration = Duration::from_secs(2);
const BARRIER_POLL: Duration = Duration::from_millis(20);

struct MockApplicationHandler {
    instance_id: String,
    barrier_dir: PathBuf,
}

impl TransitionHandler for MockApplicationHandler {
    fn handle(
        &self,
        execution: &TransitionExecution,
    ) -> std::result::Result<(), TransitionHandlerError> {
        let arm = self.barrier_dir.join("block-next");

        // Claim the one-shot barrier atomically enough for this verifier: only the
        // callback that successfully removes `block-next` becomes the blocked one.
        if std::fs::remove_file(&arm).is_ok() {
            let blocked = self.barrier_dir.join("blocked");
            std::fs::write(
                &blocked,
                format!(
                    "{} {} {} {} -> {} {}\n",
                    execution.resource(),
                    execution.partition(),
                    self.instance_id,
                    execution.source_state(),
                    execution.target_state(),
                    execution.transition_id(),
                ),
            )
            .map_err(|error| TransitionHandlerError::new(error.to_string()))?;

            let release = self.barrier_dir.join("release");
            while !release.exists() {
                thread::sleep(BARRIER_POLL);
            }
            let _ = std::fs::remove_file(&release);
            let _ = std::fs::remove_file(&blocked);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = req("CLUSTODIAN_M12_ETCD_ENDPOINT")?;
    let prefix = req("CLUSTODIAN_M12_ETCD_PREFIX")?;
    let cluster = req("CLUSTODIAN_M12_CLUSTER")?;
    let instance_id = req("CLUSTODIAN_M12_INSTANCE_ID")?;
    let barrier_dir = PathBuf::from(req("CLUSTODIAN_M12_BARRIER_DIR")?);
    tokio::fs::create_dir_all(&barrier_dir).await?;

    let coordination = EtcdCoordination::connect(EtcdCoordinationConfig {
        endpoint,
        prefix,
        cluster,
    })
    .await?;

    let instance = InstanceId::new(instance_id.clone())?;
    let runtime = ParticipantRuntime::new(
        coordination,
        instance,
        leader_standby(),
        MockApplicationHandler {
            instance_id,
            barrier_dir,
        },
    )
    .with_lease_ttl(PARTICIPANT_LEASE_TTL)?;

    runtime
        .run(|| Ok(()))
        .await
        .context("participant runtime exited")
}

fn req(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("missing {name}"))
}
