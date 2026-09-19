use clustodian::{Cluster, ClusterConfig, ClusterSpec, InstanceSpec, Placement, ResourceSpec};
use clustodian_job_workers::{
    configured_workers, partition_names, validate_partition, ProcessResult, WorkerHandler,
    WorkerState,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const RESOURCE: &str = "job-queues";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("setup") => setup().await?,
        Some("controller") => run_controller().await?,
        Some("worker") => {
            let instance = argument(&mut args, "--instance")?;
            let port = argument(&mut args, "--port")?.parse::<u16>()?;
            run_worker(instance, port).await?;
        }
        Some("observe") => observe().await?,
        Some("process") => {
            let port = argument(&mut args, "--port")?.parse::<u16>()?;
            let partition = argument(&mut args, "--partition")?;
            let job = argument(&mut args, "--job")?;
            println!(
                "{}",
                request(port, &format!("PROCESS {partition} {job}")).await?
            );
        }
        Some("status") => {
            let port = argument(&mut args, "--port")?.parse::<u16>()?;
            println!("{}", request(port, "STATUS").await?);
        }
        _ => usage(),
    }
    Ok(())
}

fn usage() {
    eprintln!(
        "usage: job-workers <setup|controller|worker|observe|process|status>\n\
         worker: --instance NAME --port PORT\n\
         process: --port PORT --partition job-queues_N --job NAME"
    );
}

fn argument(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, Box<dyn Error>> {
    match (args.next(), args.next()) {
        (Some(flag), Some(value)) if flag == name => Ok(value),
        _ => Err(format!("expected {name} VALUE").into()),
    }
}

fn endpoint() -> String {
    env::var("JOB_WORKERS_ETCD_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:2379".to_owned())
}

fn prefix() -> String {
    env::var("JOB_WORKERS_ETCD_PREFIX").unwrap_or_else(|_| "clustodian-job-workers".to_owned())
}

fn cluster() -> String {
    env::var("JOB_WORKERS_CLUSTER").unwrap_or_else(|_| "job-workers".to_owned())
}

async fn backend() -> Result<Cluster, Box<dyn Error>> {
    Ok(Cluster::connect(
        ClusterConfig::new(cluster())
            .etcd_endpoints([endpoint()])
            .namespace(prefix()),
    )
    .await?)
}

async fn setup() -> Result<(), Box<dyn Error>> {
    let workers = configured_workers();
    clustodian_job_workers::valid_worker_names(&workers)?;
    let cluster = backend().await?;

    let mut preference_lists = BTreeMap::new();
    for (index, partition) in partition_names().enumerate() {
        preference_lists.insert(
            partition,
            vec![workers[index % 3].clone(), workers[(index + 1) % 3].clone()],
        );
    }
    cluster
        .admin()
        .apply(
            ClusterSpec::new()
                .instances(
                    workers
                        .iter()
                        .map(|worker| InstanceSpec::new(worker).zone("default")),
                )
                .resource(
                    ResourceSpec::leader_standby(RESOURCE)
                        .partitions(3)
                        .replicas(2)
                        .placement(Placement::semi_auto(preference_lists)),
                ),
        )
        .await?;
    println!(
        "configured resource={RESOURCE} partitions=3 replicas=2 workers={}",
        workers.join(",")
    );
    Ok(())
}

async fn run_controller() -> Result<(), Box<dyn Error>> {
    backend()
        .await?
        .controller(
            env::var("JOB_WORKERS_CONTROLLER_ID").unwrap_or_else(|_| "controller-1".to_owned()),
        )
        .lease_ttl(Duration::from_millis(
            env::var("JOB_WORKERS_CONTROLLER_LEASE_TTL_MS")
                .unwrap_or_else(|_| "1500".to_owned())
                .parse()?,
        ))
        .run_until_signal()
        .await?;
    Ok(())
}

async fn run_worker(instance_name: String, port: u16) -> Result<(), Box<dyn Error>> {
    let cluster = backend().await?;
    let state = Arc::new(Mutex::new(WorkerState::default()));
    let handler = WorkerHandler::new(Arc::clone(&state));
    let lease_ttl_secs = env::var("JOB_WORKERS_PARTICIPANT_LEASE_TTL_SECS")
        .unwrap_or_else(|_| "2".to_owned())
        .parse()?;
    let ready_instance = instance_name.clone();
    let runtime = cluster
        .participant(instance_name.clone())
        .resource(RESOURCE, handler)
        .lease_ttl(Duration::from_secs(lease_ttl_secs))
        .on_ready(move || {
            println!("worker {ready_instance} registered");
            std::io::stdout().flush().map_err(|error| {
                std::io::Error::new(error.kind(), format!("flush ready message: {error}"))
            })
        });
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let server_state = Arc::clone(&state);
    let server_instance = instance_name.clone();
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await?;
            let state = Arc::clone(&server_state);
            let instance = server_instance.clone();
            tokio::spawn(async move {
                if let Err(error) = serve(stream, instance, state).await {
                    eprintln!("worker client error: {error}");
                }
            });
        }
        #[allow(unreachable_code)]
        Ok::<(), std::io::Error>(())
    });
    let result = runtime.run_until_signal().await;
    server.abort();
    result?;
    Ok(())
}

async fn serve(
    stream: TcpStream,
    instance: String,
    state: Arc<Mutex<WorkerState>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    while let Some(line) = lines.next_line().await? {
        let mut fields = line.splitn(3, ' ');
        let response = match fields.next() {
            Some("STATUS") => {
                let snapshot = state
                    .lock()
                    .map_err(|_| "worker state lock poisoned")?
                    .snapshot(&instance);
                serde_json::to_string(&snapshot)?
            }
            Some("PROCESS") => {
                let partition = fields.next().ok_or("PROCESS requires partition and job")?;
                let job = fields.next().ok_or("PROCESS requires partition and job")?;
                if !validate_partition(partition) {
                    return Err(format!("unknown partition {partition}").into());
                }
                let result = state
                    .lock()
                    .map_err(|_| "worker state lock poisoned")?
                    .process(partition, job);
                match result {
                    ProcessResult::Processed { job, count } => json!({
                        "status": "processed",
                        "instance": instance,
                        "partition": partition,
                        "job": job,
                        "count": count
                    })
                    .to_string(),
                    ProcessResult::NotOwner { role } => json!({
                        "status": "not_owner",
                        "instance": instance,
                        "partition": partition,
                        "role": role
                    })
                    .to_string(),
                }
            }
            Some("QUIT") => break,
            Some(command) => json!({
                "status": "error",
                "message": format!("unknown command {command}")
            })
            .to_string(),
            None => continue,
        };
        write.write_all(response.as_bytes()).await?;
        write.write_all(b"\n").await?;
    }
    Ok(())
}

async fn request(port: u16, command: &str) -> Result<String, Box<dyn Error>> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    stream.write_all(command.as_bytes()).await?;
    stream.write_all(b"\n").await?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).await?;
    Ok(response.trim_end().to_owned())
}

async fn observe() -> Result<(), Box<dyn Error>> {
    let snapshot = backend().await?.observer().snapshot().await?;
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}
