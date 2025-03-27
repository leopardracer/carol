use anyhow::Context;
use tokio::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler};

use carol::{Client, GarbageCollector};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();

    let sched = JobScheduler::new()
        .await
        .context("failed to create new job scheduler")?;

    std::fs::create_dir(".cache")?;

    let job = Job::new_async("1/7 * * * * *", |uuid, mut l| {
        Box::pin(async move {
            let mut client = Client::init("carol.sqlite", ".cache").await.unwrap();
            let mut gc = GarbageCollector::new(&mut client);
            gc.run_once().await.unwrap();

            l.next_tick_for_job(uuid).await.unwrap();
        })
    })?;

    sched.add(job).await.context("failed to add job")?;
    sched.start().await.context("failed to start job")?;
    tokio::time::sleep(Duration::from_secs(23)).await;

    Ok(())
}
