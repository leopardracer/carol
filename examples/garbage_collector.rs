use tokio::time::Duration;

use carol::{Client, GarbageCollector};

/// Run garbage collection until interrupted manually.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();

    std::fs::create_dir_all(".cache")?;

    let mut client = Client::init("carol.sqlite", ".cache").await?;
    let mut gc = GarbageCollector::new(&mut client);

    // this is going to run forever
    gc.run_every(Duration::from_secs(5)).await;

    Ok(())
}
