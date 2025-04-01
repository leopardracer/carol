use std::time::Duration;

use tokio::io::{self, AsyncBufReadExt, BufReader};

use carol::{Client, GarbageCollector};

// Accepts URLs from stdin and get them
async fn accept_get_requests(cache_dir: String, db_path: String) -> anyhow::Result<()> {
    let mut client = Client::builder(&cache_dir, &db_path)
        .default_file_duration(Duration::from_secs(20))
        .build()
        .await?;

    let stdin = io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Some(url) = lines.next_line().await? {
        match client.get(url.trim()).await {
            Ok(file) => println!("cached file {:?}", file),
            Err(err) => eprintln!("error: {}", err),
        }
    }

    Ok(())
}

// Run garbage collection
async fn run_garbage_collector(cache_dir: String, db_path: String) -> anyhow::Result<()> {
    // wait a bit to avoid client initialization conflicts
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let mut client = Client::init(&cache_dir, &db_path).await?;
    let mut gc = GarbageCollector::new(&mut client);
    // this is going to run forever
    println!("starting garbage collector");
    gc.run_every(Duration::from_secs(5)).await;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .init();

    let (cache_dir, db_path) = ("carol.sqlite".to_string(), ".cache".to_string());

    let _ = tokio::join!(
        accept_get_requests(cache_dir.clone(), db_path.clone()),
        run_garbage_collector(cache_dir, db_path)
    );

    Ok(())
}
