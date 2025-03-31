//! Client integration tests.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use tempdir::TempDir;
use tokio::fs;

use carol::pool::{Pool, PoolManager};
use carol::{Client, File};

#[tokio::test]
async fn test_with_one_file() -> anyhow::Result<()> {
    let tmp = TempDir::new("carol.test")?;
    let db_path = tmp.path().join("carol.sqlite");
    let db_path_str = format!("{}", db_path.display());
    let cache_dir = tmp.path().join("files");
    fs::create_dir(&cache_dir).await?;

    let mut client = Client::init(&db_path_str, &cache_dir).await?;

    let source_url = "https://example.com";

    // This variable will keep a reference to source URL, preventing it from removal.
    let file = client.get(source_url).await?;

    // We will use this function to own the file and drop it at the end.
    // After calling this function, file will be released and can be removed by client.
    // Instead of that you can call file.release() or drop(file) manually.
    async fn use_file(file: File) -> anyhow::Result<()> {
        let target = PathBuf::from("example.html");

        file.symlink(&target).await?;

        Command::new("cat")
            .arg(target.as_os_str())
            .stdout(Stdio::null())
            .status()?;

        fs::remove_file(&target).await?;
        Ok(())
    }

    use_file(file).await?;

    // Now because file using this URL was dropped, URL can be removed from cache.
    // If the file wasn't drop, this function will be stuck waiting for file to be release.
    client.remove(source_url).await?;

    Ok(())
}

#[tokio::test]
async fn test_with_many_files() -> anyhow::Result<()> {
    // This test shows how to use connection pool to fetch multiple files at the same time

    let tmp = TempDir::new("carol.test")?;
    let db_path = tmp.path().join("carol.sqlite");
    let db_path_str = format!("{}", db_path.display());
    let cache_dir = tmp.path().join("files");
    fs::create_dir(&cache_dir).await?;

    let source_urls = &["https://example.com", "http://example.com"];

    // Create clients pool
    let clients_pool = Pool::builder(PoolManager::new(&db_path_str, cache_dir)).build()?;

    // Define tasks to fetch all files from `source_urls`
    async fn get_file(clients_pool: Pool, url: &str) -> anyhow::Result<File> {
        let mut client = clients_pool.get().await?;
        Ok(client.get(url).await?)
    }
    let mut tasks = tokio::task::JoinSet::new();
    for url in source_urls {
        let task = get_file(clients_pool.clone(), url);
        tasks.spawn(task);
    }
    let files = tasks
        .join_all()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    // Use files as in previous test
    async fn use_files(files: &[File]) -> anyhow::Result<()> {
        for (id, file) in files.iter().enumerate() {
            let target = PathBuf::from(format!("example.{}.html", id));

            file.symlink(&target).await?;

            Command::new("cat")
                .arg(target.as_os_str())
                .stdout(Stdio::null())
                .status()?;

            fs::remove_file(&target).await?;
        }
        Ok(())
    }

    use_files(&files).await?;

    // // Now because file using this URL was dropped, URL can be removed from cache.
    // // If the file wasn't drop, this function will be stuck waiting for file to be release.
    // client.remove(source_url).await?;

    Ok(())
}
