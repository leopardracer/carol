//! Client integration tests.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use tempdir::TempDir;
use tokio::fs;

use carol::{Client, File};

#[tokio::test]
async fn test_client() -> anyhow::Result<()> {
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
