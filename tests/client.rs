//! Client integration tests.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use tempdir::TempDir;
use tokio::fs;

use carol::{errors::Error, Client};

#[tokio::test]
async fn test_client() -> anyhow::Result<()> {
    let tmp = TempDir::new("carol.test")?;
    let db_path = tmp.path().join("carol.sqlite");
    let db_path_str = format!("{}", db_path.display());
    let cache_dir = tmp.path().join("files");
    fs::create_dir(&cache_dir).await?;

    let mut client = Client::init(&db_path_str, &cache_dir).await?;

    let source_url = "https://example.com";
    let target = PathBuf::from("example.html");

    let file = client.get(source_url).await?;
    file.lock(&mut client).await?;

    // Wrap all the work into closure to ensure we unlock file at the end.
    let job = async || -> Result<(), Error> {
        file.symlink(&target).await?;

        Command::new("cat")
            .arg(target.as_os_str())
            .stdout(Stdio::null())
            .status()?;

        fs::remove_file(&target).await?;

        Ok(())
    };

    let result = job().await;
    file.release(&mut client).await?;
    result?;

    Ok(())
}
