use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use directories::ProjectDirs;
use tokio::fs;

use carol::errors::NonUtf8PathError;
use carol::Client;

/// Default cache path.
fn default_cache_path() -> PathBuf {
    if let Some(proj_dirs) = ProjectDirs::from("", "gevulot", "carol") {
        proj_dirs.cache_dir().to_path_buf()
    } else {
        PathBuf::from(".carol")
    }
}

/// Calcutale default cache directory path based on OS.
fn default_cache_directory() -> PathBuf {
    default_cache_path().join("files")
}

/// Calcutale default database path based on OS.
fn default_database_path() -> PathBuf {
    default_cache_path().join("carol.sqlite")
}

/// CLI interface of Carol.
#[derive(Parser, Clone, Debug)]
#[command(version, about = "Carol CLI")]
pub struct Cli {
    /// Path to cache directory.
    ///
    /// This is the path in filesystem, where the actual downloaded files are stored.
    #[arg(
        short = 'c',
        long,
        value_name = "PATH",
        default_value = default_cache_directory().into_os_string(),
    )]
    cache_dir: PathBuf,

    /// Path to Carol database file.
    ///
    /// It is not allowed to put database file into cache directory.
    #[arg(
        short = 'd',
        long,
        value_name = "PATH",
        default_value = default_database_path().into_os_string(),
    )]
    database: PathBuf,

    /// Command to execute.
    #[command(subcommand)]
    command: Command,
}

impl Cli {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let database = self
            .database
            .as_os_str()
            .to_str()
            .ok_or(NonUtf8PathError)
            .context("failed to create database file")?;

        // Ensure database file has its parent directory
        if let Some(parent_dir) = self.database.parent() {
            fs::create_dir_all(parent_dir)
                .await
                .context("failed to create database file")?;
        }

        // Ensure cache directory exists
        if !self.cache_dir.exists() {
            fs::create_dir_all(&self.cache_dir)
                .await
                .context("failed to create cache directory")?;
        }

        let mut client = Client::init(database, &self.cache_dir)
            .await
            .context("failed to initialize Carol client")?;

        self.command.execute(&mut client).await
    }
}

/// Carol commands.
#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
    /// Download a single file.
    Get {
        /// URL to download.
        url: String,

        /// Path to symlink pointing to the result.
        ///
        /// If none provided, path to cached file will be printed.
        target: Option<PathBuf>,
    },

    /// Manage cache files.
    #[command(subcommand)]
    Cache(CacheCommand),

    /// Run maintenance process.
    ///
    /// This process will run forever, periodically performing garbage collection.
    Maintain,
}

impl Command {
    pub async fn execute(&self, client: &mut Client) -> anyhow::Result<()> {
        match self {
            Self::Get { url, target } => {
                let file = client.get(url).await?;
                let output_path = if let Some(target) = target {
                    file.symlink(target).await?;
                    target.clone()
                } else {
                    file.cache_path().to_path_buf()
                };
                println!("{}", output_path.display());
            }
            Self::Cache(cmd) => {
                cmd.execute(client).await?;
            }
            Self::Maintain => todo!(),
        }
        Ok(())
    }
}

/// Cache manipulations subcommand.
#[derive(Clone, Debug, clap::Subcommand)]
pub enum CacheCommand {
    /// Find cached URL.
    Find {
        /// URL to find.
        url: String,
    },

    /// List all cache entries.
    List,

    /// Remove URL from cache.
    ///
    /// Warning: file will be removed regardless reference counter value.
    Remove {
        /// URL to remove.
        url: String,

        /// Force remove regardless reference counter.
        force: bool,
    },
}

impl CacheCommand {
    pub async fn execute(&self, client: &mut Client) -> anyhow::Result<()> {
        match self {
            Self::Find { url } => {
                if let Some(file) = client.find(url).await? {
                    println!("{:#?}", file);
                }
            }
            Self::List => {
                let files = client.list().await?;
                for file in files {
                    println!("{:#?}", file);
                }
            }
            Self::Remove { url, .. } => {
                client.remove(url).await?;
            }
        }
        Ok(())
    }
}
