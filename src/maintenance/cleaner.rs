use std::time::Duration;

use chrono::Utc;
use tracing::{error, info, warn};

use crate::database::api;
use crate::errors::Error;
use crate::{Client, FileStatus};

/// Cache cleaner.
///
/// Removed expired files from cache.
pub struct CacheCleaner<'c> {
    client: &'c mut Client,
}

impl<'c> CacheCleaner<'c> {
    /// Create new cache cleaner instance.
    pub fn new(client: &'c mut Client) -> Self {
        Self { client }
    }

    /// Run cache cleaning once.
    pub async fn run_once(&mut self) -> Result<(), Error> {
        self.schedule_for_removal().await?;
        self.remove().await?;
        Ok(())
    }

    /// Set expired files status to [`FileStatus::ToRemove`].
    pub async fn schedule_for_removal(&mut self) -> Result<(), Error> {
        info!("scheduling expired files for removal");
        let now = Utc::now();
        let files = self.client.list().await?; // FIXME: any iterators in diesel?

        let mut to_remove = vec![];
        for file in files.into_iter() {
            if let Some(ts) = file.expires().await? {
                if ts < now {
                    to_remove.push(file);
                }
            }
        }
        info!("expired files: {}", to_remove.len());

        for file in to_remove {
            match self.client.schedule_for_removal(file.url()).await {
                Ok(_) => {
                    info!("URL '{}' scheduled for removal", file.url());
                }
                Err(err) => {
                    warn!(
                        "failed to schedule URL '{}' for removal: {}",
                        file.url(),
                        err
                    );
                }
            }
        }
        Ok(())
    }

    /// Remove files with status `ToRemove`.
    pub async fn remove(&mut self) -> Result<(), Error> {
        let to_remove = api::filter_by_status(&mut self.client.db, FileStatus::ToRemove).await?;
        for entry in to_remove {
            match self.client.remove(&entry.url).await {
                Ok(_) => {
                    info!("successfully removed URL '{}'", &entry.url);
                }
                Err(err) => {
                    // This may happened if the file was used after being scheduled for removal
                    // Its expiration timestamp may also have been updated
                    info!("failed to remove URL '{}': {}", entry.url, err);
                }
            }
        }
        Ok(())
    }

    /// Run cache cleaning every `duration` seconds.
    ///
    /// This function never returns.
    pub async fn run_every(&mut self, duration: Duration) {
        loop {
            tokio::time::sleep(duration).await;
            match self.run_once().await {
                Ok(_) => {
                    info!("cache cleaning succeeded");
                }
                Err(err) => {
                    error!("cache cleaning failed: {}", err);
                }
            }
        }
    }
}

// TODO: Garbage collector may run a pool of clients to perform removals asynchronously.
//       For now files are removed sequentially, because &mut Client cannot be shared.
