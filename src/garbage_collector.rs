use chrono::Utc;
use tracing::{error, info, warn};

use crate::database::api;
use crate::errors::Error;
use crate::{Client, Duration, FileStatus};

/// Garbage collector.
///
/// Removed expired files from cache.
pub struct GarbageCollector<'c> {
    client: &'c mut Client,
}

impl<'c> GarbageCollector<'c> {
    /// Create new garbage collector instance.
    pub fn new(client: &'c mut Client) -> Self {
        Self { client }
    }

    /// Run garbage collection once.
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
                    info!("successfully garbage-collected URL '{}'", &entry.url);
                }
                Err(err) => {
                    // This may happened if the file was used after being scheduled for removal
                    // Its expiration timestamp may also have been updated
                    info!("failed to garbage-collect URL '{}': {}", entry.url, err);
                }
            }
        }
        Ok(())
    }

    /// Run garbage collection every `duration` seconds.
    ///
    /// This function never returns.
    pub async fn run_every(&mut self, duration: Duration) {
        loop {
            tokio::time::sleep(duration).await;
            match self.run_once().await {
                Ok(_) => {
                    info!("garbage collection succeeded");
                }
                Err(err) => {
                    error!("garbage collection failed: {}", err);
                }
            }
        }
    }
}

// TODO: Garbage collector may run a pool of clients to perform removals asynchronously.
//       For now files are removed sequentially, because &mut Client cannot be shared.
