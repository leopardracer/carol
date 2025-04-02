use derive_builder::Builder;
use tokio::fs::{self, DirEntry};
use tracing::{debug, error, info, warn};

use crate::database::api;
use crate::errors::{Error, MaintenanceError, NonUtf8PathError};
use crate::{Client, File, FileStatus, GarbageCollector};

#[derive(Default, Builder, Debug)]
#[builder(setter(into))]
pub struct MaintenanceOpts {
    /// Run cache cleaning.
    ///
    /// Find expired cache entries and remove them.
    /// Enables step [`MaintenanceRunner::run_cache_cleaning`].
    run_cache_cleaning: bool,

    /// Find corrupted cache entries.
    ///
    /// Search for cache entries with cache entry path which does not exist.
    /// Enables step [`MaintenanceRunner::find_corrupted_cache_entries`].
    find_corrupted: bool,

    /// Remove corrupted cache entries.
    ///
    /// Search through database and remove files with [`FileStatus::Corrupted`] status.
    /// Enables step [`MaintenanceRunner::remove_corrupted_entries`]
    /// which will be executed after [`MaintenanceRunner::find_corrupted_cache_entries`]
    /// if it is also enabled.
    remove_corrupted: bool,

    /// Remove dangling files from cache directory.
    ///
    /// Search for files which are no longer in the cache database
    /// and remove them from cache directory.
    /// Enables step [`MaintenanceRunner::prune_dangling_files`].
    prune_dangling: bool,
}

/// Maintenance operations for Carol cache.
///
/// This runner is used to ensure Carol state is valid during runtime.
/// It tries to fix problems with cache directory and database if some.
/// If runner is not able to solve the problem, it will produce a warning in logs.
pub struct MaintenanceRunner<'c> {
    client: &'c mut Client,
    opts: MaintenanceOpts,
}

impl<'c> MaintenanceRunner<'c> {
    /// Create new maintenance runner.
    pub fn new(client: &'c mut Client, opts: MaintenanceOpts) -> Self {
        Self { client, opts }
    }

    /// Run all configured maintenance steps once.
    pub async fn run_once(&mut self) -> Result<(), MaintenanceError> {
        if self.opts.find_corrupted {
            self.find_corrupted_cache_entries().await?;
        }
        if self.opts.remove_corrupted {
            self.remove_corrupted_entries().await?;
        }
        if self.opts.run_cache_cleaning {
            self.run_cache_cleaning().await?;
        }
        if self.opts.prune_dangling {
            self.prune_dangling_files().await?;
        }
        Ok(())
    }

    /// Run cache cleaning: remove all expired cache entries.
    pub async fn run_cache_cleaning(&mut self) -> Result<(), MaintenanceError> {
        let mut gc = GarbageCollector::new(self.client);
        gc.run_once().await?;
        Ok(())
    }

    /// Find corrupted cache entries from database.
    ///
    /// Search for cache entries with cache entry path which does not exist
    /// and mark them as [`FileStatus::Corrupted`].
    pub async fn find_corrupted_cache_entries(&mut self) -> Result<(), MaintenanceError> {
        let files = self.client.list().await?;
        for file in files {
            if let Err(err) = self.check_file(&file).await {
                error!("failed to check file (URL '{}'): {}", file.url(), err);
            }
        }
        Ok(())
    }

    /// Search through database and remove files with [`FileStatus::Corrupted`] status.
    ///
    /// If running [`Self::find_corrupted_cache_entries`], this function should be called after.
    pub async fn remove_corrupted_entries(&mut self) -> Result<(), MaintenanceError> {
        info!("removing corrupted entries");
        let corrupted = api::filter_by_status(&mut self.client.db, FileStatus::Corrupted)
            .await
            .map_err(Error::from)?;
        info!("corrupted entries: {}", corrupted.len());
        for entry in corrupted {
            if let Err(err) = self.client.remove(&entry.url).await {
                error!(
                    "failed to remove corrupted file (URL '{}'): {}",
                    &entry.url, err
                );
            }
        }
        Ok(())
    }

    /// Prune dangling files from cache directory.
    ///
    /// Search for files which are no longer in the cache database
    /// and remove them from cache directory.
    pub async fn prune_dangling_files(&mut self) -> Result<(), MaintenanceError> {
        info!("pruning dangling files");
        debug!(
            "reading entries in cache directory: {}",
            self.client.cache_dir.display()
        );
        let mut entries = fs::read_dir(&self.client.cache_dir).await?;

        // TODO: can we check entries asynchronously with a single client?
        while let Some(entry) = entries.next_entry().await? {
            if let Err(err) = self.check_dir_entry(&entry).await {
                error!(
                    "failed to handle entry '{}': {}",
                    entry.path().display(),
                    err
                );
            }
        }

        Ok(())
    }

    /// Check if cached file is corrupted and set its status to [`FileStatus::Corrupted`] if it is.
    ///
    /// This includes:
    /// - checking that `file.cache_path` exists
    async fn check_file(&mut self, file: &File) -> Result<(), MaintenanceError> {
        if !file.cache_path().exists() {
            api::update_status(&mut self.client.db, file.id, FileStatus::Corrupted)
                .await
                .map_err(Error::from)?;
        }
        Ok(())
    }

    /// Check directory entry for presence in cache database and remove if it's not found.
    async fn check_dir_entry(&mut self, entry: &DirEntry) -> Result<(), MaintenanceError> {
        let path = entry.path();
        let metadata = entry.metadata().await?;

        // Ideally nothing apart regular files should be in cache directory,
        // however for now we are not going to clean this up.
        // So just emitting warning and proceeding to next entry.
        if !metadata.is_file() {
            warn!("entry in cache directory is not a file: {}", path.display());
            return Ok(());
        }

        if api::get_by_cache_path(
            &mut self.client.db,
            path.as_os_str().to_str().ok_or(NonUtf8PathError)?,
        )
        .await
        .map_err(Error::from)?
        .is_none()
        {
            info!(
                "file '{}' is not found in database - removing it",
                path.display()
            );
            fs::remove_file(&path).await?;
            info!("'{}' removed", path.display());
        }

        Ok(())
    }
}

// TODO: Asynchronously handle files in maintenance runner.
//       Right now we synchronously iterate over files, which is not efficient.
