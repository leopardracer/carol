use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use tokio::fs;
use tracing::{trace, warn};

use crate::database;
use crate::database::api;
use crate::database::models::CacheEntry;
use crate::errors::Error;
use crate::FileStatus;

/// Cached file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct File {
    /// URL of the cache database where this file resides.
    database_url: String,

    /// Whether the file was released or not.
    ///
    /// Needed for automatic release on drop only
    /// to avoid database connection for already released files.
    pub(crate) released: bool,

    /// Primary key of the file in database.
    pub(crate) id: i32,

    /// Source URL.
    ///
    /// This must not change in the database over time, that's why
    /// it is fine to store it here.
    url: String,

    /// Path to the file in filesystem (in cache directory).
    ///
    /// This must not change in the database over time, that's why
    /// it is fine to store it here.
    cache_path: PathBuf,

    /// Creation timestamp.
    ///
    /// This is actually the timestamp of cache entry creation.
    /// The content of the file may not be fetched yet at that moment.
    ///
    /// This must not change in the database over time, that's why
    /// it is fine to store it here.
    created: DateTime<Utc>,
}

impl File {
    /// Convert from cache entry.
    pub(crate) fn from_entry(entry: CacheEntry, database_url: String) -> Self {
        Self {
            database_url,
            released: true,
            id: entry.id,
            url: entry.url,
            cache_path: PathBuf::from(entry.cache_path),
            created: entry.created,
        }
    }

    /// Source URL.
    pub fn url(&self) -> &str {
        self.url.as_str()
    }

    /// Path to the file in filesystem (in cache directory).
    pub fn cache_path(&self) -> &Path {
        self.cache_path.as_path()
    }

    /// Creation timestamp.
    ///
    /// This is the timestamp of cache entry creation.
    /// The content of the file may not be fully downloaded at that moment.
    pub fn created(&self) -> &DateTime<Utc> {
        &self.created
    }

    /// Current expiration timestamp.
    ///
    /// Value is queried from cache database and represents expiration timestamp
    /// at the moment. Keep in mind that this timestamp may be updated.
    ///
    /// After this timestamp the file status should be set to [`FileStatus::ToRemove`].
    /// Then after its reference counter drops to 0, it should be removed from cache.
    pub async fn expires(&self) -> Result<Option<DateTime<Utc>>, Error> {
        let mut connection = database::establish_connection(&self.database_url).await?;
        let entry = api::get_entry(&mut connection, self.id).await?;
        Ok(entry.expires)
    }

    /// Current status of the file.
    ///
    /// Value is queried from cache database and represents file status
    /// at the moment. Keep in mind that this status may be updated.
    pub async fn status(&self) -> Result<FileStatus, Error> {
        let mut connection = database::establish_connection(&self.database_url).await?;
        let entry = api::get_entry(&mut connection, self.id).await?;
        Ok(entry.status)
    }

    /// Create a symlink pointing to this file at `path`.
    pub async fn symlink(&self, path: impl AsRef<Path>) -> Result<(), Error> {
        fs::symlink(&self.cache_path, path)
            .await
            .map_err(Into::into)
    }

    /// Decrement files reference counter.
    ///
    /// The file will be released automatically on drop, however you won't be able to
    /// handle the errors which may occur. Use this function to catch releasing errors.
    pub async fn release(mut self) -> Result<(), Error> {
        trace!("releasing file {:?}", self);
        let mut connection = database::establish_connection(&self.database_url).await?;
        api::decrement_ref(&mut connection, self.id).await?;
        self.released = true;
        Ok(())
    }
}

impl Drop for File {
    fn drop(&mut self) {
        if !self.released {
            let database_url = std::mem::take(&mut self.database_url);
            let id = self.id;
            let release = async |database_url: String, id: i32| {
                let conn_or_error = database::establish_connection(&database_url).await;
                match conn_or_error {
                    Ok(mut connection) => {
                        if let Err(err) = api::decrement_ref(&mut connection, id).await {
                            warn!("failed to release a file: {}", err);
                        }
                    }
                    Err(err) => {
                        warn!("failed to release a file: {}", err);
                    }
                }
            };
            tokio::spawn(release(database_url, id));
        }
    }
}
