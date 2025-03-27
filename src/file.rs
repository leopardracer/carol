use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use tokio::fs;

use crate::errors::Error;
use crate::models::CacheEntry;
use crate::{Client, FileStatus};

/// Cached file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct File {
    /// Primary key of the file in database.
    pub(crate) id: i32,

    /// Source URL.
    pub url: String,

    /// Path to the file in filesystem (in cache directory).
    pub cache_path: PathBuf,

    /// Creation timestamp.
    ///
    /// This is actually the timestamp of cache entry creation.
    /// The content of the file may not be fetched yet at that moment.
    pub created: DateTime<Utc>,

    /// Expiration timestamp.
    ///
    /// After this timestamp the file status will become `ToRemove`.
    /// Then after its reference counter drops to 0, it will be removed from cache.
    pub expires: Option<DateTime<Utc>>,

    /// Status of the file.
    pub status: FileStatus,
}

impl From<CacheEntry> for File {
    fn from(value: CacheEntry) -> Self {
        Self::from_entry(value)
    }
}

impl File {
    /// Convert from cache entry.
    fn from_entry(entry: CacheEntry) -> Self {
        Self {
            id: entry.id,
            url: entry.url,
            cache_path: PathBuf::from(entry.cache_path),
            created: entry.created,
            expires: entry.expires,
            status: entry.status,
        }
    }

    /// Create a symlink for this file at `path`.
    pub async fn symlink(&self, path: impl AsRef<Path>) -> Result<(), Error> {
        fs::symlink(&self.cache_path, path)
            .await
            .map_err(Into::into)
    }

    // Unfortunately lock()/release() thing cannot be wrapped into new()/drop(),
    // because that would require `File` to hold unique reference to `Client`
    // which is really inconvenient.

    /// Increment files reference counter.
    /// When reference counter is > 0, the file will not be removed.
    pub async fn lock(&self, client: &mut Client) -> Result<(), Error> {
        client.db.increment_ref(self.id).await?;
        Ok(())
    }

    /// Decrement files reference counter.
    pub async fn release(&self, client: &mut Client) -> Result<(), Error> {
        client.db.decrement_ref(self.id).await?;
        Ok(())
    }
}
