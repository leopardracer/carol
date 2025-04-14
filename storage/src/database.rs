use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::file::{File, FileId, FileMetadata};

pub trait StorageDatabaseError: std::error::Error + Send {
    fn is_unique_violation(&self) -> bool;
    fn is_not_found(&self) -> bool;
}

#[async_trait]
pub trait StorageDatabase: Clone + Sized + Send + Sync {
    /// Type of the URI used to address the database.
    type Uri;

    /// Error type of the database operations.
    type Error: StorageDatabaseError;

    fn uri(&self) -> Self::Uri;

    /// Put new file into the database.
    async fn store(&self, metadata: FileMetadata) -> Result<FileId, Self::Error>;

    /// Get file from the database.
    async fn get(&self, id: FileId) -> Result<File<Self>, Self::Error>;

    /// Remove file from database.
    async fn remove(&self, id: FileId) -> Result<(), Self::Error>;
}

/// Status of stored file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum FileStatus {
    /// File is not yet fully moved into storage.
    #[default]
    Pending,

    /// File is ready to be used.
    Ready,

    /// File is scheduled for removal.
    ToRemove,

    /// File is corrupted. This means that something is wrong with the file
    /// or the cache entry.
    Corrupted,
}

impl fmt::Display for FileStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                Self::Ready => "Ready",
                Self::Pending => "Pending",
                Self::ToRemove => "ToRemove",
                Self::Corrupted => "Corrupted",
            }
        )
    }
}
