use async_trait::async_trait;

use crate::file::{File, FileId, FileMetadata, FileSource, FileStatus};

pub trait StorageDatabaseError: std::error::Error + Send + Sync {
    fn is_unique_violation(&self) -> bool;
    fn is_not_found(&self) -> bool;
}

#[async_trait]
pub trait StorageDatabase: Clone + Sized + Send + Sync {
    /// Type of the URI used to address the database.
    type Uri: ToString;

    /// Error type of the database operations.
    type Error: StorageDatabaseError;

    fn uri(&self) -> Self::Uri;

    /// Put new file into the database.
    async fn store(&self, metadata: FileMetadata) -> Result<FileId, Self::Error>;

    /// Get file from the database.
    async fn get(&self, id: FileId) -> Result<File, Self::Error>;

    /// Remove file from database.
    async fn remove(&self, id: FileId) -> Result<(), Self::Error>;
}

#[async_trait]
pub trait StorageDatabaseExt: StorageDatabase {
    async fn select_by_source(&self, source: &FileSource) -> Result<Vec<File>, Self::Error>;
    async fn update_status(&self, id: FileId, new_status: FileStatus) -> Result<File, Self::Error>;
}
