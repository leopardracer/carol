use std::error::Error as StdError;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use futures_util::{Stream, StreamExt};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time;

use crate::database::{StorageDatabase, StorageDatabaseError, StorageDatabaseExt};
use crate::error::StorageError;
use crate::file::{File, FileMetadata, FileSource, FileStatus, StorePolicy};
use crate::sqlite::{self, run_migrations, SqliteStorageDatabase};

#[derive(Clone)]
pub struct StorageManager<D: StorageDatabase = SqliteStorageDatabase> {
    db: D,
    dir: PathBuf,
}

impl<D: StorageDatabaseExt> StorageManager<D> {
    pub async fn add_file_from_stream<S, E>(
        &self,
        source: FileSource,
        store_policy: StorePolicy,
        filename: Option<String>,
        mut stream: S,
    ) -> Result<File, StorageError<D::Error>>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: StdError + 'static + Send + Sync,
    {
        let path = self.path_from_source(&source);
        let now = Utc::now();
        let metadata = FileMetadata {
            source: source.clone(),
            filename,
            path: path.clone(),
            store_policy,
            created: now,
            last_used: now,
        };

        match self.db.store(metadata).await {
            Ok(id) => {
                let mut run = async || -> Result<File, StorageError<D::Error>> {
                    let mut output = fs::File::create_new(&path).await?;
                    while let Some(chunk_result) = stream.next().await {
                        let chunk = chunk_result.map_err(StorageError::custom)?;
                        output.write_all(&chunk).await?;
                    }
                    let file = self.db.update_status(id, FileStatus::Ready).await?;
                    Ok(file)
                };

                let revert = async || -> Result<(), StorageError<D::Error>> {
                    fs::remove_file(&path).await?;
                    self.db.remove(id).await?;
                    Ok(())
                };

                match run().await {
                    Ok(file) => Ok(file),
                    Err(err) => {
                        revert().await?;
                        Err(err)
                    }
                }
            }
            Err(err) if err.is_unique_violation() => {
                let file = loop {
                    match self.find_by_source(&source).await? {
                        Some(file) if file.status == FileStatus::Ready => {
                            break file;
                        }
                        Some(file) if file.status == FileStatus::Pending => {
                            time::sleep(Duration::from_secs(1)).await;
                        }
                        _ => {
                            panic!("await error");
                        }
                    }
                };
                Ok(file)
            }
            Err(err) => panic!("{:?}", err),
        }
    }

    pub async fn find_by_source(
        &self,
        source: &FileSource,
    ) -> Result<Option<File>, StorageError<D::Error>> {
        let files = self.db.select_by_source(source).await?;
        // Because of the way self.path_from_source() works, sources
        // are also expected to be unique.
        debug_assert!(files.len() <= 1);
        Ok(files.into_iter().next())
    }

    /// Generate cached file path from [`FileSource`].
    ///
    /// Applies SHA256 to `source` string representation
    /// to generate the file name in cache directory.
    pub fn path_from_source(&self, source: &FileSource) -> PathBuf {
        self.dir.join(sha256::digest(source))
    }
}

impl StorageManager {
    /// Initialize new storage manager with SQLite database.
    ///
    /// If the storage doesn't exist yet, it will be created.
    pub async fn init(
        database_url: impl AsRef<str>,
        dir: impl AsRef<Path>,
        max_size: Option<usize>,
    ) -> Result<Self, StorageError<sqlite::error::DatabaseError>> {
        let dir = std::path::absolute(dir.as_ref())?;
        fs::create_dir_all(&dir).await?;
        run_migrations(database_url.as_ref()).await?;
        let db = SqliteStorageDatabase::connect_pool(database_url.as_ref(), max_size).await?;
        Ok(Self { db, dir })
    }
}
