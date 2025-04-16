use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use futures_util::{Stream, StreamExt};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time;

use crate::database::{StorageDatabase, StorageDatabaseError, StorageDatabaseExt};

use super::file::{File, FileMetadata, FileSource, FileStatus, StorePolicy};

#[derive(Clone)]
pub struct StorageManager<D: StorageDatabase> {
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
    ) -> Result<File, ()>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::error::Error,
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
                let mut run = async || -> Result<File, ()> {
                    let mut output = fs::File::create_new(&path).await.unwrap();
                    while let Some(chunk_result) = stream.next().await {
                        let chunk = chunk_result.unwrap();
                        output.write_all(&chunk).await.unwrap();
                    }
                    let file = self.db.update_status(id, FileStatus::Ready).await.unwrap();
                    Ok(file)
                };

                let revert = async || {
                    fs::remove_file(&path).await.unwrap();
                    self.db.remove(id).await.unwrap();
                };

                match run().await {
                    Ok(file) => Ok(file),
                    Err(err) => {
                        revert().await;
                        Err(err)
                    }
                }
            }
            Err(err) if err.is_unique_violation() => {
                let file = loop {
                    match self.find_by_source(&source).await {
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

    pub async fn find_by_source(&self, source: &FileSource) -> Option<File> {
        let files = self.db.select_by_source(source).await.unwrap();
        // Because of the way self.path_from_source() works, sources
        // are also expected to be unique.
        debug_assert!(files.len() <= 1);
        files.into_iter().next()
    }

    /// Generate cached file path from [`FileSource`].
    ///
    /// Applies SHA256 to `source` string representation
    /// to generate the file name in cache directory.
    pub fn path_from_source(&self, source: &FileSource) -> PathBuf {
        self.dir.join(sha256::digest(source))
    }
}

// TODO: error handling - remove unwrap()

#[cfg(feature = "sqlite")]
use crate::sqlite::{error::DatabaseError, run_migrations, SqliteStorageDatabase};
#[cfg(feature = "sqlite")]
use std::path::Path;

#[cfg(feature = "sqlite")]
impl StorageManager<SqliteStorageDatabase> {
    /// Initialize new storage manager with SQLite database.
    ///
    /// If the storage doesn't exist yet, it will be created.
    pub async fn init(
        database_url: impl AsRef<str>,
        dir: impl AsRef<Path>,
        max_size: Option<usize>,
    ) -> Result<Self, DatabaseError> {
        let dir = std::path::absolute(dir.as_ref()).unwrap();
        fs::create_dir_all(&dir).await.unwrap();
        run_migrations(database_url.as_ref()).await?;
        let db = SqliteStorageDatabase::connect_pool(database_url.as_ref(), max_size).await?;
        Ok(Self { db, dir })
    }
}
