use std::path::PathBuf;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::database::{StorageDatabase, StorageDatabaseError};
use crate::Utc;

use super::file::{File, FileMetadata, FileSource, StorePolicy};

#[derive(Clone)]
pub struct StorageManager<D: StorageDatabase> {
    db: D,
    dir: PathBuf,
}

impl<D: StorageDatabase> StorageManager<D> {
    pub async fn add_file_from_stream<S, E>(
        &self,
        source: FileSource,
        store_policy: StorePolicy,
        filename: Option<String>,
        mut stream: S,
    ) -> Result<File<D>, ()>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::error::Error,
    {
        let path = self.path_from_source(&source);
        let now = Utc::now();
        let metadata = FileMetadata {
            source,
            filename,
            path: path.clone(),
            store_policy,
            created: now,
            last_used: now,
        };

        match self.db.store(metadata).await {
            Ok(id) => {
                let mut run = async || -> Result<File<D>, ()> {
                    let mut output = fs::File::create_new(&path).await.unwrap();
                    while let Some(chunk_result) = stream.next().await {
                        let chunk = chunk_result.unwrap();
                        output.write_all(&chunk).await.unwrap();
                    }
                    Ok(self.db.get(id).await.unwrap())
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
                todo!();
            }
            Err(err) => panic!("{:?}", err),
        }
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
    pub async fn init(
        database_url: impl AsRef<str>,
        dir: impl AsRef<Path>,
        max_size: Option<usize>,
    ) -> Result<Self, DatabaseError> {
        fs::create_dir_all(dir.as_ref()).await.unwrap();
        run_migrations(database_url.as_ref()).await?;
        let db = SqliteStorageDatabase::connect_pool(database_url.as_ref(), max_size).await?;
        let dir = dir.as_ref().to_path_buf();
        Ok(Self { db, dir })
    }
}
