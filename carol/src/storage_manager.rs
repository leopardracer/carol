use std::error::Error as StdError;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use chrono::Utc;
use futures_util::{Stream, StreamExt};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time;
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::database::{StorageDatabase, StorageDatabaseError, StorageDatabaseExt};
use crate::error::StorageError;
use crate::file::{File, FileMetadata, FileSource, FileStatus, StorePolicy};
use crate::sqlite::{self, run_migrations, SqliteStorageDatabase};
use crate::storage_config::StorageConfig;

/// Storage manager. This is an adapter to interact with Carol storage.
#[derive(Clone, Debug)]
pub struct StorageManager<D: StorageDatabase = SqliteStorageDatabase> {
    db: D,
    dir: PathBuf,
    config: StorageConfig,
}

impl<D: StorageDatabase> StorageManager<D> {
    /// Returns reference to config of this storage.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Generate cached file path from [`FileSource`].
    ///
    /// Applies SHA256 to `source` string representation
    /// to generate the file name in cache directory.
    pub fn path_from_source(&self, source: &FileSource) -> PathBuf {
        self.dir.join(sha256::digest(source))
    }
}

impl<D: StorageDatabaseExt> StorageManager<D> {
    /// Add new file to storage. Content of the file is read from `stream`.
    ///
    /// "Create" and "last used" timestamps of the file will be set to `Utc::now()`.
    /// File path is defined by [`Self::path_from_source`].
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
                    // TODO: catch "no space left" error and evict something from storage
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
                // TODO: check that file is not stale
                Ok(file)
            }
            Err(err) => panic!("{:?}", err),
        }
    }

    /// Add new file to storage by **copying** it from local path.
    ///
    /// "Create" and "last used" timestamps of the file will be set to `Utc::now()`.
    /// File path is defined by [`Self::path_from_source`].
    ///
    /// **Note:** if you are using local path as `source`, keep in mind that sources are unique in
    /// the storage. Because of that the same call for a modified local file **will not update** the
    /// file in the storage.
    pub async fn copy_local_file(
        &self,
        source: FileSource,
        store_policy: StorePolicy,
        filename: Option<String>,
        path: impl AsRef<Path>,
    ) -> Result<File, StorageError<D::Error>> {
        let file = fs::File::open(path.as_ref()).await?;
        let stream =
            FramedRead::new(file, BytesCodec::new()).map(|item| item.map(BytesMut::freeze));
        self.add_file_from_stream(source, store_policy, filename, stream)
            .await
    }

    /// Find file in storage by its source.
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
}

impl StorageManager {
    /// Initialize new storage manager with SQLite database.
    ///
    /// If the storage database doesn't exist yet, it will be created.
    ///
    /// # Arguments
    ///
    /// - `database_url` - URL of SQLite database to use. Typically this is just a path,
    ///   e.g. `/path/to/db.sqlite`.
    /// - `dir` - path to storage directory, where the actual files will reside.
    ///   This **must be** an absolute path. This directory **must** exist.
    /// - `pool_size` - size of the database connection pool. If `None`, defaults to `cpu_count * 4`.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - `dir` is not absolute
    /// - `dir` does not exists or is not a directory
    /// - connection to database failed
    /// - running migrations on the database failed
    pub async fn init(
        database_url: impl AsRef<str>,
        dir: impl AsRef<Path>,
        pool_size: Option<usize>,
    ) -> Result<Self, StorageError<sqlite::error::DatabaseError>> {
        Self::init_with_config(database_url, dir, pool_size, StorageConfig::default()).await
    }

    /// Provide custom storage configuration. See [`Self::init`] for more info.
    pub async fn init_with_config(
        database_url: impl AsRef<str>,
        dir: impl AsRef<Path>,
        pool_size: Option<usize>,
        config: StorageConfig,
    ) -> Result<Self, StorageError<sqlite::error::DatabaseError>> {
        if !dir.as_ref().is_absolute() {
            return Err(StorageError::StorageDirectoryPathIsNotAbsolute);
        }
        if !dir.as_ref().is_dir() {
            return Err(StorageError::StorageDirectoryDoesNotExist);
        }
        let dir = dir.as_ref().to_path_buf();
        run_migrations(database_url.as_ref()).await?;
        let db = SqliteStorageDatabase::connect_pool(database_url.as_ref(), pool_size).await?;
        Ok(Self { db, dir, config })
    }
}

#[cfg(test)]
mod tests {
    use super::StorageManager;
    use crate::database::mocks::MockStorageDatabaseExt;
    use crate::file::{File, FileId, FileMetadata, FileSource, FileStatus, StorePolicy};
    use bytes::Bytes;
    use chrono::Utc;
    use tokio::fs;

    #[derive(Debug)]
    struct TestError;

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self)
        }
    }

    impl std::error::Error for TestError {}

    #[tokio::test]
    async fn test_add_file_from_stream() {
        // Set up initial data
        let database_url = "someurl".to_string();
        let tmp = tempfile::tempdir().unwrap();
        let source = FileSource::Custom("somesource".to_string());
        let store_policy = StorePolicy::StoreForever;
        let data: Vec<Result<Bytes, TestError>> =
            vec![Ok(Bytes::from("hello ")), Ok(Bytes::from("world"))];
        let stream = futures_util::stream::iter(data);
        let filename = None;
        let path = tmp
            .path()
            .join("6f87d01289b1845908a7c7ccd578fddbbcefd29f6144bbab658baa9f6aae2809");

        // Set up database mock
        let mut mock = MockStorageDatabaseExt::new();
        let file_id = FileId::from(1i32);
        let metadata = FileMetadata {
            source: source.clone(),
            filename: filename.clone(),
            path: path.clone(),
            store_policy,
            created: Utc::now(),
            last_used: Utc::now(),
        };

        let metadata_clone = metadata.clone();
        mock.expect_store()
            .withf(move |metadata| {
                metadata.filename == metadata_clone.filename
                    && metadata.path == metadata_clone.path
                    && metadata.source == metadata_clone.source
                    && metadata.store_policy == store_policy
            })
            .return_once(move |_| Ok(file_id));

        let database_url_clone = database_url.clone();
        mock.expect_update_status()
            .withf(move |id, new_status| *id == file_id && *new_status == FileStatus::Ready)
            .return_once(move |id, status| {
                Ok(File {
                    database: database_url_clone,
                    id,
                    status,
                    metadata,
                })
            });

        // Create manager
        let manager = StorageManager::<MockStorageDatabaseExt> {
            db: mock,
            dir: tmp.path().to_path_buf(),
            config: Default::default(),
        };

        let file = manager
            .add_file_from_stream(source.clone(), store_policy, filename.clone(), stream)
            .await
            .expect("add file from stream");

        assert_eq!(file.database, database_url);
        assert_eq!(file.id, file_id);
        assert_eq!(file.status, FileStatus::Ready);
        assert_eq!(file.metadata.filename, filename);
        assert_eq!(file.metadata.path, path);
        assert_eq!(file.metadata.source, source);
        assert_eq!(file.metadata.store_policy, store_policy);
        let content = fs::read_to_string(&file.metadata.path)
            .await
            .expect("read content");
        assert_eq!(content.as_str(), "hello world");
    }

    #[tokio::test]
    async fn test_copy_local_file() {
        // Set up initial data
        let localtmp = tempfile::tempdir().unwrap();
        let localfile_path = localtmp.path().join("localfile");
        fs::write(&localfile_path, "hello world").await.unwrap();

        let database_url = "someurl".to_string();
        let tmp = tempfile::tempdir().unwrap();
        let source = FileSource::Custom("somesource".to_string());
        let store_policy = StorePolicy::StoreForever;
        let filename = None;
        let path = tmp
            .path()
            .join("6f87d01289b1845908a7c7ccd578fddbbcefd29f6144bbab658baa9f6aae2809");

        // Set up database mock
        let mut mock = MockStorageDatabaseExt::new();
        let file_id = FileId::from(1i32);
        let metadata = FileMetadata {
            source: source.clone(),
            filename: filename.clone(),
            path: path.clone(),
            store_policy,
            created: Utc::now(),
            last_used: Utc::now(),
        };

        let metadata_clone = metadata.clone();
        mock.expect_store()
            .withf(move |metadata| {
                metadata.filename == metadata_clone.filename
                    && metadata.path == metadata_clone.path
                    && metadata.source == metadata_clone.source
                    && metadata.store_policy == store_policy
            })
            .return_once(move |_| Ok(file_id));

        let database_url_clone = database_url.clone();
        mock.expect_update_status()
            .withf(move |id, new_status| *id == file_id && *new_status == FileStatus::Ready)
            .return_once(move |id, status| {
                Ok(File {
                    database: database_url_clone,
                    id,
                    status,
                    metadata,
                })
            });

        // Create manager
        let manager = StorageManager::<MockStorageDatabaseExt> {
            db: mock,
            dir: tmp.path().to_path_buf(),
            config: Default::default(),
        };

        let file = manager
            .copy_local_file(
                source.clone(),
                store_policy,
                filename.clone(),
                &localfile_path,
            )
            .await
            .expect("copy local file");

        assert_eq!(file.database, database_url);
        assert_eq!(file.id, file_id);
        assert_eq!(file.status, FileStatus::Ready);
        assert_eq!(file.metadata.filename, filename);
        assert_eq!(file.metadata.path, path);
        assert_eq!(file.metadata.source, source);
        assert_eq!(file.metadata.store_policy, store_policy);
        let content = fs::read_to_string(&file.metadata.path)
            .await
            .expect("read content");
        assert_eq!(content.as_str(), "hello world");
    }
}
