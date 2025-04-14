//! Implementation of storage database backed by SQLite.
//!
//! An SQLite database build with migrations from `./migrations`.

use std::fmt;
use std::path::PathBuf;

use async_trait::async_trait;
use diesel::{ConnectionError, ConnectionResult, SqliteConnection};
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, SimpleAsyncConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tokio::time::{self, Duration};
use tracing::trace;

use crate::database::{FileStatus, StorageDatabase};
use crate::file::{File, FileId, FileMetadata, FileSource};

#[allow(dead_code)]
mod api;
mod models;
mod schema;

pub mod error;

use error::DatabaseError;

/// Inner SQLite connection type.
type Connection = SyncConnectionWrapper<SqliteConnection>;

/// SQLite pool connection manager type.
type ConnectionManager = AsyncDieselConnectionManager<Connection>;

/// Result of database operations.
pub type DatabaseResult<T> = Result<T, DatabaseError>;

/// Primary key type.
type PrimaryKey = i32;

const MIGRATIONS: EmbeddedMigrations =
    diesel_migrations::embed_migrations!("src/sqlite/migrations");

const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

const RETRY_TIMES: usize = 3;
const RETRY_PERIOD: Duration = Duration::from_millis(100);

/// Establish connection with SQLite database and configure it with:
///
/// - `PRAGMA journal_mode = WAL`
/// - `PRAGMA synchronous = NORMAL`
/// - `PRAGMA busy_timeout = 10_000`
///
/// We really want this to succeed, that's why we retry.
fn establish_connection(database_url: &str) -> BoxFuture<ConnectionResult<Connection>> {
    let fut = async move {
        trace!("establishing connection with {}", database_url);
        let mut connection = Connection::establish(database_url).await?;

        let query = format!(
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = {};",
            BUSY_TIMEOUT.as_millis()
        );
        trace!("executing: {}", &query);
        let result = connection.batch_execute(&query).await;

        match result {
            Ok(_) => Ok(connection),
            Err(err) => {
                trace!("SQLite connection configuration failed: {:?}", err);
                let mut result = Err(ConnectionError::CouldntSetupConfiguration(err));
                for i in 0..RETRY_TIMES {
                    trace!("retrying, attempt #{}", i + 1);
                    time::sleep(RETRY_PERIOD).await;
                    if let Err(err) = connection.batch_execute(&query).await {
                        // Last occurred error will be returned
                        trace!("SQLite connection configuration failed: {:?}", err);
                        result = Err(ConnectionError::CouldntSetupConfiguration(err));
                    } else {
                        result = Ok(connection);
                        break;
                    }
                }
                result
            }
        }
    };
    fut.boxed()
}

/// Run pending migrations on SQLite database specified with `database_url`.
pub async fn run_migrations(database_url: &str) -> DatabaseResult<()> {
    let connection = establish_connection(database_url).await?;
    let mut async_wrapper: AsyncConnectionWrapper<Connection> =
        AsyncConnectionWrapper::from(connection);

    trace!("running pending migrations");
    tokio::task::spawn_blocking(move || {
        let applied = async_wrapper
            .run_pending_migrations(MIGRATIONS)
            .expect("run migrations");
        if applied.is_empty() {
            trace!("no migrations applied");
        } else {
            trace!("migrations applied:");
            for migration in &applied {
                trace!(" - {}", migration);
            }
        }
    })
    .await
    .map_err(|e| DatabaseError::MigrationError(e.to_string()))
}

/// Storage database backed by SQLite.
#[derive(Clone)]
pub struct SqliteStorageDatabase {
    database_url: String,
    pool: Pool<Connection>,
}

impl SqliteStorageDatabase {
    /// Build connection pool for SQLite database with `database_url`.
    ///
    /// For each established connection following configs will be applied:
    /// - `PRAGMA journal_mode = WAL`
    /// - `PRAGMA synchronous = NORMAL`
    /// - `PRAGMA busy_timeout = 10_000`
    ///
    /// If SQLite database does not exists, it will be created.
    ///
    /// `max_size` defines the maximum size of the pool and defaults to `cpu_count * 4`.
    pub async fn connect_pool(database_url: &str, max_size: Option<usize>) -> DatabaseResult<Self> {
        let mut config = ManagerConfig::default();
        config.custom_setup = Box::new(establish_connection);
        let manager = ConnectionManager::new_with_config(database_url, config);
        let mut pool_builder = Pool::builder(manager);
        if let Some(max_size) = max_size {
            pool_builder = pool_builder.max_size(max_size);
        }
        let pool = pool_builder.build()?;
        Ok(Self {
            pool,
            database_url: database_url.to_string(),
        })
    }
}

impl fmt::Debug for SqliteStorageDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStorageDatabase")
            .field("database_url", &self.database_url)
            .finish()
    }
}

fn to_pk(id: FileId) -> PrimaryKey {
    u32::from(id) as i32
}

#[async_trait]
impl StorageDatabase for SqliteStorageDatabase {
    type Uri = String;

    type Error = DatabaseError;

    fn uri(&self) -> String {
        self.database_url.clone()
    }

    async fn store(&self, metadata: FileMetadata) -> DatabaseResult<FileId> {
        let new_file = models::NewFile {
            url: metadata.source.to_string(),
            cache_path: metadata.path.as_os_str().to_str().unwrap().to_string(),
            filename: metadata.filename,
            created: metadata.created,
            last_used: metadata.last_used,
            cache_policy: metadata.store_policy.into(),
            status: FileStatus::default().into(),
            ref_count: 0,
        };
        let mut conn = self.pool.get().await?;
        let entry = api::insert(conn.as_mut(), new_file).await?;
        let id: u32 = entry.id as u32;
        Ok(id.into())
    }

    async fn get(&self, id: FileId) -> DatabaseResult<File<Self>> {
        let mut conn = self.pool.get().await?;
        let file = api::get(conn.as_mut(), to_pk(id)).await?;
        Ok(File {
            database: self.clone(),
            id,
            metadata: FileMetadata {
                source: FileSource::parse(&file.url),
                filename: file.filename,
                path: PathBuf::from(file.cache_path),
                created: file.created,
                last_used: file.last_used,
                store_policy: file.cache_policy.into(),
            },
        })
    }

    async fn remove(&self, id: FileId) -> DatabaseResult<()> {
        let mut conn = self.pool.get().await?;
        api::delete(conn.as_mut(), to_pk(id)).await
    }
}
