//! Cache database.
//!
//! An SQLite database build with migrations from `./migrations`.

use diesel::{ConnectionError, ConnectionResult, SqliteConnection};
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::pooled_connection::deadpool;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, SimpleAsyncConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tokio::time::Duration;
use tracing::trace;

#[doc(no_inline)]
pub use diesel_async::pooled_connection::deadpool::BuildError;

use crate::errors::DatabaseError;

pub mod api;
pub mod models;
pub mod schema;

#[cfg(feature = "stresstest")]
#[cfg(test)]
mod database_stresstest;

/// Inner SQLite connection type.
pub type Connection = SyncConnectionWrapper<SqliteConnection>;

/// Result of database operations.
pub type DatabaseResult<T> = Result<T, DatabaseError>;

const MIGRATIONS: EmbeddedMigrations =
    diesel_migrations::embed_migrations!("src/database/migrations");

const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

/// Establish connection with SQLite database and configure it with:
/// - `PRAGMA journal_mode = WAL`
/// - `PRAGMA synchronous = NORMAL`
/// - `PRAGMA busy_timeout = 10_000`
fn establish_connection_inner(database_url: &str) -> BoxFuture<ConnectionResult<Connection>> {
    let fut = async move {
        trace!("establishing connection with {}", database_url);
        let mut connection = Connection::establish(database_url).await?;
        let query = format!(
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = {};",
            BUSY_TIMEOUT.as_millis()
        );
        trace!("executing: {}", &query);
        connection
            .batch_execute(&query)
            .await
            .map_err(ConnectionError::CouldntSetupConfiguration)?;
        Ok(connection)
    };
    fut.boxed()
}

/// Establish connection to SQLite database with database_url.
///
/// Attempts to establish connection with existing database.
/// If database does not exists, it will be created.
///
/// When connection established, following configs will be applied:
/// - `PRAGMA journal_mode = WAL`
/// - `PRAGMA synchronous = NORMAL`
/// - `PRAGMA busy_timeout = 10_000`
pub async fn establish_connection(database_url: &str) -> DatabaseResult<Connection> {
    Ok(establish_connection_inner(database_url).await?)
}

/// Run pending migrations on SQLite database specified with `database_url`.
pub async fn run_migrations(database_url: &str) -> DatabaseResult<()> {
    let connection = establish_connection_inner(database_url).await?;
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

/// Pool of connections to cache database.
///
/// Use [`build_pool`] or [`build_pool_with_size`] to create.
pub type Pool = deadpool::Pool<Connection>;

/// Build database connection pool.
/// Max size of the pool defaults to `cpu_count * 4`.
///
/// Backed by [`diesel_async::pooled_connection::deadpool`].
pub fn build_pool(database_url: &str) -> Result<Pool, BuildError> {
    let mut manager_config = ManagerConfig::default();
    manager_config.custom_setup = Box::new(establish_connection_inner);
    let manager =
        AsyncDieselConnectionManager::<Connection>::new_with_config(database_url, manager_config);
    Pool::builder(manager).build()
}

/// Build database connection pool of certain max size.
///
/// Backed by [`diesel_async::pooled_connection::deadpool`].
pub fn build_pool_with_size(database_url: &str, max_size: usize) -> Result<Pool, BuildError> {
    let mut manager_config = ManagerConfig::default();
    manager_config.custom_setup = Box::new(establish_connection_inner);
    let manager =
        AsyncDieselConnectionManager::<Connection>::new_with_config(database_url, manager_config);
    Pool::builder(manager).max_size(max_size).build()
}

/// Database fixtures. Helps in testing database-related code.
#[cfg(test)]
pub(crate) mod fixtures {
    use super::models::{CacheEntry, NewCacheEntry};
    use super::*;
    use crate::errors::NonUtf8PathError;
    use crate::{DateTime, FileStatus, Utc};
    use tempdir::TempDir;

    type Error = Box<dyn std::error::Error>;

    /// Fixture which creates new database as temp file.
    /// Removes database on drop.
    pub(crate) struct CacheDatabaseFixture {
        /// Just holds temp directory, which will be removed on drop.
        #[allow(dead_code)]
        tmp: TempDir,

        /// Path to database `*.sqlite` file.
        pub db_path: String,

        /// Database connection.
        pub db: Connection,
    }

    impl CacheDatabaseFixture {
        /// Just an example entry of database.
        pub fn default_new_entry() -> NewCacheEntry {
            NewCacheEntry {
                url: "http://localhost".to_string(),
                cache_path: "/var/cache/file".to_string(),
                created: DateTime::<Utc>::MIN_UTC,
                expires: None,
                status: FileStatus::Pending,
                ref_count: 0,
            }
        }

        /// Just an example entry of database which will be created from [`Self::default_new_entry`].
        pub fn default_entry() -> CacheEntry {
            CacheEntry {
                id: 1,
                url: "http://localhost".to_string(),
                cache_path: "/var/cache/file".to_string(),
                created: DateTime::<Utc>::MIN_UTC,
                expires: None,
                status: FileStatus::Pending,
                ref_count: 0,
            }
        }

        /// Create new empty temp database.
        pub async fn new() -> Result<Self, Error> {
            let tmp = TempDir::new("carol.test.database")?;
            let db_path = tmp.path().join("carol.sqlite");
            let db_path = db_path
                .as_os_str()
                .to_str()
                .ok_or(NonUtf8PathError)?
                .to_string();
            run_migrations(&db_path).await?;
            let db = establish_connection(&db_path).await?;
            Ok(Self { tmp, db_path, db })
        }

        /// Create new temp database with given entry.
        /// Returns also primary key of that entry.
        pub async fn new_with_entry(entry: NewCacheEntry) -> Result<(Self, i32), Error> {
            let mut fixture = Self::new().await?;
            let pk = unsafe { api::insert_unsafe(&mut fixture.db, entry).await? }.id;
            Ok((fixture, pk))
        }

        /// Create new temp database with default entry (from [`Self::default_entry`]).
        /// Returns also primary key of that entry.
        pub async fn new_with_default_entry() -> Result<(Self, i32), Error> {
            Self::new_with_entry(Self::default_new_entry()).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::CacheDatabaseFixture;
    use super::*;
    use crate::errors::NonUtf8PathError;
    use tempdir::TempDir;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_create_new_database() {
        let tmp = TempDir::new("carol.test").unwrap();
        let db_path = tmp.path().join("carol.sqlite");
        let db_path_str = db_path
            .as_os_str()
            .to_str()
            .ok_or(NonUtf8PathError)
            .unwrap();

        establish_connection(db_path_str)
            .await
            .expect("create new database");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_connect_to_existing_database() {
        let db = CacheDatabaseFixture::new().await.unwrap();

        establish_connection(&db.db_path)
            .await
            .expect("connect to existing database");
    }

    async fn do_some_pool_operations(pool: Pool) {
        let mut connection = pool.get().await.expect("get connection from pool");
        api::get_all(connection.as_mut())
            .await
            .expect("get all entries");
        api::new_entry(connection.as_mut(), "http://new-url", "/new/path", None)
            .await
            .expect("insert new entry");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_build_pool() {
        let (fixture, _) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();

        let pool = build_pool(&fixture.db_path).expect("build pool");
        do_some_pool_operations(pool).await;
    }

    #[tokio::test]
    #[traced_test]
    async fn test_build_pool_with_size() {
        let (fixture, _) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();

        let pool = build_pool_with_size(&fixture.db_path, 2).expect("build pool");
        do_some_pool_operations(pool).await;
    }
}
