use chrono::{DateTime, Utc};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper, SqliteConnection};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use tokio::time::Duration;
use tracing::{debug, trace};

use crate::errors::{DatabaseError, RemoveError};
use crate::models::{CacheEntry, NewCacheEntry};
use crate::FileStatus;

const MIGRATIONS: EmbeddedMigrations = diesel_migrations::embed_migrations!();

/// Connection to database holding metadata of cached files.
///
/// Provides convenient API for Carol cache database.
///
/// Backed by SQLite.
pub struct CacheDatabase {
    connection: SyncConnectionWrapper<SqliteConnection>,
}

impl CacheDatabase {
    const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

    /// Initialize database from path to SQlite database.
    ///
    /// Attempts to establish connection with existing database.
    /// If database does not exists, it will be created.
    ///
    /// Then runs migrations on the database.
    pub async fn init(path: &str) -> Result<Self, DatabaseError> {
        debug!("running migrations");
        let mut migrations_connection = <SqliteConnection as diesel::Connection>::establish(path)
            .map_err(DatabaseError::ConnectionError)?;
        migrations_connection
            .run_pending_migrations(MIGRATIONS)
            .map_err(|err| DatabaseError::MigrationError(err.to_string()))?;
        debug!("migrations done");

        debug!("connecting to {}", path);
        let mut connection = SyncConnectionWrapper::<SqliteConnection>::establish(path)
            .await
            .map_err(DatabaseError::ConnectionError)?;

        debug!(
            "setting up: WAL mode, busy_timeout = {}",
            Self::BUSY_TIMEOUT.as_millis()
        );
        connection
            .batch_execute("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
            .await?;
        connection
            .batch_execute(&format!(
                "PRAGMA busy_timeout = {};",
                Self::BUSY_TIMEOUT.as_millis()
            ))
            .await?;

        Ok(Self { connection })
    }

    /// Raw insert to database.
    ///
    /// Consider using [`Self::new_entry`] instead.
    ///
    /// # Safety
    ///
    /// Although it is technically safe to use this method, it may confuse
    /// semantics of the fields.
    pub async unsafe fn insert_unsafe(
        &mut self,
        new_entry: NewCacheEntry,
    ) -> Result<CacheEntry, DatabaseError> {
        self.connection
            .immediate_transaction(|conn| {
                async {
                    trace!("INSERT {:?}", new_entry);
                    let inserted = diesel::insert_into(crate::schema::files::table)
                        .values(&new_entry)
                        .get_result::<CacheEntry>(conn)
                        .await?;
                    Ok(inserted)
                }
                .scope_boxed()
            })
            .await
    }

    /// Add new entry to database.
    ///
    /// `created` timestamp is set to current UTC.
    /// `status` is set to default (`Pending`).
    pub async fn new_entry(
        &mut self,
        url: &str,
        cache_path: &str,
        expires: Option<DateTime<Utc>>,
    ) -> Result<CacheEntry, DatabaseError> {
        let new_entry = NewCacheEntry::new(url, cache_path, expires);
        unsafe { self.insert_unsafe(new_entry).await }
    }

    /// Get entry from database by primary key.
    pub async fn get_entry(&mut self, pk: i32) -> Result<CacheEntry, DatabaseError> {
        self.connection
            .transaction(|conn| {
                async {
                    trace!("SELECT pk={}", pk);
                    crate::schema::files::dsl::files
                        .find(pk)
                        .select(CacheEntry::as_select())
                        .first(conn)
                        .await
                }
                .scope_boxed()
            })
            .await
            .map_err(Into::into)
    }

    /// Remove entry from database.
    ///
    /// Returns error if reference counter is not 0 or status is not `ToRemove`.
    pub async fn remove_entry(&mut self, pk: i32) -> Result<(), DatabaseError> {
        self.connection
            .immediate_transaction(|conn| {
                async {
                    let row = crate::schema::files::dsl::files.find(pk);
                    let entry: CacheEntry = crate::schema::files::dsl::files
                        .find(pk)
                        .select(CacheEntry::as_select())
                        .first(conn)
                        .await
                        .map_err(DatabaseError::from)?;
                    if entry.ref_count != 0 {
                        return Err(DatabaseError::RemoveError(RemoveError::UsedFile));
                    }
                    if entry.status != FileStatus::ToRemove {
                        return Err(DatabaseError::RemoveError(RemoveError::WrongStatus));
                    }
                    trace!("DELETE pk={}", pk);
                    diesel::delete(row)
                        .execute(conn)
                        .await
                        .map_err(DatabaseError::from)?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
    }

    /// Raw database delete. Do not check reference counter or status.
    ///
    /// # Safety
    ///
    /// The caller must ensure that file is not used (ref_count = 0).
    pub async unsafe fn delete_unsafe(&mut self, pk: i32) -> Result<(), DatabaseError> {
        self.connection
            .immediate_transaction(|conn| {
                async {
                    let row = crate::schema::files::dsl::files.find(pk);
                    trace!("DELETE pk={}", pk);
                    diesel::delete(row).execute(conn).await?;
                    Ok(())
                }
                .scope_boxed()
            })
            .await
            .map_err(diesel::result::Error::into)
    }

    /// Get all cache entries from database.
    pub async fn get_all(&mut self) -> Result<Vec<CacheEntry>, DatabaseError> {
        self.connection
            .transaction(|conn| {
                async {
                    trace!("SELECT *");
                    crate::schema::files::dsl::files
                        .select(CacheEntry::as_select())
                        .get_results(conn)
                        .await
                }
                .scope_boxed()
            })
            .await
            .map_err(Into::into)
    }

    /// Get entry from database by URL field.
    pub async fn get_by_url(&mut self, url: &str) -> Result<Option<CacheEntry>, DatabaseError> {
        self.connection
            .transaction(|conn| {
                async {
                    let filter = crate::schema::files::dsl::url.eq(url);
                    trace!("SELECT url={}", url);
                    crate::schema::files::dsl::files
                        .filter(filter)
                        .select(CacheEntry::as_select())
                        .first(conn)
                        .await
                        .optional()
                }
                .scope_boxed()
            })
            .await
            .map_err(Into::into)
    }

    /// Get entry from database by its cache path field.
    pub async fn get_by_cache_path(
        &mut self,
        cache_path: &str,
    ) -> Result<Option<CacheEntry>, DatabaseError> {
        self.connection
            .transaction(|conn| {
                async {
                    let filter = crate::schema::files::dsl::cache_path.eq(cache_path);
                    trace!("SELECT cache_path={}", cache_path);
                    crate::schema::files::dsl::files
                        .filter(filter)
                        .select(CacheEntry::as_select())
                        .first(conn)
                        .await
                        .optional()
                }
                .scope_boxed()
            })
            .await
            .map_err(Into::into)
    }

    /// Get all cache entries with given `status`.
    pub async fn filter_by_status(
        &mut self,
        status: FileStatus,
    ) -> Result<Vec<CacheEntry>, DatabaseError> {
        self.connection
            .transaction(|conn| {
                async {
                    let filter = crate::schema::files::dsl::status.eq(status);
                    trace!("SELECT status={}", status);
                    crate::schema::files::dsl::files
                        .filter(filter)
                        .select(CacheEntry::as_select())
                        .get_results(conn)
                        .await
                }
                .scope_boxed()
            })
            .await
            .map_err(Into::into)
    }

    /// Update status of entry. Returns updated entry.
    pub async fn update_status(
        &mut self,
        pk: i32,
        status: FileStatus,
    ) -> Result<CacheEntry, DatabaseError> {
        self.connection
            .immediate_transaction(|conn| {
                async {
                    let row = crate::schema::files::dsl::files.find(pk);
                    let assignment = crate::schema::files::dsl::status.eq(status);
                    trace!("UPDATE pk: {}, status = {:?}", pk, status);
                    diesel::update(row).set(assignment).get_result(conn).await
                }
                .scope_boxed()
            })
            .await
            .map_err(Into::into)
    }

    /// Add one reference to the counter (`ref_count++`). Returns updated entry.
    pub async fn increment_ref(&mut self, pk: i32) -> Result<CacheEntry, DatabaseError> {
        self.connection
            .immediate_transaction(|conn| {
                async {
                    let row = crate::schema::files::dsl::files.find(pk);
                    use crate::schema::files::dsl::ref_count;
                    diesel::update(row)
                        .set(ref_count.eq(ref_count + 1))
                        .get_result(conn)
                        .await
                }
                .scope_boxed()
            })
            .await
            .map_err(Into::into)
    }

    /// Remove one reference from the counter (`ref_count--`). Returns updated entry.
    pub async fn decrement_ref(&mut self, pk: i32) -> Result<CacheEntry, DatabaseError> {
        self.connection
            .immediate_transaction(|conn| {
                async {
                    let row = crate::schema::files::dsl::files.find(pk);
                    use crate::schema::files::dsl::ref_count;
                    diesel::update(row)
                        .set(ref_count.eq(ref_count - 1))
                        .get_result(conn)
                        .await
                }
                .scope_boxed()
            })
            .await
            .map_err(Into::into)
    }

    /// Update `expires` field for cache entry.
    pub async fn update_expires(
        &mut self,
        pk: i32,
        expires: Option<DateTime<Utc>>,
    ) -> Result<CacheEntry, DatabaseError> {
        self.connection
            .immediate_transaction(|conn| {
                async {
                    let row = crate::schema::files::dsl::files.find(pk);
                    diesel::update(row)
                        .set(crate::schema::files::dsl::expires.eq(expires))
                        .get_result(conn)
                        .await
                }
                .scope_boxed()
            })
            .await
            .map_err(Into::into)
    }
}
