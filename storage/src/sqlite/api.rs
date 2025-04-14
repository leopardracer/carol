//! Convenient API to interact with cache database.
//!
//! Basically just fancy wrappers around transactions on [`Connection`].

use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl};
use tracing::trace;

use crate::database;

use super::error::{DatabaseError, RemoveErrorReason};
use super::models::{File, FileStatus, NewFile};
use super::schema::files::dsl::{self, files};
use super::{Connection, DatabaseResult, PrimaryKey};

/// Insert new entry to database.
pub async fn insert(connection: &mut Connection, new_entry: NewFile) -> DatabaseResult<File> {
    connection
        .immediate_transaction(|conn| {
            async {
                trace!("INSERT {:?}", new_entry);
                let inserted = diesel::insert_into(files)
                    .values(&new_entry)
                    .get_result::<File>(conn)
                    .await?;
                Ok(inserted)
            }
            .scope_boxed()
        })
        .await
}

/// Get entry from database by primary key.
pub async fn get(connection: &mut Connection, pk: PrimaryKey) -> DatabaseResult<File> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT pk={}", pk);
                files.find(pk).select(File::as_select()).first(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Delete entry from database.
///
/// Returns error if reference counter is not 0 or status is not `ToRemove`.
pub async fn delete(connection: &mut Connection, pk: PrimaryKey) -> DatabaseResult<()> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = files.find(pk);
                let entry: File = files
                    .find(pk)
                    .select(File::as_select())
                    .first(conn)
                    .await
                    .map_err(DatabaseError::from)?;
                if entry.ref_count != 0 {
                    return Err(DatabaseError::RemoveError(RemoveErrorReason::UsedFile));
                }
                if entry.status != database::FileStatus::ToRemove {
                    return Err(DatabaseError::RemoveError(RemoveErrorReason::WrongStatus));
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
/// The caller must ensure that file is not used (ref_count = 0)
/// and the status is set to `ToRemove`.
pub async unsafe fn delete_unsafe(
    connection: &mut Connection,
    pk: PrimaryKey,
) -> DatabaseResult<()> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = files.find(pk);
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
pub async fn get_all(connection: &mut Connection) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT *");
                files.select(File::as_select()).get_results(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Get entry from database by URL field.
pub async fn get_by_url(connection: &mut Connection, url: &str) -> DatabaseResult<Option<File>> {
    connection
        .transaction(|conn| {
            async {
                let filter = dsl::url.eq(url);
                trace!("SELECT url={}", url);
                files
                    .filter(filter)
                    .select(File::as_select())
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
    connection: &mut Connection,
    cache_path: &str,
) -> DatabaseResult<Option<File>> {
    connection
        .transaction(|conn| {
            async {
                let filter = dsl::cache_path.eq(cache_path);
                trace!("SELECT cache_path={}", cache_path);
                files
                    .filter(filter)
                    .select(File::as_select())
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
    connection: &mut Connection,
    status: database::FileStatus,
) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                let filter = dsl::status.eq(FileStatus(status));
                trace!("SELECT status={}", status);
                files
                    .filter(filter)
                    .select(File::as_select())
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
    connection: &mut Connection,
    pk: PrimaryKey,
    status: database::FileStatus,
) -> DatabaseResult<File> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = files.find(pk);
                let assignment = dsl::status.eq(FileStatus(status));
                trace!("UPDATE pk={}, status = {:?}", pk, status);
                diesel::update(row).set(assignment).get_result(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Add one reference to the counter (`ref_count++`). Returns updated entry.
pub async fn increment_ref(connection: &mut Connection, pk: PrimaryKey) -> DatabaseResult<File> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = files.find(pk);
                let entry: File = diesel::update(row)
                    .set(dsl::ref_count.eq(dsl::ref_count + 1))
                    .get_result(conn)
                    .await?;
                trace!("UPDATE pk={}, ref count = {}", pk, entry.ref_count);
                Ok::<File, DatabaseError>(entry)
            }
            .scope_boxed()
        })
        .await
}

/// Remove one reference from the counter (`ref_count--`). Returns updated entry.
pub async fn decrement_ref(connection: &mut Connection, pk: PrimaryKey) -> DatabaseResult<File> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = files.find(pk);
                let entry: File = diesel::update(row)
                    .set(dsl::ref_count.eq(dsl::ref_count - 1))
                    .get_result(conn)
                    .await?;
                trace!("UPDATE pk={}, ref count = {}", pk, entry.ref_count);
                Ok::<File, DatabaseError>(entry)
            }
            .scope_boxed()
        })
        .await
}
