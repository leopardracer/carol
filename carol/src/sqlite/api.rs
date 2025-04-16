//! Convenient API to interact with cache database.
//!
//! Basically just fancy wrappers around transactions on [`Connection`].

use diesel::{
    BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper,
};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl};
use tracing::trace;

use super::error::DatabaseError;
use super::models::{File, FileStatus, NewFile, StorePolicy};
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
                trace!("SELECT * WHERE id={}", pk);
                files.find(pk).select(File::as_select()).first(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Delete entry from database.
pub async fn delete(connection: &mut Connection, pk: PrimaryKey) -> DatabaseResult<()> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = files.find(pk);
                trace!("DELETE WHERE id={}", pk);
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

/// Get entry from database by source (URL or custom source).
pub async fn get_by_source(connection: &mut Connection, source: &str) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                let filter = dsl::source.eq(source);
                trace!("SELECT * WHERE source={}", source);
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

/// Get entry from database by its cache path field.
pub async fn get_by_cache_path(
    connection: &mut Connection,
    cache_path: &str,
) -> DatabaseResult<Option<File>> {
    connection
        .transaction(|conn| {
            async {
                let filter = dsl::cache_path.eq(cache_path);
                trace!("SELECT * WHERE cache_path={}", cache_path);
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
pub async fn get_by_status(
    connection: &mut Connection,
    status: FileStatus,
) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                let filter = dsl::status.eq(status);
                trace!("SELECT * WHERE status={}", status);
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
    status: FileStatus,
) -> DatabaseResult<File> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = files.find(pk);
                let assignment = dsl::status.eq(status);
                trace!("UPDATE SET status={} WHERE id={}, ", status, pk);
                diesel::update(row).set(assignment).get_result(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Get all "stale" cache entries from the database.
pub async fn get_all_stale(connection: &mut Connection) -> DatabaseResult<Vec<File>> {
    connection
        .transaction(|conn| {
            async {
                // let min_months_predicate = a::min_unit
                //     .eq(Some("Months"))
                //     .and(a::min_duration.le(Some(param1)));
                // let min_years_predicate = a::min_unit
                //     .eq(Some("Years"))
                //     .and(a::min_duration.le(Some(param1)));

                // query = query
                //     .filter(min_months_predicate.or(min_years_predicate))
                //     .filter(a::max_duration.ge(Some(param2)));
                // let x = conn.batch_execute("").await;

                trace!("SELECT * WHERE (store_policy=ExpiresAfter AND store_policy_data + created < NOW())");
                let expired_after = dsl::store_policy
                    .eq(StorePolicy::ExpiresAfter)
                    .and((dsl::store_policy_data).gt(123));
                let expired_after_not_used = dsl::store_policy
                    .eq(StorePolicy::ExpiresAfterNotUsedFor)
                    .and(dsl::store_policy_data.gt(123));
                files
                    .filter(expired_after.or(expired_after_not_used))
                    .select(File::as_select())
                    .get_results(conn)
                    .await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}
