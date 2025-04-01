//! Convenient API to interact with cache database.
//!
//! Basically just fancy wrappers around transactions on [`Connection`].

use chrono::{DateTime, Utc};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::{AsyncConnection, RunQueryDsl};
use tracing::trace;

use crate::database::models::{CacheEntry, NewCacheEntry};
use crate::errors::{DatabaseError, RemoveErrorReason};
use crate::FileStatus;

use super::{Connection, DatabaseResult};

/// Raw insert to database.
///
/// Consider using [`Self::new_entry`] instead.
///
/// # Safety
///
/// Although it is technically safe to use this method, it may confuse
/// semantics of the fields.
pub async unsafe fn insert_unsafe(
    connection: &mut Connection,
    new_entry: NewCacheEntry,
) -> DatabaseResult<CacheEntry> {
    connection
        .immediate_transaction(|conn| {
            async {
                trace!("INSERT {:?}", new_entry);
                let inserted = diesel::insert_into(crate::database::schema::files::table)
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
    connection: &mut Connection,
    url: &str,
    cache_path: &str,
    expires: Option<DateTime<Utc>>,
) -> DatabaseResult<CacheEntry> {
    let new_entry = NewCacheEntry::new(url, cache_path, expires);
    unsafe { insert_unsafe(connection, new_entry).await }
}

/// Get entry from database by primary key.
pub async fn get_entry(connection: &mut Connection, pk: i32) -> DatabaseResult<CacheEntry> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT pk={}", pk);
                crate::database::schema::files::dsl::files
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
pub async fn remove_entry(connection: &mut Connection, pk: i32) -> DatabaseResult<()> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = crate::database::schema::files::dsl::files.find(pk);
                let entry: CacheEntry = crate::database::schema::files::dsl::files
                    .find(pk)
                    .select(CacheEntry::as_select())
                    .first(conn)
                    .await
                    .map_err(DatabaseError::from)?;
                if entry.ref_count != 0 {
                    return Err(DatabaseError::RemoveError(RemoveErrorReason::UsedFile));
                }
                if entry.status != FileStatus::ToRemove {
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
/// The caller must ensure that file is not used (ref_count = 0).
pub async unsafe fn delete_unsafe(connection: &mut Connection, pk: i32) -> DatabaseResult<()> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = crate::database::schema::files::dsl::files.find(pk);
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
pub async fn get_all(connection: &mut Connection) -> DatabaseResult<Vec<CacheEntry>> {
    connection
        .transaction(|conn| {
            async {
                trace!("SELECT *");
                crate::database::schema::files::dsl::files
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
pub async fn get_by_url(
    connection: &mut Connection,
    url: &str,
) -> DatabaseResult<Option<CacheEntry>> {
    connection
        .transaction(|conn| {
            async {
                let filter = crate::database::schema::files::dsl::url.eq(url);
                trace!("SELECT url={}", url);
                crate::database::schema::files::dsl::files
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
    connection: &mut Connection,
    cache_path: &str,
) -> DatabaseResult<Option<CacheEntry>> {
    connection
        .transaction(|conn| {
            async {
                let filter = crate::database::schema::files::dsl::cache_path.eq(cache_path);
                trace!("SELECT cache_path={}", cache_path);
                crate::database::schema::files::dsl::files
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
    connection: &mut Connection,
    status: FileStatus,
) -> DatabaseResult<Vec<CacheEntry>> {
    connection
        .transaction(|conn| {
            async {
                let filter = crate::database::schema::files::dsl::status.eq(status);
                trace!("SELECT status={}", status);
                crate::database::schema::files::dsl::files
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
    connection: &mut Connection,
    pk: i32,
    status: FileStatus,
) -> DatabaseResult<CacheEntry> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = crate::database::schema::files::dsl::files.find(pk);
                let assignment = crate::database::schema::files::dsl::status.eq(status);
                trace!("UPDATE pk: {}, status = {:?}", pk, status);
                diesel::update(row).set(assignment).get_result(conn).await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

/// Add one reference to the counter (`ref_count++`). Returns updated entry.
pub async fn increment_ref(connection: &mut Connection, pk: i32) -> DatabaseResult<CacheEntry> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = crate::database::schema::files::dsl::files.find(pk);
                use crate::database::schema::files::dsl::ref_count;
                trace!("UPDATE pk: {}, increment ref count", pk);
                let entry: CacheEntry = diesel::update(row)
                    .set(ref_count.eq(ref_count + 1))
                    .get_result(conn)
                    .await?;
                trace!("RESULT pk: {} ref count = {}", pk, entry.ref_count);
                Ok::<CacheEntry, DatabaseError>(entry)
            }
            .scope_boxed()
        })
        .await
}

/// Remove one reference from the counter (`ref_count--`). Returns updated entry.
pub async fn decrement_ref(connection: &mut Connection, pk: i32) -> DatabaseResult<CacheEntry> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = crate::database::schema::files::dsl::files.find(pk);
                use crate::database::schema::files::dsl::ref_count;
                trace!("UPDATE pk: {}, decrement ref count", pk);
                let entry: CacheEntry = diesel::update(row)
                    .set(ref_count.eq(ref_count - 1))
                    .get_result(conn)
                    .await?;
                trace!("RESULT pk: {} ref count = {}", pk, entry.ref_count);
                Ok::<CacheEntry, DatabaseError>(entry)
            }
            .scope_boxed()
        })
        .await
}

/// Update `expires` field for cache entry.
pub async fn update_expires(
    connection: &mut Connection,
    pk: i32,
    expires: Option<DateTime<Utc>>,
) -> DatabaseResult<CacheEntry> {
    connection
        .immediate_transaction(|conn| {
            async {
                let row = crate::database::schema::files::dsl::files.find(pk);
                diesel::update(row)
                    .set(crate::database::schema::files::dsl::expires.eq(expires))
                    .get_result(conn)
                    .await
            }
            .scope_boxed()
        })
        .await
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::fixtures::CacheDatabaseFixture;
    use crate::errors::DieselError;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_new_entry() {
        let db_fixture = CacheDatabaseFixture::new().await.unwrap();
        let mut db = db_fixture.db;

        let entry = new_entry(&mut db, "http://localhost", "/var/cache/file", None)
            .await
            .expect("add new entry");
        assert_eq!(entry.url, "http://localhost".to_string());
        assert_eq!(entry.status, FileStatus::Pending);
        assert_eq!(entry.cache_path, "/var/cache/file".to_string());
        assert_eq!(entry.expires, None);
        assert_eq!(entry.ref_count, 0);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_get_entry() {
        let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        let entry = get_entry(&mut db, pk).await.expect("get entry");
        assert_eq!(entry, CacheDatabaseFixture::default_entry());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_get_entry_by_url() {
        let (db_fixture, _) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        let entry = get_by_url(&mut db, "http://localhost")
            .await
            .expect("get entry by url")
            .expect("get some entry by url");
        assert_eq!(entry, CacheDatabaseFixture::default_entry());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_get_entry_by_cache_path() {
        let (db_fixture, _) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        let entry = get_by_cache_path(&mut db, "/var/cache/file")
            .await
            .expect("get entry")
            .expect("get some entry by cache path");
        assert_eq!(entry, CacheDatabaseFixture::default_entry());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_get_all() {
        let (db_fixture, _) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        let all = get_all(&mut db).await.expect("get all entries");
        assert_eq!(all.len(), 1);
        let entry = &all[0];
        assert_eq!(entry, &CacheDatabaseFixture::default_entry());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_filter_by_status() {
        let (db_fixture, _) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        let all = filter_by_status(&mut db, FileStatus::Pending)
            .await
            .expect("get all pending entries");
        assert_eq!(all.len(), 1);
        let entry = &all[0];
        assert_eq!(entry, &CacheDatabaseFixture::default_entry());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_update_expires() {
        let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        update_expires(&mut db, pk, Some(DateTime::<Utc>::MAX_UTC))
            .await
            .expect("update expiration timestamp");

        let entry = get_entry(&mut db, pk).await.unwrap();
        assert_eq!(entry.expires, Some(DateTime::<Utc>::MAX_UTC));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_update_status() {
        let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        update_status(&mut db, pk, FileStatus::Ready)
            .await
            .expect("update file status");

        let entry = get_entry(&mut db, pk).await.unwrap();
        assert_eq!(entry.status, FileStatus::Ready);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_new_entry_failure() {
        let (db_fixture, _) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        let result = new_entry(&mut db, "http://localhost", "/var/cache/file2", None).await;

        assert!(
            result
                .as_ref()
                .is_err_and(DatabaseError::is_unique_violation),
            "URL must be unique"
        );

        let result = new_entry(
            &mut db,
            "http://localhost/new_path",
            "/var/cache/file",
            None,
        )
        .await;

        assert!(
            result
                .as_ref()
                .is_err_and(DatabaseError::is_unique_violation),
            "cache path must be unique"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn test_increment_decrement_ref() {
        let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        increment_ref(&mut db, pk)
            .await
            .expect("increment reference counter of file");

        let entry = get_entry(&mut db, pk).await.unwrap();
        assert_eq!(entry.ref_count, 1);

        decrement_ref(&mut db, pk)
            .await
            .expect("decrement reference counter of file");

        let entry = get_entry(&mut db, pk).await.unwrap();
        assert_eq!(entry.ref_count, 0);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_remove_not_scheduled_entry_fails() {
        let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        let result = remove_entry(&mut db, pk).await;
        assert!(matches!(
            result,
            Err(DatabaseError::RemoveError(RemoveErrorReason::WrongStatus))
        ));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_remove_used_entry_fails() {
        let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        increment_ref(&mut db, pk).await.unwrap();

        let result = remove_entry(&mut db, pk).await;
        assert!(matches!(
            result,
            Err(DatabaseError::RemoveError(RemoveErrorReason::UsedFile))
        ));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_delete_unsafe() {
        let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        unsafe { delete_unsafe(&mut db, pk) }
            .await
            .expect("remove entry unsafely");

        let all = get_all(&mut db).await.unwrap();
        assert!(all.is_empty());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_remove_non_existing_entry_fails() {
        let (db_fixture, _) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        let invalid_pk = 123i32;
        let result = remove_entry(&mut db, invalid_pk).await;
        assert!(matches!(
            result,
            Err(DatabaseError::DieselError(DieselError::NotFound))
        ));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_remove_entry() {
        let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
            .await
            .unwrap();
        let mut db = db_fixture.db;

        update_status(&mut db, pk, FileStatus::ToRemove)
            .await
            .unwrap();

        remove_entry(&mut db, pk).await.expect("remove entry");
        let all = get_all(&mut db).await.unwrap();
        assert!(all.is_empty());
    }
}
