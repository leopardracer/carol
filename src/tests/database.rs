//! Cache database wrapper unit tests.

/// Database fixtures. Helps in testing database-related code.
pub(crate) mod fixtures {
    use crate::database::CacheDatabase;
    use crate::errors::NonUtf8PathError;
    use crate::models::{CacheEntry, NewCacheEntry};
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
        pub db: CacheDatabase,
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
            let db = CacheDatabase::init(&db_path).await?;
            Ok(Self { tmp, db_path, db })
        }

        /// Create new temp database with given entry.
        /// Returns also primary key of that entry.
        pub async fn new_with_entry(entry: NewCacheEntry) -> Result<(Self, i32), Error> {
            let mut fixture = Self::new().await?;
            let pk = unsafe { fixture.db.insert_unsafe(entry).await? }.id;
            Ok((fixture, pk))
        }

        /// Create new temp database with default entry (from [`Self::default_entry`]).
        /// Returns also primary key of that entry.
        pub async fn new_with_default_entry() -> Result<(Self, i32), Error> {
            Self::new_with_entry(Self::default_new_entry()).await
        }
    }
}

use crate::database::CacheDatabase;
use crate::errors::{DatabaseError, DieselError, NonUtf8PathError, RemoveError};
use crate::{DateTime, FileStatus, Utc};
use fixtures::CacheDatabaseFixture;
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

    CacheDatabase::init(db_path_str)
        .await
        .expect("create new database");
}

#[tokio::test]
#[traced_test]
async fn test_connect_to_existing_database() {
    let db = CacheDatabaseFixture::new().await.unwrap();

    CacheDatabase::init(&db.db_path)
        .await
        .expect("connect to existing database");
}

#[tokio::test]
#[traced_test]
async fn test_new_entry() {
    let db_fixture = CacheDatabaseFixture::new().await.unwrap();
    let mut db = db_fixture.db;

    let entry = db
        .new_entry("http://localhost", "/var/cache/file", None)
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

    let entry = db.get_entry(pk).await.expect("get entry");
    assert_eq!(entry, CacheDatabaseFixture::default_entry());
}

#[tokio::test]
#[traced_test]
async fn test_get_entry_by_url() {
    let (db_fixture, _) = CacheDatabaseFixture::new_with_default_entry()
        .await
        .unwrap();
    let mut db = db_fixture.db;

    let entry = db
        .get_by_url("http://localhost")
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

    let entry = db
        .get_by_cache_path("/var/cache/file")
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

    let all = db.get_all().await.expect("get all entries");
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

    let all = db
        .filter_by_status(FileStatus::Pending)
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

    db.update_expires(pk, Some(DateTime::<Utc>::MAX_UTC))
        .await
        .expect("update expiration timestamp");

    let entry = db.get_entry(pk).await.unwrap();
    assert_eq!(entry.expires, Some(DateTime::<Utc>::MAX_UTC));
}

#[tokio::test]
#[traced_test]
async fn test_update_status() {
    let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
        .await
        .unwrap();
    let mut db = db_fixture.db;

    db.update_status(pk, FileStatus::Ready)
        .await
        .expect("update file status");

    let entry = db.get_entry(pk).await.unwrap();
    assert_eq!(entry.status, FileStatus::Ready);
}

#[tokio::test]
#[traced_test]
async fn test_new_entry_failure() {
    let (db_fixture, _) = CacheDatabaseFixture::new_with_default_entry()
        .await
        .unwrap();
    let mut db = db_fixture.db;

    let result = db
        .new_entry("http://localhost", "/var/cache/file2", None)
        .await;

    assert!(
        result
            .as_ref()
            .is_err_and(DatabaseError::is_unique_violation),
        "URL must be unique"
    );

    let result = db
        .new_entry("http://localhost/new_path", "/var/cache/file", None)
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

    db.increment_ref(pk)
        .await
        .expect("increment reference counter of file");

    let entry = db.get_entry(pk).await.unwrap();
    assert_eq!(entry.ref_count, 1);

    db.decrement_ref(pk)
        .await
        .expect("decrement reference counter of file");

    let entry = db.get_entry(pk).await.unwrap();
    assert_eq!(entry.ref_count, 0);
}

#[tokio::test]
#[traced_test]
async fn test_remove_not_scheduled_entry_fails() {
    let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
        .await
        .unwrap();
    let mut db = db_fixture.db;

    let result = db.remove_entry(pk).await;
    assert!(matches!(
        result,
        Err(DatabaseError::RemoveError(RemoveError::WrongStatus))
    ));
}

#[tokio::test]
#[traced_test]
async fn test_remove_used_entry_fails() {
    let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
        .await
        .unwrap();
    let mut db = db_fixture.db;

    db.increment_ref(pk).await.unwrap();

    let result = db.remove_entry(pk).await;
    assert!(matches!(
        result,
        Err(DatabaseError::RemoveError(RemoveError::UsedFile))
    ));
}

#[tokio::test]
#[traced_test]
async fn test_delete_unsafe() {
    let (db_fixture, pk) = CacheDatabaseFixture::new_with_default_entry()
        .await
        .unwrap();
    let mut db = db_fixture.db;

    unsafe { db.delete_unsafe(pk) }
        .await
        .expect("remove entry unsafely");

    let all = db.get_all().await.unwrap();
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
    let result = db.remove_entry(invalid_pk).await;
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

    db.update_status(pk, FileStatus::ToRemove).await.unwrap();

    db.remove_entry(pk).await.expect("remove entry");
    let all = db.get_all().await.unwrap();
    assert!(all.is_empty());
}
