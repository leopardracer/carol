use crate::schema::FileStatus;
use chrono::{DateTime, Utc};
use diesel::{Insertable, Queryable, Selectable};

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::files)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    pub id: i32,
    pub url: String,
    pub cache_path: String,
    pub created: DateTime<Utc>,
    pub expires: Option<DateTime<Utc>>,
    pub status: FileStatus,
    pub ref_count: i32,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::files)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewCacheEntry {
    pub url: String,
    pub cache_path: String,
    pub created: DateTime<Utc>,
    pub expires: Option<DateTime<Utc>>,
    pub status: FileStatus,
    pub ref_count: i32,
}

impl NewCacheEntry {
    pub fn new(url: &str, cache_path: &str, expires: Option<DateTime<Utc>>) -> Self {
        Self {
            url: url.to_string(),
            cache_path: cache_path.to_string(),
            created: Utc::now(),
            expires,
            status: FileStatus::default(),
            ref_count: 0,
        }
    }
}
