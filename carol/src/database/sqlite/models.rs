use chrono::{DateTime, Utc};
use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::serialize::{self, ToSql};
use diesel::sql_types::{Integer, Text};
use diesel::sqlite::{Sqlite, SqliteValue};
use diesel::{AsExpression, Insertable, Queryable, Selectable};

use crate::database::schema;
use crate::{CachePolicy, FileStatus};

#[derive(Queryable, Selectable)]
#[diesel(table_name = schema::files)]
#[diesel(check_for_backend(Sqlite))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheEntry {
    pub id: i32,
    pub url: String,
    pub cache_path: String,
    pub created: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    pub cache_policy: CachePolicyModel,
    pub status: FileStatusModel,
    pub ref_count: i32,
}

#[derive(Insertable)]
#[diesel(table_name = schema::files)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewCacheEntry {
    pub url: String,
    pub cache_path: String,
    pub created: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    pub cache_policy: CachePolicyModel,
    pub status: FileStatusModel,
    pub ref_count: i32,
}

impl NewCacheEntry {
    pub fn new(url: &str, cache_path: &str, cache_policy: CachePolicyModel) -> Self {
        let now = Utc::now();
        Self {
            url: url.to_string(),
            cache_path: cache_path.to_string(),
            created: now,
            last_used: now,
            status: FileStatus::default().into(),
            ref_count: 0,
            cache_policy,
        }
    }
}

/// SQLite wrapper for [`CachePolicy`], providing its serialization through [`serde_json`].
#[derive(Debug, Clone, PartialEq, Eq, FromSqlRow, AsExpression)]
#[diesel(sql_type = Text)]
pub struct CachePolicyModel(pub CachePolicy);

impl From<CachePolicy> for CachePolicyModel {
    fn from(inner: CachePolicy) -> Self {
        Self(inner)
    }
}

impl From<CachePolicyModel> for CachePolicy {
    fn from(value: CachePolicyModel) -> Self {
        value.0
    }
}

impl FromSql<Text, Sqlite> for CachePolicyModel {
    fn from_sql(mut bytes: SqliteValue) -> deserialize::Result<Self> {
        Ok(Self(serde_json::from_str(bytes.read_text())?))
    }
}

impl ToSql<Text, Sqlite> for CachePolicyModel {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Sqlite>) -> serialize::Result {
        out.set_value(serde_json::to_string(&self.0)?);
        Ok(serialize::IsNull::No)
    }
}

/// SQLite wrapper for [`FileStatus`], providing its serialization through integer conversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromSqlRow, AsExpression)]
#[diesel(sql_type = Integer)]
pub struct FileStatusModel(pub FileStatus);

impl ToSql<Integer, Sqlite> for FileStatusModel {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Sqlite>) -> serialize::Result {
        match self.0 {
            FileStatus::Pending => <i32 as ToSql<Integer, Sqlite>>::to_sql(&0, out),
            FileStatus::Ready => <i32 as ToSql<Integer, Sqlite>>::to_sql(&1, out),
            FileStatus::ToRemove => <i32 as ToSql<Integer, Sqlite>>::to_sql(&2, out),
            FileStatus::Corrupted => <i32 as ToSql<Integer, Sqlite>>::to_sql(&3, out),
        }
    }
}

impl FromSql<Integer, Sqlite> for FileStatusModel
where
    i32: FromSql<Integer, Sqlite>,
{
    fn from_sql(bytes: SqliteValue) -> deserialize::Result<Self> {
        match i32::from_sql(bytes)? {
            0 => Ok(Self(FileStatus::Pending)),
            1 => Ok(Self(FileStatus::Ready)),
            2 => Ok(Self(FileStatus::ToRemove)),
            3 => Ok(Self(FileStatus::Corrupted)),
            _ => Err("Unrecognized enum variant".into()),
        }
    }
}

impl From<FileStatus> for FileStatusModel {
    fn from(value: FileStatus) -> Self {
        Self(value)
    }
}

impl From<FileStatusModel> for FileStatus {
    fn from(value: FileStatusModel) -> Self {
        value.0
    }
}

impl PartialEq<FileStatus> for FileStatusModel {
    fn eq(&self, other: &FileStatus) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<FileStatusModel> for FileStatus {
    fn eq(&self, other: &FileStatusModel) -> bool {
        self.eq(&other.0)
    }
}
