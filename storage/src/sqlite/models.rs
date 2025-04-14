use chrono::{DateTime, Utc};
use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::serialize::{self, ToSql};
use diesel::sql_types::{Integer, Text};
use diesel::sqlite::{Sqlite, SqliteValue};
use diesel::{AsExpression, Insertable, Queryable, Selectable};

use super::schema;
use crate::database;
use crate::file;

#[derive(Queryable, Selectable)]
#[diesel(table_name = schema::files)]
#[diesel(check_for_backend(Sqlite))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct File {
    pub id: i32,
    pub url: String,
    pub cache_path: String,
    pub filename: Option<String>,
    pub created: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    pub cache_policy: StorePolicy,
    pub status: FileStatus,
    pub ref_count: i32,
}

#[derive(Insertable)]
#[diesel(table_name = schema::files)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewFile {
    pub url: String,
    pub cache_path: String,
    pub filename: Option<String>,
    pub created: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    pub cache_policy: StorePolicy,
    pub status: FileStatus,
    pub ref_count: i32,
}

/// SQLite wrapper for [`file::StorePolicy`], providing its serialization through [`serde_json`].
#[derive(Debug, Clone, PartialEq, Eq, FromSqlRow, AsExpression)]
#[diesel(sql_type = Text)]
pub struct StorePolicy(pub file::StorePolicy);

impl From<file::StorePolicy> for StorePolicy {
    fn from(inner: file::StorePolicy) -> Self {
        Self(inner)
    }
}

impl From<StorePolicy> for file::StorePolicy {
    fn from(value: StorePolicy) -> Self {
        value.0
    }
}

impl FromSql<Text, Sqlite> for StorePolicy {
    fn from_sql(mut bytes: SqliteValue) -> deserialize::Result<Self> {
        Ok(Self(serde_json::from_str(bytes.read_text())?))
    }
}

impl ToSql<Text, Sqlite> for StorePolicy {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Sqlite>) -> serialize::Result {
        out.set_value(serde_json::to_string(&self.0)?);
        Ok(serialize::IsNull::No)
    }
}

/// SQLite wrapper for [`FileStatus`], providing its serialization through integer conversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromSqlRow, AsExpression)]
#[diesel(sql_type = Integer)]
pub struct FileStatus(pub database::FileStatus);

impl ToSql<Integer, Sqlite> for FileStatus {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Sqlite>) -> serialize::Result {
        match self.0 {
            database::FileStatus::Pending => <i32 as ToSql<Integer, Sqlite>>::to_sql(&0, out),
            database::FileStatus::Ready => <i32 as ToSql<Integer, Sqlite>>::to_sql(&1, out),
            database::FileStatus::ToRemove => <i32 as ToSql<Integer, Sqlite>>::to_sql(&2, out),
            database::FileStatus::Corrupted => <i32 as ToSql<Integer, Sqlite>>::to_sql(&3, out),
        }
    }
}

impl FromSql<Integer, Sqlite> for FileStatus
where
    i32: FromSql<Integer, Sqlite>,
{
    fn from_sql(bytes: SqliteValue) -> deserialize::Result<Self> {
        match i32::from_sql(bytes)? {
            0 => Ok(Self(database::FileStatus::Pending)),
            1 => Ok(Self(database::FileStatus::Ready)),
            2 => Ok(Self(database::FileStatus::ToRemove)),
            3 => Ok(Self(database::FileStatus::Corrupted)),
            _ => Err("Unrecognized enum variant".into()),
        }
    }
}

impl From<database::FileStatus> for FileStatus {
    fn from(value: database::FileStatus) -> Self {
        Self(value)
    }
}

impl From<FileStatus> for database::FileStatus {
    fn from(value: FileStatus) -> Self {
        value.0
    }
}

impl PartialEq<database::FileStatus> for FileStatus {
    fn eq(&self, other: &database::FileStatus) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<FileStatus> for database::FileStatus {
    fn eq(&self, other: &FileStatus) -> bool {
        self.eq(&other.0)
    }
}
