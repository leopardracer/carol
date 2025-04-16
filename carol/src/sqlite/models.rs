use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::serialize::{self, ToSql};
use diesel::sql_types::Integer;
use diesel::sqlite::{Sqlite, SqliteValue};
use diesel::{AsExpression, Insertable, Queryable, Selectable};

use super::error::{ConvertStorePolicyError, CreateNewFileError};
use super::schema;
use crate::file;

#[derive(Queryable, Selectable)]
#[diesel(table_name = schema::files)]
#[diesel(check_for_backend(Sqlite))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct File {
    pub id: i32,
    pub source: String,
    pub cache_path: String,
    pub filename: Option<String>,
    pub created: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    pub store_policy: StorePolicy,
    pub store_policy_data: Option<i32>,
    pub status: FileStatus,
}

#[derive(Insertable)]
#[diesel(table_name = schema::files)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewFile {
    pub source: String,
    pub cache_path: String,
    pub filename: Option<String>,
    pub created: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
    pub store_policy: StorePolicy,
    pub store_policy_data: Option<i32>,
    pub status: FileStatus,
}

impl TryFrom<file::FileMetadata> for NewFile {
    type Error = CreateNewFileError;

    fn try_from(metadata: file::FileMetadata) -> Result<Self, Self::Error> {
        let (store_policy, store_policy_data) = metadata.store_policy.try_into()?;
        Ok(Self {
            source: metadata.source.to_string(),
            cache_path: metadata
                .path
                .as_os_str()
                .to_str()
                .ok_or(crate::error::NonUtf8PathError)?
                .to_string(),
            filename: metadata.filename,
            created: metadata.created,
            last_used: metadata.last_used,
            store_policy,
            store_policy_data,
            status: file::FileStatus::default().into(),
        })
    }
}

impl TryFrom<File> for file::FileMetadata {
    type Error = ConvertStorePolicyError;

    fn try_from(file: File) -> Result<Self, Self::Error> {
        let store_policy = (file.store_policy, file.store_policy_data).try_into()?;
        Ok(file::FileMetadata {
            source: file::FileSource::parse(&file.source),
            filename: file.filename,
            path: PathBuf::from(file.cache_path),
            store_policy,
            created: file.created,
            last_used: file.last_used,
        })
    }
}

/// SQLite analugue for [`file::StorePolicy`], providing its serialization.
/// Paired with duration in seconds as `i32`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromSqlRow, AsExpression)]
#[diesel(sql_type = Integer)]
#[repr(i32)]
pub enum StorePolicy {
    StoreForever = 0,
    ExpiresAfter = 1,
    ExpiresAfterNotUsedFor = 2,
}

impl FromSql<Integer, Sqlite> for StorePolicy {
    fn from_sql(bytes: SqliteValue) -> deserialize::Result<Self> {
        match i32::from_sql(bytes)? {
            0 => Ok(Self::StoreForever),
            1 => Ok(Self::ExpiresAfter),
            2 => Ok(Self::ExpiresAfterNotUsedFor),
            _ => Err("unrecognized StorePolicy enum variant".into()),
        }
    }
}

impl ToSql<Integer, Sqlite> for StorePolicy {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Sqlite>) -> serialize::Result {
        match self {
            Self::StoreForever => <i32 as ToSql<Integer, Sqlite>>::to_sql(&0, out),
            Self::ExpiresAfter => <i32 as ToSql<Integer, Sqlite>>::to_sql(&1, out),
            Self::ExpiresAfterNotUsedFor => <i32 as ToSql<Integer, Sqlite>>::to_sql(&2, out),
        }
    }
}

impl TryFrom<file::StorePolicy> for (StorePolicy, Option<i32>) {
    type Error = ConvertStorePolicyError;

    fn try_from(value: file::StorePolicy) -> Result<Self, Self::Error> {
        Ok(match value {
            file::StorePolicy::StoreForever => (StorePolicy::StoreForever, None),
            file::StorePolicy::ExpiresAfter { duration } => (
                StorePolicy::ExpiresAfter,
                Some(duration.as_secs().try_into()?),
            ),
            file::StorePolicy::ExpiresAfterNotUsedFor { duration } => (
                StorePolicy::ExpiresAfterNotUsedFor,
                Some(duration.as_secs().try_into()?),
            ),
        })
    }
}

impl TryFrom<(StorePolicy, Option<i32>)> for file::StorePolicy {
    type Error = ConvertStorePolicyError;

    fn try_from(value: (StorePolicy, Option<i32>)) -> Result<Self, Self::Error> {
        use ConvertStorePolicyError::MissingPolicyData;
        let (policy, data) = value;
        Ok(match policy {
            StorePolicy::StoreForever => file::StorePolicy::StoreForever,
            StorePolicy::ExpiresAfter => file::StorePolicy::ExpiresAfter {
                duration: Duration::from_secs(data.ok_or(MissingPolicyData)?.try_into()?),
            },
            StorePolicy::ExpiresAfterNotUsedFor => file::StorePolicy::ExpiresAfterNotUsedFor {
                duration: Duration::from_secs(data.ok_or(MissingPolicyData)?.try_into()?),
            },
        })
    }
}

/// SQLite wrapper for [`FileStatus`], providing its serialization through integer conversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromSqlRow, AsExpression)]
#[diesel(sql_type = Integer)]
pub struct FileStatus(pub file::FileStatus);

impl ToSql<Integer, Sqlite> for FileStatus {
    fn to_sql<'b>(&'b self, out: &mut serialize::Output<'b, '_, Sqlite>) -> serialize::Result {
        match self.0 {
            file::FileStatus::Pending => <i32 as ToSql<Integer, Sqlite>>::to_sql(&0, out),
            file::FileStatus::Ready => <i32 as ToSql<Integer, Sqlite>>::to_sql(&1, out),
            file::FileStatus::ToRemove => <i32 as ToSql<Integer, Sqlite>>::to_sql(&2, out),
            file::FileStatus::Corrupted => <i32 as ToSql<Integer, Sqlite>>::to_sql(&3, out),
        }
    }
}

impl FromSql<Integer, Sqlite> for FileStatus
where
    i32: FromSql<Integer, Sqlite>,
{
    fn from_sql(bytes: SqliteValue) -> deserialize::Result<Self> {
        match i32::from_sql(bytes)? {
            0 => Ok(Self(file::FileStatus::Pending)),
            1 => Ok(Self(file::FileStatus::Ready)),
            2 => Ok(Self(file::FileStatus::ToRemove)),
            3 => Ok(Self(file::FileStatus::Corrupted)),
            _ => Err("Unrecognized enum variant".into()),
        }
    }
}

impl From<file::FileStatus> for FileStatus {
    fn from(value: file::FileStatus) -> Self {
        Self(value)
    }
}

impl From<FileStatus> for file::FileStatus {
    fn from(value: FileStatus) -> Self {
        value.0
    }
}

impl fmt::Display for FileStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
