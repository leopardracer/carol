use std::fmt;

use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::Integer;
use diesel::sqlite::{Sqlite, SqliteValue};

diesel::table! {
    use diesel::sql_types::*;
    use super::FileStatus;

    /// Cached files metadata.
    files (id) {
        /// Primary key.
        id -> Integer,

        // Manually added UNIQUE to up.sql, because diesel can't do that
        /// URL of the downloaded file.
        url -> VarChar,

        // Manually added UNIQUE to up.sql, because diesel can't do that
        /// Path to downloaded file in cache.
        cache_path -> VarChar,

        /// Entry creation timestamp.
        created -> TimestamptzSqlite,

        /// Expiration time.
        expires -> Nullable<TimestamptzSqlite>,

        /// Current status of the cache entry.
        status -> Integer,

        /// Reference counter of the cache entry.
        ref_count -> Integer,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, FromSqlRow, AsExpression)]
#[diesel(sql_type = Integer)]
#[repr(i32)]
pub enum FileStatus {
    Pending = 0,
    Ready = 1,
    ToRemove = 2,
    Corrupted = 3,
}

impl Default for FileStatus {
    fn default() -> Self {
        Self::Pending
    }
}

impl fmt::Display for FileStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                Self::Ready => "Ready",
                Self::Pending => "Pending",
                Self::ToRemove => "ToRemove",
                Self::Corrupted => "Corrupted",
            }
        )
    }
}

impl ToSql<Integer, Sqlite> for FileStatus {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Sqlite>) -> serialize::Result {
        match *self {
            Self::Pending => <i32 as ToSql<Integer, Sqlite>>::to_sql(&0, out),
            Self::Ready => <i32 as ToSql<Integer, Sqlite>>::to_sql(&1, out),
            Self::ToRemove => <i32 as ToSql<Integer, Sqlite>>::to_sql(&2, out),
            Self::Corrupted => <i32 as ToSql<Integer, Sqlite>>::to_sql(&3, out),
        }
    }
}

impl FromSql<Integer, Sqlite> for FileStatus
where
    i32: FromSql<Integer, Sqlite>,
{
    fn from_sql(bytes: SqliteValue) -> deserialize::Result<Self> {
        match i32::from_sql(bytes)? {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Ready),
            2 => Ok(Self::ToRemove),
            3 => Ok(Self::Corrupted),
            _ => Err("Unrecognized enum variant".into()),
        }
    }
}
