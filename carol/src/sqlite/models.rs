use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::deserialize::FromSqlRow;
use diesel::sql_types::Integer;
use diesel::sqlite::Sqlite;
use diesel::{AsExpression, Insertable, Queryable, Selectable};
use diesel_enum::DbEnum;

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

/// SQLite mirror type for [`file::StorePolicy`] (when paired with duration in seconds as `i32`),
/// providing its serialization through integer conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromSqlRow, AsExpression, DbEnum)]
#[diesel(sql_type = Integer)]
#[diesel_enum(error_fn = ConvertStorePolicyError::bad_enum_variant)]
#[diesel_enum(error_type = ConvertStorePolicyError)]
#[repr(i32)]
pub enum StorePolicy {
    StoreForever = 0,
    ExpiresAfter = 1,
    ExpiresAfterNotUsedFor = 2,
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

/// SQLite mirror type for [`file::FileStatus`],
/// providing its serialization through integer conversion.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, FromSqlRow, AsExpression, DbEnum)]
#[diesel_enum(error_fn = ConvertStorePolicyError::bad_enum_variant)]
#[diesel_enum(error_type = ConvertStorePolicyError)]
#[diesel(sql_type = Integer)]
#[repr(i32)]
pub enum FileStatus {
    /// File is not yet fully moved into storage.
    #[default]
    Pending = 0,

    /// File is ready to be used.
    Ready = 1,

    /// File is scheduled for removal.
    ToRemove = 2,

    /// File is corrupted. This means that something is wrong with the file
    /// or the cache entry.
    Corrupted = 3,
}

impl From<file::FileStatus> for FileStatus {
    fn from(value: file::FileStatus) -> Self {
        match value {
            file::FileStatus::Pending => Self::Pending,
            file::FileStatus::Ready => Self::Ready,
            file::FileStatus::ToRemove => Self::ToRemove,
            file::FileStatus::Corrupted => Self::Corrupted,
        }
    }
}

impl From<FileStatus> for file::FileStatus {
    fn from(value: FileStatus) -> Self {
        match value {
            FileStatus::Pending => file::FileStatus::Pending,
            FileStatus::Ready => file::FileStatus::Ready,
            FileStatus::ToRemove => file::FileStatus::ToRemove,
            FileStatus::Corrupted => file::FileStatus::Corrupted,
        }
    }
}

impl fmt::Display for FileStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        file::FileStatus::from(*self).fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    #[rstest]
    #[case::valid_metadata(
        file::FileMetadata {
            source: file::FileSource::Custom("somesource".to_string()),
            filename: Some("somename".to_string()),
            path: PathBuf::from("/some/path"),
            store_policy: file::StorePolicy::StoreForever,
            created: DateTime::<Utc>::MAX_UTC,
            last_used: DateTime::<Utc>::MAX_UTC,
        },
        "/some/path".to_string(),
        "somesource".to_string(),
        StorePolicy::StoreForever,
        None,
    )]
    #[cfg_attr(
        unix,
        should_panic(
            expected = "convert FileMetadata into NewFile: NonUtf8PathError(NonUtf8PathError)"
        )
    )]
    #[cfg_attr(
        unix,
        case::non_utf8_path(
            file::FileMetadata {
                source: file::FileSource::Custom("somesource".to_string()),
                filename: Some("somename".to_string()),
                path: PathBuf::from(OsString::from_vec(vec![255])),
                store_policy: file::StorePolicy::StoreForever,
                created: DateTime::<Utc>::MAX_UTC,
                last_used: DateTime::<Utc>::MAX_UTC,
            },
            "".to_string(), // there is no valid value, conversion will panic
            "somesource".to_string(),
            StorePolicy::StoreForever,
            None,
        )
    )]
    #[trace]
    fn test_file_metadata_to_new_file_conversion(
        #[case] metadata: file::FileMetadata,
        #[case] expected_path: String,
        #[case] expected_source: String,
        #[case] expected_policy: StorePolicy,
        #[case] expected_policy_data: Option<i32>,
    ) {
        let new_file =
            NewFile::try_from(metadata.clone()).expect("convert FileMetadata into NewFile");
        assert_eq!(new_file.cache_path, expected_path);
        assert_eq!(new_file.created, metadata.created);
        assert_eq!(new_file.filename, metadata.filename);
        assert_eq!(new_file.last_used, metadata.last_used);
        assert_eq!(new_file.source, expected_source);
        assert_eq!(new_file.status, FileStatus::default());
        assert_eq!(new_file.store_policy, expected_policy);
        assert_eq!(new_file.store_policy_data, expected_policy_data);
    }

    #[rstest]
    #[case::valid_file(
        File {
            id: 1,
            source: "http://localhost:8080/file.txt".to_string(),
            cache_path: "/some/path".to_string(),
            filename: Some("file.txt".to_string()),
            created: DateTime::<Utc>::MAX_UTC,
            last_used: DateTime::<Utc>::MAX_UTC,
            store_policy: StorePolicy::StoreForever,
            store_policy_data: None,
            status: FileStatus::Pending,
        },
        PathBuf::from("/some/path"),
        file::FileSource::Url(url::Url::parse("http://localhost:8080/file.txt").unwrap()),
        file::StorePolicy::StoreForever,
    )]
    #[trace]
    fn test_file_to_file_metadata_conversion(
        #[case] file: File,
        #[case] expected_path: PathBuf,
        #[case] expected_source: file::FileSource,
        #[case] expected_policy: file::StorePolicy,
    ) {
        let metadata =
            file::FileMetadata::try_from(file.clone()).expect("convert File into FileMetadata");
        assert_eq!(metadata.path, expected_path);
        assert_eq!(metadata.created, file.created);
        assert_eq!(metadata.filename, file.filename);
        assert_eq!(metadata.last_used, file.last_used);
        assert_eq!(metadata.source, expected_source);
        assert_eq!(metadata.store_policy, expected_policy);
    }

    #[rstest]
    #[case(file::StorePolicy::StoreForever, StorePolicy::StoreForever, None)]
    #[case(file::StorePolicy::ExpiresAfter { duration: Duration::from_secs(42) }, StorePolicy::ExpiresAfter, Some(42))]
    #[should_panic(expected = "serialize StorePolicy: TryFromIntError")]
    #[case(file::StorePolicy::ExpiresAfter { duration: Duration::from_secs(i32::MAX as u64 + 1) }, StorePolicy::ExpiresAfter, Some(42))]
    #[should_panic(expected = "serialize StorePolicy: TryFromIntError")]
    #[case(file::StorePolicy::ExpiresAfterNotUsedFor { duration: Duration::from_secs(i32::MAX as u64 + 1) }, StorePolicy::ExpiresAfter, Some(42))]
    #[trace]
    fn test_store_policy_serialization(
        #[case] input: file::StorePolicy,
        #[case] expected_policy: StorePolicy,
        #[case] expected_data: Option<i32>,
    ) {
        let (policy, data) = input.try_into().expect("serialize StorePolicy");
        assert_eq!(policy, expected_policy);
        assert_eq!(data, expected_data);
    }

    #[rstest]
    #[case(StorePolicy::StoreForever, None, file::StorePolicy::StoreForever)]
    #[case(StorePolicy::StoreForever, Some(42), file::StorePolicy::StoreForever)]
    #[should_panic(expected = "deserialize StorePolicy: MissingPolicyData")]
    #[case::missing_policy_data(
        StorePolicy::ExpiresAfter,
        None,
        file::StorePolicy::ExpiresAfter {
            duration: Duration::from_secs(42), // there is no valid value, conversion will panic
        },
    )]
    #[should_panic(expected = "deserialize StorePolicy: MissingPolicyData")]
    #[case::missing_policy_data(
        StorePolicy::ExpiresAfterNotUsedFor,
        None,
        file::StorePolicy::ExpiresAfterNotUsedFor {
            duration: Duration::from_secs(42), // there is no valid value, conversion will panic
        },
    )]
    #[should_panic(expected = "deserialize StorePolicy: TryFromIntError(TryFromIntError(()))")]
    #[case::negative_policy_data(
        StorePolicy::ExpiresAfterNotUsedFor,
        Some(-42),
        file::StorePolicy::ExpiresAfterNotUsedFor {
            duration: Duration::from_secs(42), // there is no valid value, conversion will panic
        },
    )]
    #[trace]
    fn test_store_policy_deserialization(
        #[case] policy: StorePolicy,
        #[case] data: Option<i32>,
        #[case] expected_policy: file::StorePolicy,
    ) {
        let result = file::StorePolicy::try_from((policy, data)).expect("deserialize StorePolicy");
        assert_eq!(result, expected_policy);
    }
}
