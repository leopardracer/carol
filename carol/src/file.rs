//! Objects operated by storage.

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

/// File identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileId(i32);

impl From<i32> for FileId {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl From<FileId> for i32 {
    fn from(value: FileId) -> Self {
        value.0
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// File source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileSource {
    /// File was fetched from given URL.
    Url(Url),

    /// Custom source.
    Custom(String),
}

impl FileSource {
    /// Try parsing input string as URL and return [`FileSource::Url`] if succeeded.
    /// Otherwise return [`FileSource::Custom`].
    pub fn parse(input: &str) -> Self {
        match Url::parse(input) {
            Ok(url) => Self::Url(url),
            Err(_) => Self::Custom(input.to_string()),
        }
    }

    /// Return as string slice.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Url(url) => url.as_str(),
            Self::Custom(s) => s.as_str(),
        }
    }
}

impl AsRef<str> for FileSource {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<Url> for FileSource {
    fn from(url: Url) -> Self {
        Self::Url(url)
    }
}

impl FromStr for FileSource {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::parse(s))
    }
}

impl From<FileSource> for String {
    fn from(value: FileSource) -> String {
        match value {
            FileSource::Url(url) => url.into(),
            FileSource::Custom(s) => s,
        }
    }
}

impl sha256::Sha256Digest for FileSource {
    fn digest(self) -> String {
        String::from(self).digest()
    }
}

impl sha256::Sha256Digest for &FileSource {
    fn digest(self) -> String {
        self.as_str().digest()
    }
}

impl fmt::Display for FileSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url(url) => url.fmt(f),
            Self::Custom(string) => string.fmt(f),
        }
    }
}

/// Status of stored file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum FileStatus {
    /// File is not yet fully moved into storage.
    #[default]
    Pending,

    /// File is ready to be used.
    Ready,

    /// File is scheduled for removal.
    ToRemove,

    /// File is corrupted. This means that something is wrong with the file
    /// or the cache entry.
    Corrupted,
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

/// Describes when stored file will be considered "stale".
///
/// File can be stored only for a certain period of time.
/// When policy restrictions are met, the file will be marked as "stale".
/// It means that it no longer should be used. User may remove or update it.
///
/// If maintenance server is running, it may remove "stale" files automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum StorePolicy {
    /// File will never be removed. Default policy.
    #[default]
    StoreForever,

    /// File is considered stale after a certain time period after creation.
    ExpiresAfter { duration: Duration },

    /// File is considered stale after not being used for a certain period of time.
    ///
    /// When this policy is used, [`FileMetadata::last_used`] timestamp is used to define
    /// if the file is stale.
    ExpiresAfterNotUsedFor { duration: Duration },
}

/// File metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMetadata {
    /// File source.
    pub source: FileSource,

    /// Original file name.
    pub filename: Option<String>,

    /// Path to stored file.
    pub path: PathBuf,

    /// Store policy of the file.
    ///
    /// Describes when the file will be considered "stale".
    pub store_policy: StorePolicy,

    /// Creation timestamp.
    pub created: DateTime<Utc>,

    /// Last used timestamp.
    pub last_used: DateTime<Utc>,
}

impl FileMetadata {
    pub fn time_to_live(&self, now: DateTime<Utc>) -> Option<TimeDelta> {
        match self.store_policy {
            StorePolicy::StoreForever => None,
            StorePolicy::ExpiresAfter { duration } => Some(self.created + duration - now),
            StorePolicy::ExpiresAfterNotUsedFor { duration } => {
                Some(self.last_used + duration - now)
            }
        }
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.time_to_live(now)
            .is_some_and(|delta| delta.num_seconds() <= 0)
    }
}

/// Stored file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    /// URI of the database used to manage this file.
    pub database: String,

    /// Unique file identifier.
    pub id: FileId,

    /// Status of stored file.
    pub status: FileStatus,

    /// Metadata of stored file.
    pub metadata: FileMetadata,
}

#[cfg(test)]
mod tests {
    use super::{FileMetadata, FileSource, StorePolicy};
    use chrono::{DateTime, TimeDelta, Utc};
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use std::time::Duration;

    #[rstest]
    #[trace]
    fn test_file_source_urls(
        #[values(
            "https://localhost:8080/file.txt",
            "https://example.com",
            "https://example.com/",
            "file://path/to/file",
            "schema://host.com/file?param=value"
        )]
        input: &str,
    ) {
        let source = FileSource::parse(input.as_ref());
        assert!(matches!(source, FileSource::Url(..)));
    }

    #[rstest]
    #[trace]
    fn test_file_source_custom(#[values("some.source", "/path/to/local", "")] input: &str) {
        let source = FileSource::parse(input.as_ref());
        assert!(matches!(source, FileSource::Custom(..)));
    }

    #[rstest]
    #[case("https://example.com/", "https://example.com/")]
    #[case("https://example.com", "https://example.com/")]
    #[case("https://example.com/file", "https://example.com/file")]
    #[trace]
    fn test_file_source_as_str(#[case] input: &str, #[case] expected: &str) {
        let source = FileSource::parse(input.as_ref());
        assert_eq!(source.as_str(), expected);
    }

    #[rstest]
    #[case(
        "https://example.com/",
        "0f115db062b7c0dd030b16878c99dea5c354b49dc37b38eb8846179c7783e9d7"
    )]
    #[case(
        "https://example.com",
        "0f115db062b7c0dd030b16878c99dea5c354b49dc37b38eb8846179c7783e9d7"
    )]
    #[case("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")]
    #[trace]
    fn test_file_source_digest(#[case] input: FileSource, #[case] expected: &str) {
        let digest = sha256::digest(&input);
        assert_eq!(digest, expected);
    }

    #[fixture]
    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    #[rstest]
    #[case(
        DateTime::<Utc>::MIN_UTC,
        DateTime::<Utc>::MIN_UTC,
        StorePolicy::StoreForever,
        None,
    )]
    #[case(
        now,
        DateTime::<Utc>::MIN_UTC,
        StorePolicy::ExpiresAfter { duration: Duration::from_secs(1) },
        Some(TimeDelta::seconds(1)),
    )]
    #[case(
        now - TimeDelta::seconds(2),
        DateTime::<Utc>::MIN_UTC,
        StorePolicy::ExpiresAfter { duration: Duration::from_secs(1) },
        Some(TimeDelta::seconds(-1)),
    )]
    #[case(
        DateTime::<Utc>::MIN_UTC,
        now,
        StorePolicy::ExpiresAfterNotUsedFor { duration: Duration::from_secs(1) },
        Some(TimeDelta::seconds(1)),
    )]
    #[case(
        DateTime::<Utc>::MIN_UTC,
        now - TimeDelta::seconds(2),
        StorePolicy::ExpiresAfterNotUsedFor { duration: Duration::from_secs(1) },
        Some(TimeDelta::seconds(-1)),
    )]
    #[trace]
    fn test_file_time_to_live(
        now: DateTime<Utc>,
        #[case] created: DateTime<Utc>,
        #[case] last_used: DateTime<Utc>,
        #[case] store_policy: StorePolicy,
        #[case] expected: Option<TimeDelta>,
    ) {
        let file = FileMetadata {
            source: FileSource::Custom("".to_string()),
            filename: None,
            path: PathBuf::from(""),
            store_policy,
            created,
            last_used,
        };
        let ttl = file.time_to_live(now);
        assert_eq!(ttl, expected);
    }

    #[rstest]
    #[case( // StoreForever never expires
        DateTime::<Utc>::MIN_UTC,
        DateTime::<Utc>::MIN_UTC,
        StorePolicy::StoreForever,
        false,
    )]
    #[case( // created now and will expire in 1 sec
        now,
        DateTime::<Utc>::MIN_UTC,
        StorePolicy::ExpiresAfter { duration: Duration::from_secs(1) },
        false,
    )]
    #[case( // created 2 secs ago and lived for 1 sec after that
        now - TimeDelta::seconds(2),
        DateTime::<Utc>::MIN_UTC,
        StorePolicy::ExpiresAfter { duration: Duration::from_secs(1) },
        true,
    )]
    #[case( // last used now and will expire in 1 sec
        DateTime::<Utc>::MIN_UTC,
        now,
        StorePolicy::ExpiresAfterNotUsedFor { duration: Duration::from_secs(1) },
        false,
    )]
    #[case( // last used 2 secs ago and lived for 1 sec after that
        DateTime::<Utc>::MIN_UTC,
        now - TimeDelta::seconds(2),
        StorePolicy::ExpiresAfterNotUsedFor { duration: Duration::from_secs(1) },
        true,
    )]
    #[trace]
    fn test_file_is_expired(
        now: DateTime<Utc>,
        #[case] created: DateTime<Utc>,
        #[case] last_used: DateTime<Utc>,
        #[case] store_policy: StorePolicy,
        #[case] expected: bool,
    ) {
        let file = FileMetadata {
            source: FileSource::Custom("".to_string()),
            filename: None,
            path: PathBuf::from(""),
            store_policy,
            created,
            last_used,
        };
        let expired = file.is_expired(now);
        assert_eq!(expired, expected);
    }
}
