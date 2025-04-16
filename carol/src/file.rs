//! Objects operated by storage.

use std::fmt;
use std::path::PathBuf;
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

impl From<FileSource> for String {
    fn from(value: FileSource) -> Self {
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
    pub fn time_to_live(&self) -> Option<TimeDelta> {
        match self.store_policy {
            StorePolicy::StoreForever => None,
            StorePolicy::ExpiresAfter { duration } => Some(Utc::now() - (self.created + duration)),
            StorePolicy::ExpiresAfterNotUsedFor { duration } => {
                Some(Utc::now() - (self.last_used + duration))
            }
        }
    }

    pub fn is_expired(&self) -> bool {
        self.time_to_live()
            .is_none_or(|delta| delta.num_seconds() <= 0)
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
