//! Objects operated by storage.

use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::{DateTime, Url, Utc};

use super::database::StorageDatabase;

/// File identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileId(u32);

impl From<u32> for FileId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<FileId> for u32 {
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
    /// When this policy is used, [`File::last_used`][crate::file::File::last_used]
    /// timestamp is used to define if the file is stale.
    ExpiresAfterNotUsedFor { duration: Duration },
}

/// Stored file metadata.
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

/// Stored file.
///
/// Generic argument `D` defines which database is used to manage this file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File<D: StorageDatabase> {
    /// URI of the database used to manage this file.
    pub database: D,

    /// Unique file identifier.
    pub id: FileId,

    /// Metadata of stored file.
    pub metadata: FileMetadata,
}

impl<D: StorageDatabase> File<D> {
    pub fn time_to_live(&self, _now: SystemTime) -> Duration {
        todo!()
    }
}
