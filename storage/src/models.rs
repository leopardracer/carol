//! Objects operated by storage.

use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::{DateTime, Url, Utc};

use super::database::StorageDatabase;
use super::store_policy::StorePolicy;

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

impl fmt::Display for FileSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url(url) => url.fmt(f),
            Self::Custom(string) => string.fmt(f),
        }
    }
}

/// Stored file.
///
/// Generic argument `D` defines which database is used to manage this file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct File<D: StorageDatabase> {
    /// URI of the database used to manage this file.
    pub database_uri: D::Uri,

    /// Unique file identifier.
    pub id: FileId,

    /// File source.
    pub source: Option<FileSource>,

    /// Path to stored file.
    pub path: PathBuf,

    /// Creation timestamp.
    pub created: DateTime<Utc>,

    /// Last used timestamp.
    pub last_used: DateTime<Utc>,

    /// Store policy of the file.
    ///
    /// Describes when the file will be considered "stale".
    pub store_policy: StorePolicy,
}

impl<D: StorageDatabase> File<D> {
    pub fn time_to_live(&self, _now: SystemTime) -> Duration {
        todo!()
    }
}
