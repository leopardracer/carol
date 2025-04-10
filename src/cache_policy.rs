use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{DateTime, Utc};

/// Describes when cached file is considered stale.
///
/// Default cache policy means that file will never be stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CachePolicy {
    /// File will never be stale.
    #[default]
    None,

    /// File will be stale after a certain timestamp.
    ExpiresAt { timestamp: DateTime<Utc> },

    /// File will be stale after a certain time period after creation.
    ExpiresAfter { duration: Duration },

    /// File will be stale after now being used for a certaion period of time.
    ExpiresAfterNotUsedFor { duration: Duration },
}

impl CachePolicy {
    pub fn at(timestamp: DateTime<Utc>) -> Self {
        Self::ExpiresAt { timestamp }
    }

    pub fn after(duration: Duration) -> Self {
        Self::ExpiresAfter { duration }
    }

    pub fn not_used_for(duration: Duration) -> Self {
        Self::ExpiresAfterNotUsedFor { duration }
    }
}

impl std::fmt::Display for CachePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                Self::None => "never expires".to_string(),
                Self::ExpiresAt { timestamp } => format!("expires at {}", timestamp),
                Self::ExpiresAfter { duration } => format!(
                    "expires after {} seconds after creation",
                    duration.as_secs()
                ),
                Self::ExpiresAfterNotUsedFor { duration } =>
                    format!("expires after not used for {} seconds", duration.as_secs()),
            }
        )
    }
}
