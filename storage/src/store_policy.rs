use std::time::Duration;

use serde::{Deserialize, Serialize};

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
    Forever,

    /// File is considered stale after a certain time period after creation.
    ExpiresAfter { duration: Duration },

    /// File is considered stale after not being used for a certain period of time.
    ///
    /// When this policy is used, [`File::last_used`][super::models::File::last_used]
    /// timestamp is used to define if the file is stale.
    ExpiresAfterNotUsedFor { duration: Duration },
}
