//! # CAROL (Cache Roller)
//!
//! ## Example
//!
//! ```rust
//! # async fn test(cache_dir: &str, database: &str, url: &str, target: &str) {
//! // Create client
//! let mut client = carol::Client::init(database, cache_dir).await.unwrap();
//!
//! // Download file or just get it if it's already downloaded
//! let file = client.get(url).await.unwrap();
//!
//! // Create symlink to downloaded file, so it can be accessed at different FS path
//! file.symlink(target).await.unwrap();
//!
//! // use content from `target` symlink
//! // ...
//! # }
//! ```

mod client;
mod database;
mod file;
mod garbage_collector;
mod maintenance;

pub mod errors;
pub mod pool;

pub use client::Client;
pub use database::schema::FileStatus;
pub use file::File;
pub use garbage_collector::GarbageCollector;
pub use maintenance::MaintenanceRunner;

// Re-exports of public API arguments from extern crates
#[doc(no_inline)]
pub use chrono::{DateTime, Utc};
#[doc(no_inline)]
pub use tokio::time::Duration;

/// Retry policy.
///
/// Defines whether to retry an action in case of failure and how to do it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryPolicy {
    /// Do not retry action.
    None,

    /// Retry action `number` times (after failure on the first attempt)
    /// sleeping for `period` between attempts.
    Fixed { number: usize, period: Duration },
}
