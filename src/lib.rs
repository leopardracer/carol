//! # CAROL (Cache Roller)
//!
//! This library implements caching engine for downloading remote resources over HTTP(-S)
//! with the main goal to avoid copying of large files. It is fully asynchronous and
//! self-contained. This library is **not** a caching proxy.
//!
//! ## How it works
//!
//! The main idea of Carol is to store downloaded files in the filesystem, tracking and
//! managing their state. When the same URL will be requested next time, path to already
//! existing file will be returned.
//!
//! Carol consists of two parts:
//!
//! - **cache directory** in your filesystem, where downloaded files are stored
//! - **cache database** which keeps metadata about cached files and provides synchronisation
//!
//! To interact with Carol cache, use [`Client`]. Cache directory and database will be
//! initialized with the first client.
//!
//! To fetch multiple files asynchronously consider using [`pool::Pool`]. It is a wrapper
//! for [`deadpool`] which manages Carol clients.
//!
//! ## Reference counters
//!
//! Carol ensures that your file will never be removed from cache while used.
//! To track whether the file is used Carol stores *reference counter* for each file
//! in the database.
//!
//! When using Carol API each alive variable of type [`File`] "holds" a reference to that
//! cached file. While files reference counter is greater than 0, it cannot be removed with
//! [`Client::remove`]. In order to "release" the file and make it removable in the future,
//! use [`File::release`] or just simply drop your variable.
//!
//! **Warning:** when relying on automatic file release on drop you won't be able to handle the
//! errors which may occur during decrementing reference counter. As a result, your file may
//! be stuck in "used" state. So it is recommended to explicitly use [`File::release`].
//!
//! ## Example
//!
//! ```rust
//! # async fn test(cache_dir: &str, database: &str, url: &str, target: &str) {
//! use carol::Client;
//!
//! // Initialize client
//! let mut client = Client::init(database, cache_dir).await.unwrap();
//!
//! // Download file or just get it if it's already downloaded
//! let file = client.get(url).await.unwrap();
//!
//! // You can access downloaded file directly from cache directory
//! let full_path = file.cache_path();
//!
//! // Alternatively create symlink to downloaded file,
//! // so it can be accessed at a different path
//! file.symlink(target).await.unwrap();
//!
//! // use file however you need
//! // ...
//!
//! // "Free" file so it can be removed from cache later
//! drop(file);
//! # }
//! ```
//!
//! ## Cache expiration and eviction
//!
//! Carol ignroes HTTP caching policies deliberately. Instead it allows user to decide,
//! how long the file should be stored in cache.
//!
//! *Coming soon...*
//!

mod client;
mod database;
mod file;
mod garbage_collector;

pub mod errors;
pub mod pool;
pub mod maintenance;

pub use client::{Client, ClientBuilder};
pub use database::schema::FileStatus;
pub use file::File;
pub use garbage_collector::GarbageCollector;

// Re-exports of public API arguments from extern crates
#[doc(no_inline)]
pub use chrono::{DateTime, Utc};
#[doc(no_inline)]
pub use reqwest::Client as ReqwestClient;

/// Retry policy.
///
/// Defines whether to retry an action in case of failure and how to do it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RetryPolicy {
    /// Do not retry action.
    #[default]
    None,

    /// Retry action `number` times (after failure on the first attempt)
    /// sleeping for `period` between attempts.
    Fixed {
        number: usize,
        period: std::time::Duration,
    },
}
