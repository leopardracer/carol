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
//! // Increment files reference counter,
//! // so it won't be garbage-collected while you working with it
//! file.lock(&mut client).await;
//!
//! // Create symlink to downloaded file, so it can be accessed at different FS path
//! file.symlink(target).await.unwrap();
//!
//! // use content from `target` symlink
//! // ...
//!
//! // Release file
//! file.release(&mut client).await;
//! # }
//! ```

mod client;
mod database;
mod file;
mod garbage_collector;
mod maintenance;

pub mod errors;

pub use client::Client;
pub use file::File;
pub use garbage_collector::GarbageCollector;
pub use maintenance::MaintenanceRunner;
pub use database::schema::FileStatus;

// Re-exports of public API arguments from extern crates
#[doc(no_inline)]
pub use chrono::{DateTime, Utc};
#[doc(no_inline)]
pub use tokio::time::Duration;
