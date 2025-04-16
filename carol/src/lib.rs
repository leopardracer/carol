//! Carol files storage.

mod database;
mod file;
mod storage_manager;

// Public modules
pub mod error;
pub mod sqlite;

// Public re-exports
pub use database::{StorageDatabase, StorageDatabaseError, StorageDatabaseExt};
pub use file::{File, FileId, FileMetadata, FileSource, FileStatus, StorePolicy};
pub use storage_manager::StorageManager;

// Re-exports of extern crates containing types referenced in public API
#[doc(no_inline)]
pub use chrono;
#[doc(no_inline)]
pub use url;
