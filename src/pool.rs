//! Pool of Carol clients.
//!
//! This is based on [`deadpool::managed`].
//!
//! ## Example
//!
//! ```rust
//! # async fn test(database_url: &str, cache_dir: &str) {
//! use carol::pool::{Pool, PoolManager};
//!
//! // Build clients pool
//! let pool_manager = PoolManager::new(database_url, cache_dir);
//! let pool = Pool::builder(pool_manager).build().unwrap();
//!
//! // Get Carol client from pool
//! let mut client = pool.get().await.unwrap();
//!
//! // Use it as usually
//! let file = client.get("https://example.com").await.unwrap();
//! # }
//! ```

use std::path::{Path, PathBuf};

use deadpool::managed;

use crate::errors::Error;
use crate::Client;

/// Manager of pool of Carol clients.
pub struct PoolManager {
    cache_dir: PathBuf,
    database_url: String,
}

impl PoolManager {
    /// Create new manager.
    pub fn new<P>(database_url: &str, cache_dir: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            cache_dir: cache_dir.as_ref().to_path_buf(),
            database_url: database_url.to_string(),
        }
    }
}

impl managed::Manager for PoolManager {
    type Type = Client;
    type Error = Error;

    async fn create(&self) -> Result<Client, Error> {
        Client::init(&self.database_url, &self.cache_dir).await
    }

    async fn recycle(&self, _: &mut Client, _: &managed::Metrics) -> managed::RecycleResult<Error> {
        Ok(())
    }
}

/// Pool of Carol clients.
///
/// Alias for [`deadpool::managed::Pool`].
pub type Pool = managed::Pool<PoolManager>;

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_pool_build() {
        let tmp_database = TempDir::new("carol.test.database").unwrap();
        let database = tmp_database.path().join("carol.sqlite");
        let database_path = database.as_os_str().to_str().unwrap().to_string();
        let tmp_cache_dir = TempDir::new("carol.test.database").unwrap();

        let mgr = PoolManager::new(&database_path, tmp_cache_dir.path());
        let pool = Pool::builder(mgr).max_size(16).build().expect("build pool");

        let mut conn = pool.get().await.expect("get connection from pool");

        let file = conn
            .get("https://example.com")
            .await
            .expect("get example file");

        file.release().await.expect("release example file");
        conn.remove("https://example.com")
            .await
            .expect("remove example file");
    }
}
