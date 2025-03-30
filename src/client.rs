use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time;
use tokio_stream::StreamExt;
use tracing::{debug, error, trace, warn};

use crate::database::models::CacheEntry;
use crate::database::{self, api, Connection};
use crate::errors::{Error, NonUtf8PathError};
use crate::{DateTime, Duration, File, FileStatus, Utc};

/// Carol client.
///
/// Consists of two parts: cached files database and filesystem cache directory,
/// where the files are actually stored.
///
/// This client is responsible for synchronization between cache directory and
/// database states.
/// The main rule is:
///  - File is present in database and has status `Ready` if and only if
///    it is properly stored on the disk.
///
/// Operations like [`Client::get`] and [`Client::remove`] are performed in a transactional manner
/// and must not result in an invalid state.
/// If the state becomes broken anyway, maintenance could possibly detect corrupted state
/// and fix it.
///
/// Carol uses reference counter approach to track which files are "used" at the moment
/// and cannot be removed. Current references to the file are stored in the databse.
/// Each alive [`File`] variable is treated as a reference and this reference is removed on drop.
pub struct Client {
    /// Path to cache directory.
    pub(crate) cache_dir: PathBuf,

    /// Cache database connection.
    pub(crate) db: Connection,

    database_url: String,

    default_duration: Option<Duration>,
}

impl Client {
    /// Initialize Carol client.
    ///
    /// Initializes database if it doesn't exist.
    pub async fn init<P>(database_path: &str, cache_dir: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        trace!("running migrations on {}", database_path);
        database::run_migrations(database_path).await?;
        trace!("establishing cache database connection: {}", database_path);
        let db = database::establish_connection(database_path).await?;
        trace!("checking cache directory: {}", cache_dir.as_ref().display());
        let meta = fs::metadata(cache_dir.as_ref()).await?;
        if !meta.is_dir() {
            return Err(Error::CustomError(
                "cache path is not a directory".to_string(),
            ));
        }
        Ok(Self {
            cache_dir: cache_dir.as_ref().to_path_buf(),
            db,
            database_url: database_path.to_string(),
            default_duration: None,
        })
    }

    /// Set default duration for downloaded files.
    ///
    /// `duration` will be added to [`Utc::now()`] and set
    /// as an expiration timestamp for all downloaded files.
    /// If not set, files won't have any expiration timestamp and will never expire.
    pub fn set_default_duration(&mut self, duration: Duration) {
        self.default_duration = Some(duration);
    }

    /// Append database URL to cache entry.
    fn to_file(&self, entry: CacheEntry) -> File {
        File::from_entry(entry, self.database_url.clone())
    }

    /// Download the file from URL into cache.
    ///
    /// If the file is already cached, it won't be re-downloaded.
    pub async fn get<T>(&mut self, url: T) -> Result<File, Error>
    where
        T: AsRef<str>,
    {
        let url = url.as_ref();
        debug!("get URL: {}", url);
        let url_hash = sha256::digest(url);
        let cache_path = self.cache_dir.join(&url_hash);

        let result = api::new_entry(
            &mut self.db,
            url,
            cache_path.to_str().ok_or(NonUtf8PathError)?,
            self.default_duration.map(|period| Utc::now() + period),
        )
        .await;

        match result {
            Ok(entry) => {
                // File was not in cache, so let's download it
                debug!("cache miss: {:?}", entry);
                let mut file = self
                    .fetch_entry(&entry)
                    .await
                    .map(|entry| self.to_file(entry))?;
                // At this point file was downloaded and stored in cache
                // If this function fails, it means that we only failed
                // to increment the reference counter. So it is safe
                // to just return an error here and let user call `get()` again.
                file.lock().await?;
                Ok(file)
            }
            Err(err) if err.is_unique_violation() => {
                // File is already in cache
                debug!("cache hit: {}", url);
                let mut file = self
                    .wait_entry(url)
                    .await
                    .map(|entry| self.to_file(entry))?;
                // See comment above about the error in reference counter increment.
                file.lock().await?;
                Ok(file)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Find file by URL in cache. Returns `None` is URL is not cached.
    pub async fn find<T>(&mut self, url: T) -> Result<Option<File>, Error>
    where
        T: AsRef<str>,
    {
        api::get_by_url(&mut self.db, url.as_ref())
            .await
            .map(|res| res.map(|entry| self.to_file(entry)))
            .map_err(Into::into)
    }

    /// Set expiration timestamp for URL.
    ///
    /// Returns error if URL is not cached.
    pub async fn set_expires<T>(&mut self, url: T, expires_at: DateTime<Utc>) -> Result<(), Error>
    where
        T: AsRef<str>,
    {
        if let Some(file) = self.find(url).await? {
            api::update_expires(&mut self.db, file.id, Some(expires_at)).await?;
            Ok(())
        } else {
            Err(Error::CustomError("URL is not cached".to_string()))
        }
    }

    /// Remove URL from cache.
    ///
    /// Does nothing if the URL is not cached.
    pub async fn remove<T>(&mut self, url: T) -> Result<(), Error>
    where
        T: AsRef<str>,
    {
        // If this fails, nothing is wrong with the state.
        // File can be safely removed later.
        self.schedule_for_removal(&url).await?;

        if let Some(file) = self.find(url).await? {
            // If this fails, nothing changed in the state, so everything is fine.
            // File can be safely removed later, state is not corrupted.
            self.wait_file_free(&file).await?;

            // If this fails, the file will still be marked as `ToRemove`,
            // so it will be garbage collected later.
            api::remove_entry(&mut self.db, file.id).await?;

            // If this fails, we will have a dangling file in cache directory,
            // which will be removed later on maintenance.
            fs::remove_file(file.cache_path()).await?;
        }
        Ok(())
    }

    /// Schedule URL for removal (set its status to [`FileStatus::ToRemove`]).
    ///
    /// Does nothing if the URL is not cached.
    pub async fn schedule_for_removal<T>(&mut self, url: T) -> Result<(), Error>
    where
        T: AsRef<str>,
    {
        if let Some(file) = self.find(url).await? {
            api::update_status(&mut self.db, file.id, FileStatus::ToRemove).await?;
        }
        Ok(())
    }

    /// Force-remove URL from cache.
    ///
    /// This is an unsafe method, which doesn't check reference counter of the file.
    /// Calling this may lead to file being removed while in use or
    /// cache corruption.
    ///
    /// # Safety
    ///
    /// User must ensure the file is not used and schedule it for removal first.
    pub async unsafe fn remove_unsafe<T>(&mut self, url: T) -> Result<(), Error>
    where
        T: AsRef<str>,
    {
        if let Some(file) = self.find(url).await? {
            // Since this is unsafe method, calling it may lead to invalid cache state
            api::delete_unsafe(&mut self.db, file.id).await?;
            fs::remove_file(file.cache_path()).await?;
        }
        Ok(())
    }

    /// List all cache entries.
    pub async fn list(&mut self) -> Result<Vec<File>, Error> {
        api::get_all(&mut self.db)
            .await
            .map(|res| res.into_iter().map(|entry| self.to_file(entry)).collect())
            .map_err(Into::into)
    }

    /// Query entry URL and store response body in target file. Then update entry status.
    /// If anything fails during this process, clean-up trimmed file and cache entry.
    async fn fetch_entry(&mut self, entry: &CacheEntry) -> Result<CacheEntry, Error> {
        let mut file = fs::File::create_new(&entry.cache_path).await?;

        let mut fetch = async || -> Result<CacheEntry, Error> {
            trace!("fetching {}", &entry.url);
            let response = reqwest::get(&entry.url).await?.error_for_status()?;
            let code = response.status().as_u16();
            if code != 200 {
                // For responses with 2XX codes we probably only interested in 200.
                return Err(Error::CustomError(format!(
                    "HTTP response status code {}",
                    code
                )));
            }
            let mut stream = response.bytes_stream();
            while let Some(chunk_result) = stream.next().await {
                let chunk = chunk_result?;
                file.write_all(&chunk).await?;
            }
            let updated_entry =
                api::update_status(&mut self.db, entry.id, FileStatus::Ready).await?;
            debug!("updating entry status: {:?}", updated_entry);
            Ok(updated_entry)
        };

        match fetch().await {
            Ok(updated_entry) => Ok(updated_entry),
            Err(err) => {
                // cleanup before returning
                // just log error because we already have main error to return
                let failed_to_update = api::update_status(&mut self.db, entry.id, FileStatus::Corrupted)
                    .await
                    .map_err(|err| warn!("failed to mark file, which was not downloaded successfully, as corrupted: {}", err))
                    .is_err();

                let failed_to_remove = fs::remove_file(&entry.cache_path)
                    .await
                    .map_err(|err| {
                        warn!(
                            "failed to remove file, which was not downloaded successfully: {}",
                            err
                        )
                    })
                    .is_err();

                let failed_to_remove_entry = api::remove_entry(&mut self.db, entry.id)
                    .await
                    .map_err(|err| {
                        warn!(
                        "failed to remove cache entry, which was not downloaded successfully: {}",
                        err
                    )
                    })
                    .is_err();

                let corrupted = failed_to_update && failed_to_remove && failed_to_remove_entry;
                if corrupted {
                    // This is a very annoying error for user, because he can't do much about it.
                    // User will have to remove the file manually.
                    // Maintenance cannot fix this automatically because the file wasn't marked
                    // as Corrupted or ToRemove and was not removed from cache directory.
                    // FIXME: there must be something we could do here
                    error!(
                        "Failed to download URL: {}. \
                        This error probably means that your cache state is now corrupted.\
                        Try removing this URL manually using Carol CLI: carol files remove --force '{}'",
                        entry.url, entry.url
                    );
                }

                Err(err)
            }
        }
    }

    /// Wait until file becomes `Ready`.
    async fn wait_entry(&mut self, url: &str) -> Result<CacheEntry, Error> {
        debug!("awaiting URL: {}", url);
        loop {
            if let Some(entry) = api::get_by_url(&mut self.db, url).await? {
                if entry.status == FileStatus::Ready {
                    return Ok(entry);
                } else {
                    // file is still downloading, come back later
                    time::sleep(Duration::from_secs(1)).await;
                }
            } else {
                // If at some point entry dissapeared, it means that is was removed
                // There are two reasons for that:
                // - it was garbage collected (this is very unlikely to happened, so we will ignore this)
                // - downloading failed
                // So we will return downloading error here
                return Err(Error::AwaitingError);
            }
        }
    }

    /// Wait until file reference counter becomes 0.
    ///
    /// Notice that reference counter may be increased after this function returns.
    /// So removing file right after this function call may still fail.
    async fn wait_file_free(&mut self, file: &File) -> Result<(), Error> {
        debug!("waiting file to become free: {:?}", file);
        loop {
            let entry = api::get_entry(&mut self.db, file.id).await?;
            trace!("ref count = {}", entry.ref_count);
            if entry.ref_count == 0 {
                break;
            } else {
                // file is still used, come back later
                time::sleep(Duration::from_secs(1)).await;
            }
        }
        Ok(())
    }
}

// TODO: probably we need some timeouts for downloading/awaiting

/// Client fixtures. Helps in testing client-related code.
#[cfg(test)]
pub(crate) mod fixtures {
    use crate::database::fixtures::CacheDatabaseFixture;
    use crate::Client;
    use http_test_server::http::{Method, Status};
    use http_test_server::TestServer;
    use std::path::PathBuf;
    use tempdir::TempDir;

    type Error = Box<dyn std::error::Error>;

    /// Just keeps temp web server running and stops on drop.
    /// Returns also host string e.g. `http://localhost:8080`.
    pub fn setup_test_server() -> Result<(TestServer, String), Error> {
        let server = TestServer::new()?;
        let resource = server.create_resource("/file.txt");
        resource
            .status(Status::OK)
            .method(Method::GET)
            .header("Content-Type", "text/plain")
            .body("This is test file.");
        let port = server.port();
        let host = format!("http://localhost:{}", port);
        Ok((server, host))
    }

    pub struct ClientFixture {
        /// Holds test web server running and destroys it on drop.
        #[allow(dead_code)]
        web_server: Option<TestServer>,

        /// Host string like `http://localhost:8080`.
        pub host: String,

        /// Client with temp database and cache directory.
        pub client: Client,

        /// Hold cache directory and destroys it on drop.
        #[allow(dead_code)]
        tmp: TempDir,

        /// Path to cache directory.
        pub cache_dir: PathBuf,

        /// Database fixture.
        pub db: CacheDatabaseFixture,
    }

    impl ClientFixture {
        /// Create new client with empty cache.
        pub async fn new() -> Result<Self, Error> {
            let (web_server, host) = setup_test_server()?;
            let db = CacheDatabaseFixture::new().await?;
            let tmp = TempDir::new("carol.test.files").unwrap();
            let cache_dir = tmp.path().to_path_buf();
            let client = Client::init(&db.db_path, &cache_dir).await?;
            Ok(Self {
                web_server: Some(web_server),
                host,
                client,
                db,
                tmp,
                cache_dir,
            })
        }

        pub fn drop_web_server(&mut self) {
            self.web_server = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::ClientFixture;
    use super::*;
    use tempdir::TempDir;
    use tokio::fs;
    use tracing_test::traced_test;

    #[tokio::test]
    #[traced_test]
    async fn test_init_new_cache() {
        let tmp_database = TempDir::new("carol.test.database").unwrap();
        let database = tmp_database.path().join("carol.sqlite");
        let database_path = database.as_os_str().to_str().unwrap().to_string();

        let tmp_cache_dir = TempDir::new("carol.test.database").unwrap();

        Client::init(&database_path, tmp_cache_dir.path())
            .await
            .expect("initialize new cache");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_connect_to_existing_cache() {
        let fixture = ClientFixture::new().await.unwrap();

        Client::init(&fixture.db.db_path, &fixture.cache_dir)
            .await
            .expect("connect to existing cache");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_get() {
        let fixture = ClientFixture::new().await.unwrap();
        let mut client = fixture.client;
        let url = format!("{}/file.txt", &fixture.host);
        let file = client.get(&url).await.expect("get file from URL");
        assert_eq!(
            file.status().await.expect("get file status"),
            FileStatus::Ready
        );
        assert_eq!(file.url(), url);
        let content = fs::read_to_string(file.cache_path())
            .await
            .expect("read downloaded file");
        assert_eq!(content, "This is test file.");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_get_not_re_downloading() {
        let mut fixture = ClientFixture::new().await.unwrap();
        let client = &mut fixture.client;

        let url = format!("{}/file.txt", &fixture.host);
        let first_file = client
            .get(&url)
            .await
            .expect("get file from URL first time");

        // drop server to ensure next time nothing will be downloaded
        fixture.drop_web_server();
        assert!(reqwest::get(&url).await.is_err());

        let client = &mut fixture.client;
        let second_file = client
            .get(&url)
            .await
            .expect("get file from URL second time");
        assert_eq!(first_file.id, second_file.id);
    }
}
