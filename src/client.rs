use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::Client as ReqwestClient;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time;
use tokio_stream::StreamExt;
use tracing::{debug, error, trace, warn};

use crate::database::models::CacheEntry;
use crate::database::{self, api, Connection};
use crate::errors::{Error, NonUtf8PathError};
use crate::{DateTime, File, FileStatus, RetryPolicy, Utc};

type StdResult<T, E> = std::result::Result<T, E>;
type Result<T> = StdResult<T, Error>;

/// Used to create precisely configured [`Client`].
#[must_use]
#[derive(Clone, Debug, Default)]
pub struct ClientBuilder {
    cache_dir: PathBuf,
    database_url: String,
    reqwest_client: Option<ReqwestClient>,
    default_file_duration: Option<Duration>,
    download_retry_policy: RetryPolicy,
}

impl ClientBuilder {
    /// Create new client builder.
    pub fn new<P>(database_url: &str, cache_dir: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            database_url: database_url.to_string(),
            cache_dir: cache_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    /// Set [`reqwest::Client`] to use for downloading.
    ///
    /// If not set, [`Client`] will build default [`reqwest::Client`] on initialization.
    pub fn reqwest_client(mut self, reqwest_client: ReqwestClient) -> Self {
        self.reqwest_client = Some(reqwest_client);
        self
    }

    /// Set default file duration.
    ///
    /// `duration` will be added to [`Utc::now()`] and set as an expiration timestamp when
    /// downloading a file. If not set, new files won't have any expiration timestamp and
    /// will never expire.
    pub fn default_file_duration(mut self, duration: Duration) -> Self {
        self.default_file_duration = Some(duration);
        self
    }

    /// Set retry policy for fetching new files.
    /// Applies only to downloading process during [`Client::get`].
    ///
    /// If not set, [`RetryPolicy::None`] will be used.
    pub fn download_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.download_retry_policy = policy;
        self
    }

    /// Build client.
    ///
    /// # Errors
    ///
    /// Returns error if client initialization fails.
    pub async fn build(self) -> Result<Client> {
        let mut client = Client::init(&self.database_url, &self.cache_dir).await?;
        if let Some(reqwest_client) = self.reqwest_client {
            client.reqwest_client = reqwest_client;
        }
        client.default_duration = self.default_file_duration;
        client.download_retry_policy = self.download_retry_policy;
        Ok(client)
    }
}

/// Carol client.
///
/// Operations like [`Client::get`] and [`Client::remove`] are performed in a transactional manner
/// and should not result in an invalid state.
/// If the state becomes broken anyway, maintenance could possibly detect corrupted state
/// and fix it.
///
/// Downloading is implemented through [`reqwest`].
///
/// See for [crate-level documentation][crate] for more information.
pub struct Client {
    /// Path to cache directory.
    pub(crate) cache_dir: PathBuf,

    /// Cache database connection.
    pub(crate) db: Connection,

    /// We store origin of the database to pass to `File`-s
    database_url: String,

    reqwest_client: ReqwestClient,
    default_duration: Option<Duration>,
    download_retry_policy: RetryPolicy,
}

impl Client {
    /// Create [`ClientBuilder`] for precise [`Client`] configuration.
    pub fn builder<P>(database_url: &str, cache_dir: P) -> ClientBuilder
    where
        P: AsRef<Path>,
    {
        ClientBuilder::new(database_url, cache_dir)
    }

    /// Initialize Carol client.
    ///
    /// Initializes cache directory and database if they don't exist.
    pub async fn init<P>(database_url: &str, cache_dir: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        trace!("running migrations on {}", database_url);
        database::run_migrations(database_url).await?;

        trace!("establishing cache database connection: {}", database_url);
        let db = database::establish_connection(database_url).await?;

        // Database must store absolute paths for cached files,
        // so this path will be stored as absolute.
        let cache_dir = std::path::absolute(cache_dir)?;
        if !cache_dir.exists() {
            fs::create_dir_all(&cache_dir).await?;
        }

        let reqwest_client = ReqwestClient::builder()
            .build()
            .map_err(Error::ReqwestClientBuildError)?;

        Ok(Self {
            cache_dir,
            db,
            database_url: database_url.to_string(),
            default_duration: None,
            reqwest_client,
            download_retry_policy: RetryPolicy::None,
        })
    }

    /// Download the file from URL into cache.
    ///
    /// If the file is already cached, it won't be re-downloaded.
    pub async fn get<T>(&mut self, url: T) -> Result<File>
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

        let entry = match result {
            Ok(entry) => {
                // File was not in cache, so let's download it
                debug!("cache miss: {:?}", entry);
                let result = self.fetch_entry(&entry).await;
                match result {
                    Ok(entry) => entry,
                    Err(err) => match self.download_retry_policy {
                        RetryPolicy::None => return Err(err),
                        RetryPolicy::Fixed { number, period } => {
                            let mut result = Err(err);

                            // retry cycle
                            for i in 0..number {
                                time::sleep(period).await;
                                debug!("retrying fetch URL '{}', attempt #{}", entry.url, i + 1);
                                match self.fetch_entry(&entry).await {
                                    Ok(entry) => {
                                        result = Ok(entry);
                                        break;
                                    }
                                    Err(err) => result = Err(err),
                                }
                            }

                            // latest occured error will be thrown
                            result?
                        }
                    },
                }
            }
            Err(err) if err.is_unique_violation() => {
                // File is already in cache
                debug!("cache hit: {}", url);
                self.wait_entry(url).await?
            }
            Err(err) => return Err(err.into()),
        };

        // At this point file was downloaded and stored in cache
        // If this function fails, it means that we only failed
        // to increment the reference counter. So it is safe
        // to just return an error here and let user call `get()` again.
        self.create_locked_file(entry).await
    }

    /// Find file by URL in cache. Returns `None` is URL is not cached.
    pub async fn find<T>(&mut self, url: T) -> Result<Option<File>>
    where
        T: AsRef<str>,
    {
        debug!("lookup URL '{}'", url.as_ref());
        let maybe_entry = api::get_by_url(&mut self.db, url.as_ref()).await?;
        Ok(if let Some(entry) = maybe_entry {
            Some(self.create_locked_file(entry).await?)
        } else {
            None
        })
    }

    /// Set expiration timestamp for URL.
    ///
    /// Returns error if URL is not cached.
    pub async fn set_expires<T>(&mut self, url: T, expires_at: DateTime<Utc>) -> Result<()>
    where
        T: AsRef<str>,
    {
        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            api::update_expires(&mut self.db, entry.id, Some(expires_at)).await?;
            Ok(())
        } else {
            Err(Error::UrlNotCached(url.as_ref().to_string()))
        }
    }

    /// Remove URL from cache.
    ///
    /// Does nothing if the URL is not cached.
    pub async fn remove<T>(&mut self, url: T) -> Result<()>
    where
        T: AsRef<str>,
    {
        debug!("removing URL '{}' from cache", url.as_ref());
        // If this fails, nothing is wrong with the state.
        // File can be safely removed later.
        self.schedule_for_removal(&url).await?;

        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            // If this fails, the file will still be marked as `ToRemove`,
            // so it will be garbage collected later.
            api::remove_entry(&mut self.db, entry.id).await?;

            // If this fails, we will have a dangling file in cache directory,
            // which will be removed later on maintenance.
            fs::remove_file(entry.cache_path).await?;
        }
        Ok(())
    }

    /// Schedule URL for removal (set its status to [`FileStatus::ToRemove`]).
    ///
    /// Does nothing if the URL is not cached.
    pub async fn schedule_for_removal<T>(&mut self, url: T) -> Result<()>
    where
        T: AsRef<str>,
    {
        debug!("scheduling URL '{}' for removal", url.as_ref());
        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            api::update_status(&mut self.db, entry.id, FileStatus::ToRemove).await?;
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
    pub async unsafe fn remove_unsafe<T>(&mut self, url: T) -> Result<()>
    where
        T: AsRef<str>,
    {
        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            // Since this is unsafe method, calling it may lead to invalid cache state
            api::delete_unsafe(&mut self.db, entry.id).await?;
            fs::remove_file(entry.cache_path).await?;
        }
        Ok(())
    }

    /// List all cache entries.
    pub async fn list(&mut self) -> Result<Vec<File>> {
        let entries = api::get_all(&mut self.db)
            .await
            .map(|res| res.into_iter().collect::<Vec<_>>())?;
        let mut files = Vec::new();
        // TODO: iterate files asynchronously
        for entry in entries {
            files.push(self.create_locked_file(entry).await?);
        }
        Ok(files)
    }

    /// Update file with given URL.
    ///
    /// This will remove old cache entry and create new one.
    ///
    /// # Errors
    ///
    /// Returns error if:
    ///
    /// - URL is not cached
    /// - File is used at the moment
    pub async fn update<T>(&mut self, url: T) -> Result<File>
    where
        T: AsRef<str>,
    {
        debug!("updating URL '{}'", url.as_ref());
        if api::get_by_url(&mut self.db, url.as_ref()).await?.is_some() {
            self.remove(url.as_ref()).await?;
            self.get(url.as_ref()).await
        } else {
            Err(Error::UrlNotCached(url.as_ref().to_string()))
        }
    }

    /// Wait until file reference counter for URL becomes 0.
    ///
    /// Loops with period of 1 second until reach `timeout`.
    ///
    /// # Warning
    ///
    /// This method **does not guarantee** that right after return file cannot become again.
    /// E.g. remove right after this call may stil fail with "reference counter is not 0".
    /// This will work only in relaxed scenarios, when files are not frequently work.
    /// Basically used during maintenance.
    ///
    /// # Errors
    ///
    /// Returns error if:
    ///
    /// - URL is not cached
    /// - Timeout is exceeded
    pub async fn wait_url_released<T>(&mut self, url: T, timeout: Duration) -> Result<()>
    where
        T: AsRef<str>,
    {
        const PERIOD: Duration = Duration::from_secs(1);

        debug!("waiting URL '{}' to become free", url.as_ref());
        if let Some(entry) = api::get_by_url(&mut self.db, url.as_ref()).await? {
            let mut inner = async || -> Result<()> {
                loop {
                    let entry = api::get_entry(&mut self.db, entry.id).await?;
                    if entry.ref_count == 0 {
                        debug!("URL '{}' is free", url.as_ref());
                        break;
                    } else {
                        // file is still used, come back later
                        time::sleep(PERIOD).await;
                    }
                }
                Ok(())
            };

            match time::timeout(timeout, inner()).await {
                Ok(result) => result,
                Err(_) => Err(Error::TimeoutExceeded),
            }
        } else {
            Err(Error::UrlNotCached(url.as_ref().to_string()))
        }
    }

    /// Query entry URL and store response body in target file. Then update entry status.
    /// If anything fails during this process, clean-up trimmed file and cache entry.
    async fn fetch_entry(&mut self, entry: &CacheEntry) -> Result<CacheEntry> {
        let mut file = fs::File::create_new(&entry.cache_path).await?;

        let mut fetch = async || -> Result<CacheEntry> {
            trace!("fetching {}", &entry.url);
            let response = self
                .reqwest_client
                .get(&entry.url)
                .send()
                .await?
                .error_for_status()?;
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

    /// Wait until file comes out of `Pending`.
    async fn wait_entry(&mut self, url: &str) -> Result<CacheEntry> {
        debug!("awaiting URL: {}", url);
        let maybe_id = api::get_by_url(&mut self.db, url)
            .await?
            .map(|entry| entry.id);

        if let Some(id) = maybe_id {
            loop {
                match api::get_entry(&mut self.db, id).await {
                    Ok(entry) if entry.status == FileStatus::Pending => {
                        // file is still downloading, come back later
                        time::sleep(Duration::from_secs(1)).await;
                    }
                    Ok(entry) => return Ok(entry),
                    Err(err) if err.is_not_found() => {
                        // If at some point entry dissapeared, it means that is was removed
                        // There are two reasons for that:
                        // - it was garbage collected (this is very unlikely to happened, so we will ignore this)
                        // - downloading failed
                        // So we will return downloading error here
                        return Err(Error::AwaitingError);
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        } else {
            Err(Error::UrlNotCached(url.to_string()))
        }
    }

    /// Pair database URL with cache entry, create [`File`] and increment its reference counter.
    async fn create_locked_file(&mut self, entry: CacheEntry) -> Result<File> {
        let mut file = File::from_entry(entry, self.database_url.clone());
        api::increment_ref(&mut self.db, file.id).await?;
        file.released = false;
        Ok(file)
    }
}

// TODO: Most of the method implementations abuse lock/release system of files,
//       which results into a lot of unneccessary database connections.
//       It would be nice to re-write implementations without the use of `File`-s.

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("cache_dir", &self.cache_dir)
            .field("database_url", &self.database_url)
            .field("default_duration", &self.default_duration)
            .field("reqwest_client", &self.reqwest_client)
            .finish()
    }
}

/// Client fixtures. Helps in testing client-related code.
#[cfg(test)]
pub(crate) mod fixtures {
    use crate::database::fixtures::CacheDatabaseFixture;
    use crate::{Client, RetryPolicy};
    use http_test_server::http::{Method, Status};
    use http_test_server::TestServer;
    use std::path::PathBuf;
    use std::time::Duration;
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
            let client = Client::builder(&db.db_path, &cache_dir)
                .download_retry_policy(RetryPolicy::Fixed {
                    number: 3,
                    period: Duration::from_secs(1),
                })
                .build()
                .await?;
            Ok(Self {
                web_server: Some(web_server),
                host,
                client,
                db,
                tmp,
                cache_dir,
            })
        }

        pub fn drop_web_server(&mut self) -> TestServer {
            std::mem::take(&mut self.web_server).unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::ClientFixture;
    use super::*;
    use crate::errors::{DatabaseError, RemoveErrorReason};
    use std::time::Duration;
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

    #[tokio::test]
    #[traced_test]
    async fn test_downloading_retry() {
        let mut fixture = ClientFixture::new().await.unwrap();
        let url = format!("{}/new_file.txt", &fixture.host);
        let test_server = fixture.drop_web_server();

        // Create resource after 2500 millisecs
        // Give client time to fail 2 times before providing resource
        let create_resource = async || {
            tokio::time::sleep(Duration::from_millis(2500)).await;
            use http_test_server::http::{Method, Status};

            let resource = test_server.create_resource("/new_file.txt");
            resource
                .status(Status::OK)
                .method(Method::GET)
                .header("Content-Type", "text/plain")
                .body("This is new test file.");
        };

        let get_resource = async || {
            let mut client = fixture.client;
            let file = client
                .get(&url)
                .await
                .expect("get URL without errors after 3 retries");
            let content = fs::read_to_string(file.cache_path())
                .await
                .expect("read downloaded file");
            assert_eq!(content, "This is new test file.");
        };

        tokio::join!(create_resource(), get_resource());

        // check that client tried fetching 2 times before success
        assert!(logs_contain(&format!(
            "retrying fetch URL '{}', attempt #1",
            url
        )));
        assert!(logs_contain(&format!(
            "retrying fetch URL '{}', attempt #2",
            url
        )));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_remove() {
        let fixture = ClientFixture::new().await.unwrap();
        let mut client = fixture.client;
        let url = format!("{}/file.txt", &fixture.host);
        let file = client.get(&url).await.expect("get file from URL");
        file.release().await.expect("release file");
        client.remove(&url).await.expect("remove URL from cache");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_remove_failure() {
        let fixture = ClientFixture::new().await.unwrap();
        let mut client = fixture.client;
        let url = format!("{}/file.txt", &fixture.host);
        let file = client.get(&url).await.expect("get file from URL");
        let result = client.remove(&url).await;
        assert!(matches!(
            result,
            Err(Error::DatabaseError(DatabaseError::RemoveError(
                RemoveErrorReason::UsedFile
            )))
        ));
        file.release().await.unwrap();
    }

    #[tokio::test]
    #[traced_test]
    async fn test_set_expires() {
        let fixture = ClientFixture::new().await.unwrap();
        let mut client = fixture.client;
        let url = format!("{}/file.txt", &fixture.host);
        client.get(&url).await.unwrap();
        client
            .set_expires(&url, DateTime::<Utc>::MAX_UTC)
            .await
            .expect("set expiration timestamp");
        let file = client.get(&url).await.unwrap();
        let expires_at = file.expires().await.expect("get expiration timestamp");
        assert_eq!(expires_at, Some(DateTime::<Utc>::MAX_UTC));
    }

    #[tokio::test]
    #[traced_test]
    async fn test_schedule_for_removal() {
        let fixture = ClientFixture::new().await.unwrap();
        let mut client = fixture.client;
        let url = format!("{}/file.txt", &fixture.host);
        client.get(&url).await.unwrap();
        client
            .schedule_for_removal(&url)
            .await
            .expect("schedule URL for removal");
        let file = client.get(&url).await.unwrap();
        let status = file.status().await.expect("get file status");
        assert_eq!(status, FileStatus::ToRemove);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_update() {
        let fixture = ClientFixture::new().await.unwrap();
        let mut client = fixture.client;
        let url = format!("{}/file.txt", &fixture.host);
        let old_file = client.get(&url).await.unwrap();
        let old_timestamp = *old_file.created();
        // release file to allow updating it
        old_file.release().await.unwrap();

        let new_file = client.update(&url).await.expect("update cache entry");
        let new_timestamp = new_file.created();
        assert!(new_timestamp > &old_timestamp);
    }

    #[tokio::test]
    #[traced_test]
    async fn test_wait_url_released() {
        let fixture = ClientFixture::new().await.unwrap();
        let mut client = fixture.client;
        let url = format!("{}/file.txt", &fixture.host);
        let file = client.get(&url).await.expect("get file from URL");

        // Release file after 1 second
        let release_file = async || {
            tokio::time::sleep(Duration::from_secs(1)).await;
            file.release().await.unwrap();
        };

        // Wait 5 seconds for URL to become free
        let mut wait_for_url_released = async || {
            client
                .wait_url_released(&url, Duration::from_secs(5))
                .await
                .expect("wait for URL released");
        };

        tokio::join!(release_file(), wait_for_url_released());
    }

    #[tokio::test]
    #[traced_test]
    async fn test_wait_url_released_failure() {
        let fixture = ClientFixture::new().await.unwrap();
        let mut client = fixture.client;
        let url = format!("{}/file.txt", &fixture.host);
        let file = client.get(&url).await.expect("get file from URL");

        // Release file after 2 second
        let release_file = async move || {
            tokio::time::sleep(Duration::from_secs(2)).await;
            file.release().await.unwrap();
        };

        // Wait 1 seconds for URL to become free (should fail)
        let mut wait_for_url_released = async || {
            let result = client.wait_url_released(&url, Duration::from_secs(1)).await;
            assert!(matches!(dbg!(result), Err(Error::TimeoutExceeded)));
        };

        tokio::join!(release_file(), wait_for_url_released());
    }

    // TODO: tests granularity:
    //       don't mix up methods in tests, prepare proper cache state manually instead
}
