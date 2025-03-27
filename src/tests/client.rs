//! Carol client tests.

/// Client fixtures. Helps in testing client-related code.
pub(crate) mod fixtures {
    use crate::tests::database::fixtures::CacheDatabaseFixture;
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

use crate::{Client, FileStatus};
use fixtures::ClientFixture;
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
    assert_eq!(file.status, FileStatus::Ready);
    assert_eq!(file.url, url);
    let content = fs::read_to_string(&file.cache_path)
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
