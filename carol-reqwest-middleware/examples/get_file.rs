use std::time::Duration;

use carol_reqwest_middleware::storage::{FileMetadata, StorageManager, StorePolicy};
use carol_reqwest_middleware::CarolMiddleware;

const DATABASE_URL: &str = "get-file.carol.sqlite";
const CACHE_DIR: &str = "get-file.carol.files";

/// `get_file URL`
#[tokio::main]
async fn main() {
    let url = std::env::args().nth(1).expect("URL not provided");

    let storage_manager = StorageManager::init(DATABASE_URL, CACHE_DIR, None)
        .await
        .expect("init storage manager");
    let reqwest_client = reqwest::Client::builder().build().unwrap();
    let client = reqwest_middleware::ClientBuilder::new(reqwest_client)
        .with(CarolMiddleware {
            storage_manager,
            store_policy: StorePolicy::ExpiresAfterNotUsedFor {
                duration: Duration::from_secs(3600),
            },
        })
        .build();

    let response = client.get(&url).send().await.expect("get URL");
    let file = response
        .json::<FileMetadata>()
        .await
        .expect("deserialize response");

    println!("{:#?}", file);
}
