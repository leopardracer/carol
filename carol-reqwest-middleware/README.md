# carol-reqwest-middleware

HTTP caching middleware for [`reqwest`][1] library. Middleware interface is provided by
[`reqwest-middleware`][2].

## Example

```rust
use carol_reqwest_middleware::storage::{FileMetadata, StorageManager, StorePolicy};
use carol_reqwest_middleware::CarolMiddleware;

let storage_manager = StorageManager::init(database_url, cache_dir, None).await.unwrap();
let carol_middleware = CarolMiddleware {
    storage_manager,
    store_policy: StorePolicy::ExpiresAfterNotUsedFor {
        duration: std::time::Duration::from_secs(3600),
    },
};

let reqwest_client = reqwest::Client::builder().build().unwrap();
let client = reqwest_middleware::ClientBuilder::new(reqwest_client).with(carol_middleware).build();
let response = client.get("https://example.com").send().await.unwrap();
let file = response
    .json::<FileMetadata>()
    .await
    .unwrap();

// Downloaded file is stored at 'file.path'
let content = std::fs::read(&file.path);
```

[1]: <https://crates.io/crates/reqwest>
[2]: <https://crates.io/crates/reqwest-middleware>
