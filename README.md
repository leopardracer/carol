# Carol

Asynchronous managed storage of files in the filesystem. Designed to manage a set of huge files.

The main idea of Carol is to pair files stored in a filesystem directory with a database holding
additional metadata and providing synchronization.

Source tracking and storage eviction features allows to use Carol as a **cache backend** when
fetching huge files from the network (see [`carol-reqwest-middleware`][1]).

This repository contains:

- `carol` - main library crate
- `carol-reqwest-middleware` - HTTP caching middleware for [`reqwest`][2] library

Find out more from docs:

```shell
cargo doc --open
```

## API example

```rust
use carol::{FileSource, StorageManager, StorePolicy};

let cache_dir = "/tmp/carol";

// Create storage directory
std::fs::create_dir_all(&cache_dir).unwrap();

// Initialize storage manager
let manager = StorageManager::init("carol.sqlite", &cache_dir, None).await.unwrap();

// Copy some local file into storage
std::fs::write("myfile", "hello world").unwrap();

let file = manager
    .copy_local_file(
        FileSource::parse("./myfile"),
        StorePolicy::StoreForever,
        Some("myfile".to_string()),
        "myfile",
    )
    .await
    .unwrap();
```

## Roadmap

- [ ] Proper storage eviction
- [ ] Advisory locks for stored files
- [ ] CLI interface for storage manager

[1]: <https://github.com/gevulotnetwork/carol/tree/main/carol-reqwest-middleware>
[2]: <https://crates.io/crates/reqwest>
