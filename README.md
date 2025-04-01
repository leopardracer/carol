# Cache Roller (CAROL)

Asyncronous caching engine for fetching files over HTTP(-S).
Designed to avoid copying huge files. This is **not** a HTTP proxy.

The main idea of Carol is to store downloaded files in the filesystem,
tracking and managing their state through cache database.

Repository contains `carol` Rust library and `carol` CLI tool.

Find out more from docs:

```shell
cargo doc --open
```

See also `examples/` directory.

## API example

```rust
use carol::Client;

// Initialize client
let mut client = Client::init(database, cache_dir).await.unwrap();

// Download file or just get it if it's already downloaded
let file = client.get(url).await.unwrap();

// You can access downloaded file directly from cache directory
let full_path = file.cache_path();

// Alternatively create symlink to downloaded file,
// so it can be accessed at a different path
file.symlink(target).await.unwrap();

// use file however you need
// ...

// "Free" file so it can be removed from cache later
drop(file);
```

## CLI example

This downloads file into cache directory and creates symlink `./example` pointing to the result.

```plaintext
$ carol get https://example.com example
example
$ cat example
<!doctype html>
<html>
<head>
...
$ readlink example 
/home/user/.cache/carol/files/100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9
```

## Roadmap

- [ ] Retry policies for client and maintainers
- [ ] Timeouts for different operations
- [ ] Add more scenarios to integration tests
- [x] Client configuration options (possibly ClientBuilder)
- [ ] Update (re-download) file feature
- [ ] Refine logging
- [ ] Add "Last used" timestamp to file meta
