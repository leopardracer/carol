# Cache Roller (CAROL)

Library to download from HTTP(-S) servers and caching the results.

## Usage example

```rust
// Fetch a single file
let mut client = carol::Client::init("carol.sqlite", ".cache").await?;
let file = client.get("https://example.com").await?;
drop(file);
client.remove("https://example.com").await?;
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
- [ ] Client configuration options (possibly ClientBuilder)
- [ ] Update (re-download) file feature
- [ ] Refine logging
