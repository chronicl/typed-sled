# typed-sled - a database build on top of sled

[![API](https://docs.rs/typed-sled/badge.svg)](https://docs.rs/typed-sled)

sled is a high-performance embedded database with an API that is similar to a `BTreeMap<[u8], [u8]>`.  
typed-sled builds on top of sled and offers an API that is similar to a `BTreeMap<K, V>`, where K and V are user defined types.

## Example

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SomeValue(u32);

// Creating a temporary sled database
let db = sled::Config::new().temporary(true).open().unwrap();

// The id is used by sled to identify which Tree in the database (db) to open
let tree = typed_sled::Tree::<String, SomeValue>::open(&db, "unique_id");

// insert and get, similar to std's BTreeMap
tree.insert(&"some_key".to_owned(), &SomeValue(10))?;

assert_eq!(tree.get(&"some_key".to_owned())?, Some(SomeValue(10)));
Ok(())


```

## features

Multiple features for common use cases are available:

- Search engine for searching through a tree's keys and values by using [tantivy].
- Automatic key generation.
- Custom (de)serialization. By default [bincode] is used for (de)serialization, however custom (de)serializers are supported, making zero-copy or lazy (de)serialization possible.
- Converting one typed Tree to another typed Tree with different key and value types.

[sled]: https://github.com/spacejam/sled
[bincode]: https://github.com/bincode-org/bincode
[tantivy]: https://github.com/quickwit-inc/tantivy
