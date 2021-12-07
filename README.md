# Sled with Types instead of Bytes

[![API](https://docs.rs/typed-sled/badge.svg)](https://docs.rs/typed-sled)

This crate builds on top of [sled], a high-performance embedded database with
an API that is similar to a `BTreeMap<[u8], [u8]>`. The (de)serialization of
keys and values that are inserted into a sled::Tree for you.

## Features

Multiple features for common use cases are available:

- Search engine for searching through a trees keys and values using [tantivy].
- Automatic key generation.
- Converting one typed Tree to another typed Tree with different key and value types.
- Custom (de)serialization. By default [bincode] is used for (de)serialization, however custom (de)serializers are supported, making zero-copy or lazy (de)serialization possible.

## Example

```rust
use serde::{Deserialize, Serialize};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // If you want to persist the data use sled::open instead
    let db = sled::Config::new().temporary(true).open().unwrap();

    // The id is used by sled to identify which Tree in the database (db) to open.
    let tree = typed_sled::Tree::<String, SomeValue>::open(&db, "unique_id");

    tree.insert(&"some_key".to_owned(), &SomeValue(10))?;

    assert_eq!(tree.get(&"some_key".to_owned())?, Some(SomeValue(10)));
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SomeValue(u32);
```

[sled]: https://github.com/spacejam/sled
[bincode]: https://github.com/bincode-org/bincode
[tantivy]: https://github.com/quickwit-inc/tantivy
