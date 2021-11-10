# Sled with Types instead of Bytes

[![API](https://docs.rs/typed-sled/badge.svg)](https://docs.rs/typed-sled)

This crate builds on top of [sled] and it's api is identical, except that it uses types in all places where sled would use bytes or it's IVec type. Types are binary encoded using [bincode](https://docs.rs/bincode/1.3.3/bincode/index.html).

## Example

```rust
    let db: sled::Db = sled::open("db")?;

    // The id is used by sled to identify which Tree in the database (db) to open.
    let animals = typed_sled::Tree::<String, Animal>::init(&db, "unique_id");

    let larry = "Larry".to_string();
    animals.insert(&larry, &Animal::Dog)?;

    assert_eq!(animals.get(&larry)?, Some(Animal::Dog));
```

Not tested throughoutly, in particular the Subscriber api might not be implemented correctly.

[sled]: https://github.com/spacejam/sled
