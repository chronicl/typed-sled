# Sled with Types instead of Bytes
[![API](https://docs.rs/typed-sled/badge.svg)](https://docs.rs/typed-sled)

This crate builds on top of ['sled'] and it's api is identical, except that it uses types in all places where sled would use bytes or it's IVec type.

## Example

```rust
    let db: sled::Db = sled::open("db")?;
    let animals = typed_sled::Tree<String, Animal>::init(&db, "animals")
    tree.insert("Larry", Animal::Dog)?;
```

Not tested throughoutly, in particular the Subscriber api might not be implemented correctly.

['sled']: https://github.com/spacejam/sled