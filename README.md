# Sled with Types instead of Bytes
[![API](https://docs.rs/typed-sled/badge.svg)](https://docs.rs/typed-sled)

This crate builds on top of sled and it's api is identical, except that it uses types in all places where sled would use bytes or it's IVec type.

Simply implement typed_sled::Tree on a wrapper struct of sled::Tree and you are ready to go.

By no means production ready nor tested throughoutly, in particular the Subscriber api might not be implemented correctly.