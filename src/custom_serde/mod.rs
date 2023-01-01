#![allow(clippy::type_complexity)]
//! Support for custom (de)serialization.
//!
//! This module exports the same types as the root module, however the types take
//! an additional generic parameter called `SerDe`. The `SerDe` type
//! must implement the trait [SerDe][crate::custom_serde::serialize::SerDe] which defines how (de)serialization takes place.
//!
//! The [Tree<K, V>][crate::Tree] is equivalent to
//! [Tree<K, V, BincodeSerDe>][crate::custom_serde::Tree] from this module.
//!
//! The following features are supported for the custom (de)serialization Tree:
//! * [key_generating][self::key_generating]: Create `Tree`s with automatically generated keys.
//! * [convert][self::convert]: Convert any `Tree` into another `Tree` with different key and value types.
//!
//! # Example
//! ```
//! use serde::{Deserialize, Serialize};
//! use typed_sled::custom_serde::{serialize::BincodeSerDeLazy, Tree};
//!
//! #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
//! struct SomeValue<'a>(&'a str);
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Creating a temporary sled database.
//!     // If you want to persist the data use sled::open instead.
//!     let db = sled::Config::new().temporary(true).open().unwrap();
//!
//!     // Notice that we are using &str, and SomeValue<'a> here which do not implement
//!     // serde::DeserializeOwned and thus could not be used with typed_sled::Tree.
//!     // However our custom lazy Deserializer contained in BincodeSerDeLazy allows us
//!     // to perform the deserialization lazily and only requires serde::Deserialize<'a>
//!     // for the lazy deserialization.
//!     let tree = Tree::<&str, SomeValue, BincodeSerDeLazy>::open(&db, "unique_id");
//!
//!     tree.insert(&"some_key", &SomeValue("some_value"))?;
//!
//!     assert_eq!(
//!         tree.get(&"some_key")?.unwrap().deserialize(),
//!         SomeValue("some_value")
//!     );
//!     Ok(())
//! }
//! ```
//!
//! [sled]: https://docs.rs/sled/latest/sled/
use crate::custom_serde::serialize::{Deserializer, Key, Serializer, Value};
use core::fmt;
use core::iter::{DoubleEndedIterator, Iterator};
use core::ops::{Bound, RangeBounds};
use sled::{
    transaction::{ConflictableTransactionResult, TransactionResult},
    IVec, Result,
};
use std::marker::PhantomData;

pub mod serialize;

#[cfg(feature = "convert")]
pub mod convert;

#[cfg(feature = "key-generating")]
pub mod key_generating;

// #[cfg(feature = "search")]
// pub mod search;

// pub trait Bin = DeserializeOwned + Serialize + Clone + Send + Sync;

/// A flash-sympathetic persistent lock-free B+ tree.
///
/// A `Tree` represents a single logical keyspace / namespace / bucket.
///
/// # Example
/// ```
/// use serde::{Deserialize, Serialize};
/// use typed_sled::custom_serde::{serialize::BincodeSerDeLazy, Tree};
///
/// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// struct SomeValue<'a>(&'a str);
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Creating a temporary sled database.
///     // If you want to persist the data use sled::open instead.
///     let db = sled::Config::new().temporary(true).open().unwrap();
///
///     // Notice that we are using &str, and SomeValue<'a> here which do not implement
///     // serde::DeserializeOwned and thus could not be used with typed_sled::Tree.
///     // However our custom lazy Deserializer contained in BincodeSerDeLazy allows us
///     // to perform the deserialization lazily and only requires serde::Deserialize<'a>
///     // for the lazy deserialization.
///     let tree = Tree::<&str, SomeValue, BincodeSerDeLazy>::open(&db, "unique_id");
///
///     tree.insert(&"some_key", &SomeValue("some_value"))?;
///
///     assert_eq!(
///         tree.get(&"some_key")?.unwrap().deserialize(),
///         SomeValue("some_value")
///     );
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct Tree<K, V, SerDe> {
    inner: sled::Tree,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
    _serde: PhantomData<fn(SerDe)>,
}

// Manual implementation to make Clone behave better.
// With derive(Clone) calling clone on a reference returns a reference.
impl<K, V, SerDe> Clone for Tree<K, V, SerDe> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _key: PhantomData,
            _value: PhantomData,
            _serde: PhantomData,
        }
    }
}

// // implemented like this in the sled source
// // impl<V: std::fmt::Debug> std::error::Error for CompareAndSwapError<V> {}

// These Trait bounds should probably be specified on the functions themselves, but too lazy.
impl<K, V, SerDe> Tree<K, V, SerDe> {
    /// Initialize a typed tree. The id identifies the tree to be opened from the db.
    /// # Example
    ///
    /// ```
    /// use serde::{Deserialize, Serialize};
    /// use typed_sled::custom_serde::{serialize::BincodeSerDeLazy, Tree};
    ///
    /// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    /// struct SomeValue<'a>(&'a str);
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     // Creating a temporary sled database.
    ///     // If you want to persist the data use sled::open instead.
    ///     let db = sled::Config::new().temporary(true).open().unwrap();
    ///
    ///     // Notice that we are using &str, and SomeValue<'a> here which do not implement
    ///     // serde::DeserializeOwned and thus could not be used with typed_sled::Tree.
    ///     // However our custom lazy Deserializer contained in BincodeSerDeLazy allows us
    ///     // to perform the deserialization lazily and only requires serde::Deserialize<'a>
    ///     // for the lazy deserialization.
    ///     let tree = Tree::<&str, SomeValue, BincodeSerDeLazy>::open(&db, "unique_id");
    ///
    ///     tree.insert(&"some_key", &SomeValue("some_value"))?;
    ///
    ///     assert_eq!(
    ///         tree.get(&"some_key")?.unwrap().deserialize(),
    ///         SomeValue("some_value")
    ///     );
    ///     Ok(())
    /// }
    //     /// ```
    pub fn open<T: AsRef<str>>(db: &sled::Db, id: T) -> Self {
        Self {
            inner: db.open_tree(id.as_ref()).unwrap(),
            _key: PhantomData,
            _value: PhantomData,
            _serde: PhantomData,
        }
    }

    /// Insert a key to a new value, returning the last value if it was set.
    pub fn insert(&self, key: &K, value: &V) -> Result<Option<Value<K, V, SerDe>>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .insert(
                SerDe::SK::serialize(key).as_ref(),
                SerDe::SV::serialize(value).as_ref(),
            )
            .map(|opt| opt.map(|old_value| SerDe::DV::deserialize(old_value)))
    }

    /// Perform a multi-key serializable transaction.
    pub fn transaction<F, A, E>(&self, f: F) -> TransactionResult<A, E>
    where
        F: Fn(&TransactionalTree<K, V, SerDe>) -> ConflictableTransactionResult<A, E>,
    {
        self.inner.transaction(|sled_transactional_tree| {
            f(&TransactionalTree {
                inner: sled_transactional_tree,
                _key: PhantomData,
                _value: PhantomData,
                _serde: PhantomData,
            })
        })
    }

    /// Create a new batched update that can be atomically applied.
    ///
    /// It is possible to apply a Batch in a transaction as well, which is the way you can apply a Batch to multiple Trees atomically.
    pub fn apply_batch(&self, batch: Batch<K, V, SerDe>) -> Result<()> {
        self.inner.apply_batch(batch.inner)
    }

    /// Retrieve a value from the Tree if it exists.
    pub fn get(&self, key: &K) -> Result<Option<Value<K, V, SerDe>>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .get(SerDe::SK::serialize(key))
            .map(|opt| opt.map(|v| SerDe::DV::deserialize(v)))
    }

    /// Retrieve a value from the Tree if it exists. The key must be in serialized form.
    pub fn get_from_raw<B: AsRef<[u8]>>(&self, key_bytes: B) -> Result<Option<Value<K, V, SerDe>>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .get(key_bytes.as_ref())
            .map(|opt| opt.map(|v| SerDe::DV::deserialize(v)))
    }

    /// Deserialize a key and retrieve it's value from the Tree if it exists.
    /// The deserialization is only done if a value was retrieved successfully.
    pub fn get_kv_from_raw<B: AsRef<[u8]>>(
        &self,
        key_bytes: B,
    ) -> Result<Option<(Key<K, V, SerDe>, Value<K, V, SerDe>)>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner.get(key_bytes.as_ref()).map(|opt| {
            opt.map(|v| {
                (
                    SerDe::DK::deserialize(sled::IVec::from(key_bytes.as_ref())),
                    SerDe::DV::deserialize(v),
                )
            })
        })
    }

    /// Delete a value, returning the old value if it existed.
    pub fn remove(&self, key: &K) -> Result<Option<Value<K, V, SerDe>>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .remove(SerDe::SK::serialize(key).as_ref())
            .map(|opt| opt.map(|v| SerDe::DV::deserialize(v)))
    }

    /// Compare and swap. Capable of unique creation, conditional modification, or deletion. If old is None, this will only set the value if it doesn't exist yet. If new is None, will delete the value if old is correct. If both old and new are Some, will modify the value if old is correct.
    ///
    /// It returns Ok(Ok(())) if operation finishes successfully.
    ///
    /// If it fails it returns: - Ok(Err(CompareAndSwapError(current, proposed))) if operation failed to setup a new value. CompareAndSwapError contains current and proposed values. - Err(Error::Unsupported) if the database is opened in read-only mode.
    pub fn compare_and_swap(
        &self,
        key: &K,
        old: Option<&V>,
        new: Option<&V>,
    ) -> Result<core::result::Result<(), CompareAndSwapError<Value<K, V, SerDe>>>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .compare_and_swap(
                SerDe::SK::serialize(key),
                old.map(|old| SerDe::SV::serialize(old))
                    .as_ref()
                    .map(|old| old.as_ref()),
                new.map(|new| SerDe::SV::serialize(new))
                    .as_ref()
                    .map(|new| new.as_ref()),
            )
            .map(|cas_res| {
                cas_res.map_err(|cas_err| CompareAndSwapError {
                    current: cas_err.current.map(|b| SerDe::DV::deserialize(b)),
                    proposed: cas_err.proposed.map(|b| SerDe::DV::deserialize(b)),
                })
            })
    }

    /// Fetch the value, apply a function to it and return the result.
    pub fn update_and_fetch<F>(&self, key: &K, mut f: F) -> Result<Option<Value<K, V, SerDe>>>
    where
        SerDe: serialize::SerDe<K, V>,
        F: FnMut(Option<Value<K, V, SerDe>>) -> Option<V>,
    {
        self.inner
            .update_and_fetch(SerDe::SK::serialize(key), |opt_value| {
                f(opt_value.map(|v| SerDe::DV::deserialize(sled::IVec::from(v)))).map(|value| {
                    // TODO: Maybe add Into<IVec> to SerDe::SV::Bytes
                    let bytes = SerDe::SV::serialize(&value);
                    let mut v = Vec::with_capacity(bytes.as_ref().len());
                    v.extend(bytes.as_ref());
                    v
                })
            })
            .map(|res| res.map(|v| SerDe::DV::deserialize(v)))
    }

    /// Fetch the value, apply a function to it and return the previous value.
    // not sure if implemented correctly (different trait bound for F)
    pub fn fetch_and_update<F>(&self, key: &K, mut f: F) -> Result<Option<Value<K, V, SerDe>>>
    where
        SerDe: serialize::SerDe<K, V>,
        F: FnMut(Option<Value<K, V, SerDe>>) -> Option<V>,
    {
        self.inner
            .fetch_and_update(SerDe::SK::serialize(key), |opt_value| {
                f(opt_value.map(|v| SerDe::DV::deserialize(sled::IVec::from(v)))).map(|value| {
                    // TODO: Maybe add Into<IVec> to SerDe::SV::Bytes
                    let bytes = SerDe::SV::serialize(&value);
                    let mut v = Vec::with_capacity(bytes.as_ref().len());
                    v.extend(bytes.as_ref());
                    v
                })
            })
            .map(|res| res.map(|v| SerDe::DV::deserialize(v)))
    }

    /// Subscribe to `Event`s that happen to keys that have
    /// the specified prefix. Events for particular keys are
    /// guaranteed to be witnessed in the same order by all
    /// threads, but threads may witness different interleavings
    /// of `Event`s across different keys. If subscribers don't
    /// keep up with new writes, they will cause new writes
    /// to block. There is a buffer of 1024 items per
    ///  `Subscriber`. This can be used to build reactive
    /// and replicated systems.
    pub fn watch_prefix(&self, prefix: &K) -> Subscriber<K, V, SerDe>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        Subscriber::from_sled(self.inner.watch_prefix(SerDe::SK::serialize(prefix)))
    }

    /// Subscribe to  all`Event`s. Events for particular keys are
    /// guaranteed to be witnessed in the same order by all
    /// threads, but threads may witness different interleavings
    /// of `Event`s across different keys. If subscribers don't
    /// keep up with new writes, they will cause new writes
    /// to block. There is a buffer of 1024 items per
    /// `Subscriber`. This can be used to build reactive
    /// and replicated systems.
    pub fn watch_all(&self) -> Subscriber<K, V, SerDe>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        Subscriber::from_sled(self.inner.watch_prefix(vec![]))
    }

    /// Synchronously flushes all dirty IO buffers and calls
    /// fsync. If this succeeds, it is guaranteed that all
    /// previous writes will be recovered if the system
    /// crashes. Returns the number of bytes flushed during
    /// this call.
    ///
    /// Flushing can take quite a lot of time, and you should
    /// measure the performance impact of using it on
    /// realistic sustained workloads running on realistic
    /// hardware.
    pub fn flush(&self) -> Result<usize> {
        self.inner.flush()
    }

    /// Asynchronously flushes all dirty IO buffers
    /// and calls fsync. If this succeeds, it is
    /// guaranteed that all previous writes will
    /// be recovered if the system crashes. Returns
    /// the number of bytes flushed during this call.
    ///
    /// Flushing can take quite a lot of time, and you
    /// should measure the performance impact of
    /// using it on realistic sustained workloads
    /// running on realistic hardware.
    pub async fn flush_async(&self) -> Result<usize> {
        self.inner.flush_async().await
    }

    /// Returns `true` if the `Tree` contains a value for
    /// the specified key.
    pub fn contains_key(&self, key: &K) -> Result<bool>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner.contains_key(SerDe::SK::serialize(key))
    }

    /// Retrieve the key and value before the provided key,
    /// if one exists.
    pub fn get_lt(&self, key: &K) -> Result<Option<(Key<K, V, SerDe>, Value<K, V, SerDe>)>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .get_lt(SerDe::SK::serialize(key))
            .map(|res| res.map(|(k, v)| (SerDe::DK::deserialize(k), SerDe::DV::deserialize(v))))
    }

    /// Retrieve the next key and value from the `Tree` after the
    /// provided key.
    pub fn get_gt(&self, key: &K) -> Result<Option<(Key<K, V, SerDe>, Value<K, V, SerDe>)>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .get_gt(SerDe::SK::serialize(key))
            .map(|res| res.map(|(k, v)| (SerDe::DK::deserialize(k), SerDe::DV::deserialize(v))))
    }

    /// Merge state directly into a given key's value using the
    /// configured merge operator. This allows state to be written
    /// into a value directly, without any read-modify-write steps.
    /// Merge operators can be used to implement arbitrary data
    /// structures.
    ///
    /// Calling `merge` will return an `Unsupported` error if it
    /// is called without first setting a merge operator function.
    ///
    /// Merge operators are shared by all instances of a particular
    /// `Tree`. Different merge operators may be set on different
    /// `Tree`s.
    pub fn merge(&self, key: &K, value: &V) -> Result<Option<Value<K, V, SerDe>>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .merge(SerDe::SK::serialize(key), SerDe::SV::serialize(value))
            .map(|res| res.map(|old_v| SerDe::DV::deserialize(old_v)))
    }

    /// For now this maps directly to sled::Tree::set_merge_operator,
    /// meaning you will have to handle (de)serialization yourself.
    ///
    /// Sets a merge operator for use with the `merge` function.
    ///
    /// Merge state directly into a given key's value using the
    /// configured merge operator. This allows state to be written
    /// into a value directly, without any read-modify-write steps.
    /// Merge operators can be used to implement arbitrary data
    /// structures.
    ///
    /// # Panics
    ///
    /// Calling `merge` will panic if no merge operator has been
    /// configured.
    pub fn set_merge_operator(&self, merge_operator: impl sled::MergeOperator + 'static) {
        self.inner.set_merge_operator(merge_operator);
    }

    /// Create a double-ended iterator over the tuples of keys and
    /// values in this tree.
    pub fn iter(&self) -> Iter<K, V, SerDe> {
        Iter::from_sled(self.inner.iter())
    }

    /// Create a double-ended iterator over tuples of keys and values,
    /// where the keys fall within the specified range.
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> Iter<K, V, SerDe>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        match (range.start_bound(), range.end_bound()) {
            (Bound::Unbounded, Bound::Unbounded) => {
                Iter::from_sled(self.inner.range::<&[u8], _>(..))
            }
            (Bound::Unbounded, Bound::Excluded(b)) => {
                Iter::from_sled(self.inner.range(..SerDe::SK::serialize(b)))
            }
            (Bound::Unbounded, Bound::Included(b)) => {
                Iter::from_sled(self.inner.range(..=SerDe::SK::serialize(b)))
            }
            // FIX: This is not excluding lower bound.
            (Bound::Excluded(b), Bound::Unbounded) => {
                Iter::from_sled(self.inner.range(SerDe::SK::serialize(b)..))
            }
            (Bound::Excluded(b), Bound::Excluded(bb)) => Iter::from_sled(
                self.inner
                    .range(SerDe::SK::serialize(b)..SerDe::SK::serialize(bb)),
            ),
            (Bound::Excluded(b), Bound::Included(bb)) => Iter::from_sled(
                self.inner
                    .range(SerDe::SK::serialize(b)..=SerDe::SK::serialize(bb)),
            ),
            (Bound::Included(b), Bound::Unbounded) => {
                Iter::from_sled(self.inner.range(SerDe::SK::serialize(b)..))
            }
            (Bound::Included(b), Bound::Excluded(bb)) => Iter::from_sled(
                self.inner
                    .range(SerDe::SK::serialize(b)..SerDe::SK::serialize(bb)),
            ),
            (Bound::Included(b), Bound::Included(bb)) => Iter::from_sled(
                self.inner
                    .range(SerDe::SK::serialize(b)..=SerDe::SK::serialize(bb)),
            ),
        }
    }

    /// Create an iterator over tuples of keys and values,
    /// where the all the keys starts with the given prefix.
    pub fn scan_prefix(&self, prefix: &K) -> Iter<K, V, SerDe>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        Iter::from_sled(self.inner.scan_prefix(SerDe::SK::serialize(prefix)))
    }

    /// Returns the first key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn first(&self) -> Result<Option<(Key<K, V, SerDe>, Value<K, V, SerDe>)>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .first()
            .map(|res| res.map(|(k, v)| (SerDe::DK::deserialize(k), SerDe::DV::deserialize(v))))
    }

    /// Returns the last key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn last(&self) -> Result<Option<(Key<K, V, SerDe>, Value<K, V, SerDe>)>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .last()
            .map(|res| res.map(|(k, v)| (SerDe::DK::deserialize(k), SerDe::DV::deserialize(v))))
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    pub fn pop_max(&self) -> Result<Option<(Key<K, V, SerDe>, Value<K, V, SerDe>)>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .pop_max()
            .map(|res| res.map(|(k, v)| (SerDe::DK::deserialize(k), SerDe::DV::deserialize(v))))
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    pub fn pop_min(&self) -> Result<Option<(Key<K, V, SerDe>, Value<K, V, SerDe>)>>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .pop_min()
            .map(|res| res.map(|(k, v)| (SerDe::DK::deserialize(k), SerDe::DV::deserialize(v))))
    }

    /// Returns the number of elements in this tree.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the `Tree` contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clears the `Tree`, removing all values.
    ///
    /// Note that this is not atomic.
    pub fn clear(&self) -> Result<()> {
        self.inner.clear()
    }

    /// Returns the name of the tree.
    pub fn name(&self) -> IVec {
        self.inner.name()
    }

    /// Returns the CRC32 of all keys and values
    /// in this Tree.
    ///
    /// This is O(N) and locks the underlying tree
    /// for the duration of the entire scan.
    pub fn checksum(&self) -> Result<u32> {
        self.inner.checksum()
    }
}

pub struct TransactionalTree<'a, K, V, SerDe> {
    inner: &'a sled::transaction::TransactionalTree,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
    _serde: PhantomData<fn(SerDe)>,
}

impl<'a, K, V, SerDe> TransactionalTree<'a, K, V, SerDe> {
    pub fn insert(
        &self,
        key: &K,
        value: &V,
    ) -> std::result::Result<
        Option<Value<K, V, SerDe>>,
        sled::transaction::UnabortableTransactionError,
    >
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .insert(
                SerDe::SK::serialize(key).as_ref(),
                SerDe::SV::serialize(value).as_ref(),
            )
            .map(|opt| opt.map(|v| SerDe::DV::deserialize(v)))
    }

    pub fn remove(
        &self,
        key: &K,
    ) -> std::result::Result<
        Option<Value<K, V, SerDe>>,
        sled::transaction::UnabortableTransactionError,
    >
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .remove(SerDe::SK::serialize(key).as_ref())
            .map(|opt| opt.map(|v| SerDe::DV::deserialize(v)))
    }

    pub fn get(
        &self,
        key: &K,
    ) -> std::result::Result<
        Option<Value<K, V, SerDe>>,
        sled::transaction::UnabortableTransactionError,
    >
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .get(SerDe::SK::serialize(key))
            .map(|opt| opt.map(|v| SerDe::DV::deserialize(v)))
    }

    pub fn apply_batch(
        &self,
        batch: &Batch<K, V, SerDe>,
    ) -> std::result::Result<(), sled::transaction::UnabortableTransactionError> {
        self.inner.apply_batch(&batch.inner)
    }

    pub fn flush(&self) {
        self.inner.flush()
    }

    pub fn generate_id(&self) -> Result<u64> {
        self.inner.generate_id()
    }
}

pub struct Iter<K, V, SerDe> {
    inner: sled::Iter,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
    _serde: PhantomData<fn(SerDe)>,
}

impl<K, V, SerDe: serialize::SerDe<K, V>> Iterator for Iter<K, V, SerDe> {
    type Item = Result<(Key<K, V, SerDe>, Value<K, V, SerDe>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|res| res.map(|(k, v)| (SerDe::DK::deserialize(k), SerDe::DV::deserialize(v))))
    }

    fn last(mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|res| res.map(|(k, v)| (SerDe::DK::deserialize(k), SerDe::DV::deserialize(v))))
    }
}

impl<K, V, SerDe: serialize::SerDe<K, V>> DoubleEndedIterator for Iter<K, V, SerDe> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|res| res.map(|(k, v)| (SerDe::DK::deserialize(k), SerDe::DV::deserialize(v))))
    }
}

impl<K, V, SerDe> Iter<K, V, SerDe> {
    pub fn from_sled(iter: sled::Iter) -> Self {
        Iter {
            inner: iter,
            _key: PhantomData,
            _value: PhantomData,
            _serde: PhantomData,
        }
    }

    pub fn keys(self) -> impl DoubleEndedIterator<Item = Result<Key<K, V, SerDe>>> + Send + Sync
    where
        SerDe: serialize::SerDe<K, V>,
        K: Sync + Send,
        V: Sync + Send,
    {
        self.map(|r| r.map(|(k, _v)| k))
    }

    /// Iterate over the values of this Tree
    pub fn values(self) -> impl DoubleEndedIterator<Item = Result<Value<K, V, SerDe>>> + Send + Sync
    where
        SerDe: serialize::SerDe<K, V>,
        K: Sync + Send,
        V: Sync + Send,
    {
        self.map(|r| r.map(|(_k, v)| v))
    }
}

#[derive(Clone, Debug)]
pub struct Batch<K, V, SerDe> {
    inner: sled::Batch,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
    _serde: PhantomData<fn(SerDe)>,
}

impl<K, V, SerDe> Batch<K, V, SerDe> {
    pub fn insert(&mut self, key: &K, value: &V)
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner.insert(
            SerDe::SK::serialize(key).as_ref(),
            SerDe::SV::serialize(value).as_ref(),
        );
    }

    pub fn remove(&mut self, key: &K)
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner.remove(SerDe::SK::serialize(key).as_ref())
    }
}

// Implementing Default manually to not require K, V and SerDe to implement Default.
impl<K, V, SerDe> Default for Batch<K, V, SerDe> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            _key: PhantomData,
            _value: PhantomData,
            _serde: PhantomData,
        }
    }
}

use pin_project::pin_project;
#[pin_project]
pub struct Subscriber<K, V, SerDe> {
    #[pin]
    inner: sled::Subscriber,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
    _serde: PhantomData<fn(SerDe)>,
}

impl<K, V, SerDe> Subscriber<K, V, SerDe> {
    pub fn next_timeout(
        &mut self,
        timeout: core::time::Duration,
    ) -> core::result::Result<Event<K, V, SerDe>, std::sync::mpsc::RecvTimeoutError>
    where
        SerDe: serialize::SerDe<K, V>,
    {
        self.inner
            .next_timeout(timeout)
            .map(|e| Event::from_sled(e))
    }

    pub fn from_sled(subscriber: sled::Subscriber) -> Self {
        Self {
            inner: subscriber,
            _key: PhantomData,
            _value: PhantomData,
            _serde: PhantomData,
        }
    }
}

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
impl<K: Unpin, V: Unpin, SerDe: serialize::SerDe<K, V>> Future for Subscriber<K, V, SerDe> {
    type Output = Option<Event<K, V, SerDe>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project()
            .inner
            .poll(cx)
            .map(|opt| opt.map(|e| Event::from_sled(e)))
    }
}

impl<K, V, SerDe: serialize::SerDe<K, V>> Iterator for Subscriber<K, V, SerDe> {
    type Item = Event<K, V, SerDe>;

    fn next(&mut self) -> Option<Event<K, V, SerDe>> {
        self.inner.next().map(|e| Event::from_sled(e))
    }
}

pub enum Event<K, V, SerDe: serialize::SerDe<K, V>> {
    Insert {
        key: Key<K, V, SerDe>,
        value: Value<K, V, SerDe>,
    },
    Remove {
        key: Key<K, V, SerDe>,
    },
}

impl<K, V, SerDe: serialize::SerDe<K, V>> Event<K, V, SerDe> {
    pub fn key(&self) -> &Key<K, V, SerDe> {
        match self {
            Self::Insert { key, .. } | Self::Remove { key } => key,
        }
    }

    pub fn from_sled(event: sled::Event) -> Self {
        match event {
            sled::Event::Insert { key, value } => Self::Insert {
                key: SerDe::DK::deserialize(key),
                value: SerDe::DV::deserialize(value),
            },
            sled::Event::Remove { key } => Self::Remove {
                key: SerDe::DK::deserialize(key),
            },
        }
    }
}

/// Compare and swap error.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompareAndSwapError<V> {
    /// The current value which caused your CAS to fail.
    pub current: Option<V>,
    /// Returned value that was proposed unsuccessfully.
    pub proposed: Option<V>,
}

impl<V> fmt::Display for CompareAndSwapError<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Compare and swap conflict")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::custom_serde::serialize::BincodeSerDe;

    #[test]
    fn test_range() {
        let config = sled::Config::new().temporary(true);
        let db = config.open().unwrap();

        let tree: Tree<u32, u32, BincodeSerDe> = Tree::open(&db, "test_tree");

        tree.insert(&1, &2).unwrap();
        tree.insert(&3, &4).unwrap();
        tree.insert(&6, &2).unwrap();
        tree.insert(&10, &2).unwrap();
        tree.insert(&15, &2).unwrap();
        tree.flush().unwrap();

        let expect_results = [(6, 2), (10, 2)];

        for (i, result) in tree.range(6..11).enumerate() {
            assert_eq!(result.unwrap(), expect_results[i]);
        }
    }

    #[test]
    fn test_cas() {
        let config = sled::Config::new().temporary(true);
        let db = config.open().unwrap();

        let tree: Tree<u32, u32, BincodeSerDe> = Tree::open(&db, "test_tree");

        let current = 2;
        tree.insert(&1, &current).unwrap();
        let expected = 3;
        let proposed = 4;
        let res = tree
            .compare_and_swap(&1, Some(&expected), Some(&proposed))
            .expect("db failure");

        assert_eq!(
            res,
            Err(CompareAndSwapError {
                current: Some(current),
                proposed: Some(proposed),
            }),
        );
    }
}
