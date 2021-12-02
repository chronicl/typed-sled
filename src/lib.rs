//! Sled with Types instead of Bytes.
//!
//! This crate builds on top of [sled] and handles the (de)serialization
//! of keys and values that are inserted into a sled::Tree for you.
//! It also includes a few convenience features:
//! * [convert]: Convert any `Tree` into another `Tree` with different key and value types.
//! * [key_generating]: Create `Tree`s with automatically generated keys.
//! * [search]: `SearchEngine` on top of a `Tree`.
//!
//! Some info about [sled]:
//! `sled` is a high-performance embedded database with
//! an API that is similar to a `BTreeMap<[u8], [u8]>`,
//! but with several additional capabilities for
//! assisting creators of stateful systems. It is fully thread-safe,
//! and all operations are atomic.
//!
//! # Example
//! ```
//! # use serde::{Serialize, Deserialize};
//! #
//! # #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
//! # enum Animal {
//! #     Dog,
//! #    Cat,
//! # }
//!
//! # pub fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = sled::Config::new().temporary(true);
//! let db = config.open().unwrap();
//!
//! // The id is used by sled to identify which Tree in the database (db) to open.
//! let animals = typed_sled::Tree::<String, Animal>::open(&db, "unique_id");
//!
//! let larry = "Larry".to_string();
//! animals.insert(&larry, &Animal::Dog)?;
//! assert_eq!(animals.get(&larry)?, Some(Animal::Dog));
//! # Ok(()) }
//! ```
//!
//! [sled]: https://docs.rs/sled/latest/sled/
use core::fmt;
use core::iter::{DoubleEndedIterator, Iterator};
use core::ops::{Bound, RangeBounds};
use serde::Serialize;
use sled::{
    transaction::{ConflictableTransactionResult, TransactionResult},
    IVec, Result,
};
use std::marker::PhantomData;

pub use sled;

#[cfg(feature = "convert")]
pub mod convert;

#[cfg(feature = "key-generating")]
pub mod key_generating;

#[cfg(feature = "search")]
pub mod search;

// pub trait Bin = DeserializeOwned + Serialize + Clone + Send + Sync;

/// A flash-sympathetic persistent lock-free B+ tree.
///
/// A `Tree` represents a single logical keyspace / namespace / bucket.
///
/// # Example
/// ```
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// # enum Animal {
/// #     Dog,
/// #    Cat,
/// # }
///
/// # pub fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = sled::Config::new().temporary(true);
/// let db = config.open().unwrap();
///
/// let animals = typed_sled::Tree::<String, Animal>::open(&db, "animals");
/// animals.insert(&"Larry".to_string(), &Animal::Dog);
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct Tree<K, V> {
    inner: sled::Tree,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
}

// Manual implementation to make ToOwned behave better.
// With derive(Clone) to_owned() on a reference returns a reference.
impl<K, V> Clone for Tree<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _key: PhantomData,
            _value: PhantomData,
        }
    }
}

/// Trait alias for bounds required on keys and values.
/// For now only types that implement DeserializeOwned
/// are supported.
// [specilization] might make
// supporting any type that implements Deserialize<'a>
// possible without much overhead. Otherwise the branch
// custom_de_serialization introduces custom (de)serialization
// for each `Tree` which might also make it possible.
//
// [specilization]: https://github.com/rust-lang/rust/issues/31844
pub trait KV: serde::de::DeserializeOwned + Serialize {}

impl<T: serde::de::DeserializeOwned + Serialize> KV for T {}

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

// implemented like this in the sled source
impl<V: std::fmt::Debug> std::error::Error for CompareAndSwapError<V> {}

// These Trait bounds should probably be specified on the functions themselves, but too lazy.
impl<K, V> Tree<K, V> {
    /// Initialize a typed tree. The id identifies the tree to be opened from the db.
    /// # Example
    ///
    /// ```
    /// # use serde::{Serialize, Deserialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    /// # enum Animal {
    /// #     Dog,
    /// #    Cat,
    /// # }
    ///
    /// # pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = sled::Config::new().temporary(true);
    /// let db = config.open().unwrap();
    ///
    /// let animals = typed_sled::Tree::<String, Animal>::open(&db, "animals");
    /// animals.insert(&"Larry".to_string(), &Animal::Dog)?;
    /// # Ok(()) }
    /// ```
    pub fn open<T: AsRef<str>>(db: &sled::Db, id: T) -> Self {
        Self {
            inner: db.open_tree(id.as_ref()).unwrap(),
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    /// Insert a key to a new value, returning the last value if it was set.
    pub fn insert(&self, key: &K, value: &V) -> Result<Option<V>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .insert(serialize(key), serialize(value))
            .map(|opt| opt.map(|old_value| deserialize(&old_value)))
    }

    /// Perform a multi-key serializable transaction.
    pub fn transaction<F, A, E>(&self, f: F) -> TransactionResult<A, E>
    where
        F: Fn(&TransactionalTree<K, V>) -> ConflictableTransactionResult<A, E>,
    {
        self.inner.transaction(|sled_transactional_tree| {
            f(&TransactionalTree {
                inner: sled_transactional_tree,
                _key: PhantomData,
                _value: PhantomData,
            })
        })
    }

    /// Create a new batched update that can be atomically applied.
    ///
    /// It is possible to apply a Batch in a transaction as well, which is the way you can apply a Batch to multiple Trees atomically.
    pub fn apply_batch(&self, batch: Batch<K, V>) -> Result<()> {
        self.inner.apply_batch(batch.inner)
    }

    /// Retrieve a value from the Tree if it exists.
    pub fn get(&self, key: &K) -> Result<Option<V>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .get(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    /// Retrieve a value from the Tree if it exists. The key must be in serialized form.
    pub fn get_from_raw<B: AsRef<[u8]>>(&self, key_bytes: B) -> Result<Option<V>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .get(key_bytes.as_ref())
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    /// Deserialize a key and retrieve it's value from the Tree if it exists.
    /// The deserialization is only done if a value was retrieved successfully.
    pub fn get_kv_from_raw<B: AsRef<[u8]>>(&self, key_bytes: B) -> Result<Option<(K, V)>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .get(key_bytes.as_ref())
            .map(|opt| opt.map(|v| (deserialize(key_bytes.as_ref()), deserialize(&v))))
    }

    /// Delete a value, returning the old value if it existed.
    pub fn remove(&self, key: &K) -> Result<Option<V>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .remove(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
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
    ) -> Result<core::result::Result<(), CompareAndSwapError<V>>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .compare_and_swap(
                serialize(key),
                old.map(|old| serialize(old)),
                new.map(|new| serialize(new)),
            )
            .map(|cas_res| {
                cas_res.map_err(|cas_err| CompareAndSwapError {
                    current: cas_err.current.as_ref().map(|b| deserialize(b)),
                    proposed: cas_err.proposed.as_ref().map(|b| deserialize(b)),
                })
            })
    }

    /// Fetch the value, apply a function to it and return the result.
    // not sure if implemented correctly (different trait bound for F)
    pub fn update_and_fetch<F>(&self, key: &K, mut f: F) -> Result<Option<V>>
    where
        K: KV,
        V: KV,
        F: FnMut(Option<V>) -> Option<V>,
    {
        self.inner
            .update_and_fetch(serialize(&key), |opt_value| {
                f(opt_value.map(|v| deserialize(v))).map(|v| serialize(&v))
            })
            .map(|res| res.map(|v| deserialize(&v)))
    }

    /// Fetch the value, apply a function to it and return the previous value.
    // not sure if implemented correctly (different trait bound for F)
    pub fn fetch_and_update<F>(&self, key: &K, mut f: F) -> Result<Option<V>>
    where
        K: KV,
        V: KV,
        F: FnMut(Option<V>) -> Option<V>,
    {
        self.inner
            .fetch_and_update(serialize(key), |opt_value| {
                f(opt_value.map(|v| deserialize(v))).map(|v| serialize(&v))
            })
            .map(|res| res.map(|v| deserialize(&v)))
    }

    /// Subscribe to `Event`s that happen to keys that have
    /// the specified prefix. Events for particular keys are
    /// guaranteed to be witnessed in the same order by all
    /// threads, but threads may witness different interleavings
    /// of `Event`s across different keys. If subscribers don't
    /// keep up with new writes, they will cause new writes
    /// to block. There is a buffer of 1024 items per
    /// `Subscriber`. This can be used to build reactive
    /// and replicated systems.
    pub fn watch_prefix(&self, prefix: &K) -> Subscriber<K, V>
    where
        K: KV,
    {
        Subscriber::from_sled(self.inner.watch_prefix(serialize(prefix)))
    }

    /// Subscribe to  all`Event`s. Events for particular keys are
    /// guaranteed to be witnessed in the same order by all
    /// threads, but threads may witness different interleavings
    /// of `Event`s across different keys. If subscribers don't
    /// keep up with new writes, they will cause new writes
    /// to block. There is a buffer of 1024 items per
    /// `Subscriber`. This can be used to build reactive
    /// and replicated systems.
    pub fn watch_all(&self) -> Subscriber<K, V>
    where
        K: KV,
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
        K: KV,
    {
        self.inner.contains_key(serialize(key))
    }

    /// Retrieve the key and value before the provided key,
    /// if one exists.
    pub fn get_lt(&self, key: &K) -> Result<Option<(K, V)>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .get_lt(serialize(key))
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    /// Retrieve the next key and value from the `Tree` after the
    /// provided key.
    pub fn get_gt(&self, key: &K) -> Result<Option<(K, V)>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .get_gt(serialize(key))
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
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
    pub fn merge(&self, key: &K, value: &V) -> Result<Option<V>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .merge(serialize(key), serialize(value))
            .map(|res| res.map(|old_v| deserialize(&old_v)))
    }

    // TODO: implement using own MergeOperator trait
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
    pub fn set_merge_operator(&self, merge_operator: impl MergeOperator<K, V> + 'static)
    where
        K: KV,
        V: KV,
    {
        self.inner
            .set_merge_operator(move |key: &[u8], old_v: Option<&[u8]>, value: &[u8]| {
                let opt_v = merge_operator(
                    deserialize(key),
                    old_v.map(|v| deserialize(v)),
                    deserialize(value),
                );
                opt_v.map(|v| serialize(&v))
            });
    }

    /// Create a double-ended iterator over the tuples of keys and
    /// values in this tree.
    pub fn iter(&self) -> Iter<K, V> {
        Iter::from_sled(self.inner.iter())
    }

    /// Create a double-ended iterator over tuples of keys and values,
    /// where the keys fall within the specified range.
    pub fn range<R: RangeBounds<K>>(&self, range: R) -> Iter<K, V>
    where
        K: KV + std::fmt::Debug,
    {
        match (range.start_bound(), range.end_bound()) {
            (Bound::Unbounded, Bound::Unbounded) => {
                return Iter::from_sled(self.inner.range::<&[u8], _>(..))
            }
            (Bound::Unbounded, Bound::Excluded(b)) => {
                return Iter::from_sled(self.inner.range(..serialize(b)))
            }
            (Bound::Unbounded, Bound::Included(b)) => {
                return Iter::from_sled(self.inner.range(..=serialize(b)))
            }
            // FIX: This is not excluding lower bound.
            (Bound::Excluded(b), Bound::Unbounded) => {
                return Iter::from_sled(self.inner.range(serialize(b)..))
            }
            (Bound::Excluded(b), Bound::Excluded(bb)) => {
                return Iter::from_sled(self.inner.range(serialize(b)..serialize(bb)))
            }
            (Bound::Excluded(b), Bound::Included(bb)) => {
                return Iter::from_sled(self.inner.range(serialize(b)..=serialize(bb)))
            }
            (Bound::Included(b), Bound::Unbounded) => {
                return Iter::from_sled(self.inner.range(serialize(b)..))
            }
            (Bound::Included(b), Bound::Excluded(bb)) => {
                return Iter::from_sled(self.inner.range(serialize(b)..serialize(bb)))
            }
            (Bound::Included(b), Bound::Included(bb)) => {
                return Iter::from_sled(self.inner.range(serialize(b)..=serialize(bb)))
            }
        }
    }

    /// Create an iterator over tuples of keys and values,
    /// where the all the keys starts with the given prefix.
    pub fn scan_prefix(&self, prefix: &K) -> Iter<K, V>
    where
        K: KV,
    {
        Iter::from_sled(self.inner.scan_prefix(serialize(prefix)))
    }

    /// Returns the first key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn first(&self) -> Result<Option<(K, V)>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .first()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    /// Returns the last key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    pub fn last(&self) -> Result<Option<(K, V)>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .last()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    pub fn pop_max(&self) -> Result<Option<(K, V)>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .pop_max()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    pub fn pop_min(&self) -> Result<Option<(K, V)>>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .pop_min()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
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

pub trait MergeOperator<K, V>: Fn(K, Option<V>, V) -> Option<V> {}

pub struct TransactionalTree<'a, K, V> {
    inner: &'a sled::transaction::TransactionalTree,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
}

impl<'a, K, V> TransactionalTree<'a, K, V> {
    pub fn insert(
        &self,
        key: &K,
        value: &V,
    ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .insert(serialize(key), serialize(value))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    pub fn remove(
        &self,
        key: &K,
    ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .remove(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    pub fn get(
        &self,
        key: &K,
    ) -> std::result::Result<Option<V>, sled::transaction::UnabortableTransactionError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .get(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    pub fn apply_batch(
        &self,
        batch: &Batch<K, V>,
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

pub struct Iter<K, V> {
    inner: sled::Iter,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
}

impl<K: KV, V: KV> Iterator for Iter<K, V> {
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    fn last(mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }
}

impl<K: KV, V: KV> DoubleEndedIterator for Iter<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }
}

impl<K, V> Iter<K, V> {
    pub fn from_sled(iter: sled::Iter) -> Self {
        Iter {
            inner: iter,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    pub fn keys(self) -> impl DoubleEndedIterator<Item = Result<K>> + Send + Sync
    where
        K: KV + Send + Sync,
        V: KV + Send + Sync,
    {
        self.map(|r| r.map(|(k, _v)| k))
    }

    /// Iterate over the values of this Tree
    pub fn values(self) -> impl DoubleEndedIterator<Item = Result<V>> + Send + Sync
    where
        K: KV + Send + Sync,
        V: KV + Send + Sync,
    {
        self.map(|r| r.map(|(_k, v)| v))
    }
}

#[derive(Clone, Debug)]
pub struct Batch<K, V> {
    inner: sled::Batch,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
}

impl<K, V> Batch<K, V> {
    pub fn insert(&mut self, key: &K, value: &V)
    where
        K: KV,
        V: KV,
    {
        self.inner.insert(serialize(key), serialize(value));
    }

    pub fn remove(&mut self, key: &K)
    where
        K: KV,
    {
        self.inner.remove(serialize(key))
    }
}

// Implementing Default manually to not require K and V to implement Default.
impl<K, V> Default for Batch<K, V> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            _key: PhantomData,
            _value: PhantomData,
        }
    }
}

use pin_project::pin_project;
#[pin_project]
pub struct Subscriber<K, V> {
    #[pin]
    inner: sled::Subscriber,
    _key: PhantomData<fn() -> K>,
    _value: PhantomData<fn() -> V>,
}

impl<K, V> Subscriber<K, V> {
    pub fn next_timeout(
        &mut self,
        timeout: core::time::Duration,
    ) -> core::result::Result<Event<K, V>, std::sync::mpsc::RecvTimeoutError>
    where
        K: KV,
        V: KV,
    {
        self.inner
            .next_timeout(timeout)
            .map(|e| Event::from_sled(&e))
    }

    pub fn from_sled(subscriber: sled::Subscriber) -> Self {
        Self {
            inner: subscriber,
            _key: PhantomData,
            _value: PhantomData,
        }
    }
}

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
impl<K: KV + Unpin, V: KV + Unpin> Future for Subscriber<K, V> {
    type Output = Option<Event<K, V>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project()
            .inner
            .poll(cx)
            .map(|opt| opt.map(|e| Event::from_sled(&e)))
    }
}

impl<K: KV, V: KV> Iterator for Subscriber<K, V> {
    type Item = Event<K, V>;

    fn next(&mut self) -> Option<Event<K, V>> {
        self.inner.next().map(|e| Event::from_sled(&e))
    }
}

pub enum Event<K, V> {
    Insert { key: K, value: V },
    Remove { key: K },
}

impl<K, V> Event<K, V> {
    pub fn key(&self) -> &K
    where
        K: KV,
    {
        match self {
            Self::Insert { key, .. } | Self::Remove { key } => key,
        }
    }

    pub fn from_sled(event: &sled::Event) -> Self
    where
        K: KV,
        V: KV,
    {
        match event {
            sled::Event::Insert { key, value } => Self::Insert {
                key: deserialize(key),
                value: deserialize(value),
            },
            sled::Event::Remove { key } => Self::Remove {
                key: deserialize(key),
            },
        }
    }
}

/// The function which is used to deserialize all keys and values.
pub fn deserialize<'a, T>(bytes: &'a [u8]) -> T
where
    T: serde::de::Deserialize<'a>,
{
    bincode::deserialize(bytes).expect("deserialization failed, did the type serialized change?")
}

/// The function which is used to serialize all keys and values.
pub fn serialize<T>(value: &T) -> Vec<u8>
where
    T: serde::Serialize,
{
    bincode::serialize(value).expect("serialization failed, did the type serialized change?")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range() {
        let config = sled::Config::new().temporary(true);
        let db = config.open().unwrap();

        let tree: Tree<u32, u32> = Tree::open(&db, "test_tree");

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

        let tree: Tree<u32, u32> = Tree::open(&db, "test_tree");

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
