#![feature(trait_alias)]
//! Sled with Types instead of Bytes.
//! This crate builds on top of `sled` and handles the (de)serialization
//! of keys and values that are inserted into a sled::Tree for you.
//! It hasn't been tested extensively.
//!
//! Some info about sled:
//! `sled` is a high-performance embedded database with
//! an API that is similar to a `BTreeMap<[u8], [u8]>`,
//! but with several additional capabilities for
//! assisting creators of stateful systems. It is fully thread-safe,
//! and all operations are atomic.
//!
//! # Example
/// ```
/// struct Wrapper {
///     inner: sled::Tree,
/// }

/// impl Tree for Wrapper {
///     type Key = u64;
///     type Value = String;

///     fn tree(&self) -> &sled::Tree {
///         &self.inner
///     }
/// }
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db: sled::Db = sled::open("db")?;
/// let tree: sled::Tree = Wrapper{ inner: db.open_tree(b"tree for Wrapper")? };
/// tree.insert(100, "one hundred");
/// # Ok(()) }
/// ```
use async_trait::async_trait;
use core::iter::{DoubleEndedIterator, Iterator};
use serde::{de::DeserializeOwned, Serialize};
use sled::{
    transaction::{ConflictableTransactionResult, TransactionResult, TransactionalTree},
    CompareAndSwapError, IVec, Result,
};
use std::marker::PhantomData;

pub use sled;

pub trait Bin = DeserializeOwned + Serialize + Clone + Send + Sync;

/// A flash-sympathetic persistent lock-free B+ tree.
///
/// A `Tree` represents a single logical keyspace / namespace / bucket.
///
/// # Example
///
/// ```
/// struct Wrapper {
///     inner: sled::Tree,
/// }

/// impl Tree for Wrapper {
///     type Key = u64;
///     type Value = String;

///     fn tree(&self) -> &sled::Tree {
///         &self.inner
///     }
/// }
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db: sled::Db = sled::open("db")?;
/// let tree: sled::Tree = Wrapper{ inner: db.open_tree(b"tree for Wrapper")? };
/// tree.insert(100, "one hundred");
/// # Ok(()) }
/// ```
struct Tree<K, V> {
    inner: sled::Tree,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

// These Trait bounds should probably be specified on the functions themselves, but too lazy.
impl<K: Bin, V: Bin> Tree<K, V> {
    /// Insert a key to a new value, returning the last value if it was set.
    fn insert(&self, key: &K, value: &V) -> Result<Option<V>> {
        self.inner
            .insert(serialize(key), serialize(value))
            .map(|opt| opt.map(|old_value| deserialize(&old_value)))
    }

    /// Perform a multi-key serializable transaction.
    fn transaction<F, A, E>(&self, f: F) -> TransactionResult<A, E>
    where
        F: Fn(&TransactionalTree) -> ConflictableTransactionResult<A, E>,
    {
        self.inner.transaction(f)
    }

    /// Create a new batched update that can be atomically applied.
    ///
    /// It is possible to apply a Batch in a transaction as well, which is the way you can apply a Batch to multiple Trees atomically.
    fn apply_batch(&self, batch: Batch<K, V>) -> Result<()> {
        self.inner.apply_batch(batch.inner)
    }

    /// Retrieve a value from the Tree if it exists.
    fn get(&self, key: &K) -> Result<Option<V>> {
        self.inner
            .get(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    /// Delete a value, returning the old value if it existed.
    fn remove(&self, key: &K) -> Result<Option<V>> {
        self.inner
            .remove(serialize(key))
            .map(|opt| opt.map(|v| deserialize(&v)))
    }

    ///     Compare and swap. Capable of unique creation, conditional modification, or deletion. If old is None, this will only set the value if it doesn't exist yet. If new is None, will delete the value if old is correct. If both old and new are Some, will modify the value if old is correct.
    ///
    /// It returns Ok(Ok(())) if operation finishes successfully.
    ///
    /// If it fails it returns: - Ok(Err(CompareAndSwapError(current, proposed))) if operation failed to setup a new value. CompareAndSwapError contains current and proposed values. - Err(Error::Unsupported) if the database is opened in read-only mode.
    fn compare_and_swap(
        &self,
        key: &K,
        old: Option<&V>,
        new: Option<&V>,
    ) -> Result<core::result::Result<(), CompareAndSwapError>> {
        self.inner.compare_and_swap(
            serialize(key),
            old.map(|old| serialize(old)),
            new.map(|new| serialize(new)),
        )
    }

    /// Fetch the value, apply a function to it and return the result.
    // not sure if implemented correctly (different trait bound for F)
    fn update_and_fetch<F>(&self, key: &K, mut f: F) -> Result<Option<V>>
    where
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
    fn fetch_and_update<F>(&self, key: &K, mut f: F) -> Result<Option<V>>
    where
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
    fn watch_prefix(&self, prefix: &V) -> Subscriber<K, V> {
        Subscriber::from_sled(self.inner.watch_prefix(serialize(prefix)))
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
    fn flush(&self) -> Result<usize> {
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
    async fn flush_async(&self) -> Result<usize> {
        self.inner.flush_async().await
    }

    /// Returns `true` if the `Tree` contains a value for
    /// the specified key.
    fn contains_key(&self, key: &K) -> Result<bool> {
        self.inner.contains_key(serialize(key))
    }

    /// Retrieve the key and value before the provided key,
    /// if one exists.
    fn get_lt(&self, key: &K) -> Result<Option<(K, V)>> {
        self.inner
            .get_lt(serialize(key))
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    /// Retrieve the next key and value from the `Tree` after the
    /// provided key.
    fn get_gt(&self, key: &K) -> Result<Option<(K, V)>> {
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
    fn merge(&self, key: &K, value: &V) -> Result<Option<V>> {
        self.inner
            .merge(serialize(key), serialize(value))
            .map(|res| res.map(|old_v| deserialize(&old_v)))
    }

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
    fn set_merge_operator(&self, merge_operator: impl sled::MergeOperator + 'static) {
        self.inner.set_merge_operator(merge_operator);
    }

    /// Create a double-ended iterator over the tuples of keys and
    /// values in this tree.
    fn iter(&self) -> Iter<K, V> {
        Iter::from_sled(self.inner.iter())
    }

    /// Create a double-ended iterator over tuples of keys and values,
    /// where the keys fall within the specified range.
    fn range<R: core::ops::RangeBounds<K>>(&self, range: R) -> Iter<K, V> {
        Iter::from_sled(
            self.inner
                .range(serialize(&range.start_bound())..serialize(&range.end_bound())),
        )
    }

    /// Create an iterator over tuples of keys and values,
    /// where the all the keys starts with the given prefix.
    fn scan_prefix(&self, prefix: &V) -> Iter<K, V> {
        Iter::from_sled(self.inner.scan_prefix(serialize(prefix)))
    }

    /// Returns the first key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    fn first(&self) -> Result<Option<(K, V)>> {
        self.inner
            .first()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    /// Returns the last key and value in the `Tree`, or
    /// `None` if the `Tree` is empty.
    fn last(&self) -> Result<Option<(K, V)>> {
        self.inner
            .last()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    /// Atomically removes the maximum item in the `Tree` instance.
    fn pop_max(&self) -> Result<Option<(K, V)>> {
        self.inner
            .pop_max()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    /// Atomically removes the minimum item in the `Tree` instance.
    fn pop_min(&self) -> Result<Option<(K, V)>> {
        self.inner
            .pop_min()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }

    /// Returns the number of elements in this tree.
    fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the `Tree` contains no elements.
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clears the `Tree`, removing all values.
    ///
    /// Note that this is not atomic.
    fn clear(&self) -> Result<()> {
        self.inner.clear()
    }

    /// Returns the name of the tree.
    fn name(&self) -> IVec {
        self.inner.name()
    }

    /// Returns the CRC32 of all keys and values
    /// in this Tree.
    ///
    /// This is O(N) and locks the underlying tree
    /// for the duration of the entire scan.
    fn checksum(&self) -> Result<u32> {
        self.inner.checksum()
    }
}

pub struct Iter<K, V> {
    inner: sled::Iter,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

impl<K: Bin, V: Bin> Iterator for Iter<K, V> {
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

impl<K: Bin, V: Bin> DoubleEndedIterator for Iter<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|res| res.map(|(k, v)| (deserialize(&k), deserialize(&v))))
    }
}

impl<K: Bin, V: Bin> Iter<K, V> {
    fn from_sled(iter: sled::Iter) -> Self {
        Iter {
            inner: iter,
            phantom_key: PhantomData,
            phantom_value: PhantomData,
        }
    }

    pub fn keys(self) -> impl DoubleEndedIterator<Item = Result<K>> + Send + Sync
    where
        K: Send + Sync,
        V: Send + Sync,
    {
        self.map(|r| r.map(|(k, _v)| k))
    }

    /// Iterate over the values of this Tree
    pub fn values(self) -> impl DoubleEndedIterator<Item = Result<V>> + Send + Sync
    where
        K: Send + Sync,
        V: Send + Sync,
    {
        self.map(|r| r.map(|(_k, v)| v))
    }
}

#[derive(Default, Clone, Debug)]
pub struct Batch<K, V> {
    inner: sled::Batch,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

impl<K: Bin, V: Bin> Batch<K, V> {
    fn insert(&mut self, key: K, value: V) {
        self.inner.insert(serialize(&key), serialize(&value));
    }

    fn remove(&mut self, key: K) {
        self.inner.remove(serialize(&key))
    }
}

pub struct Subscriber<K, V> {
    inner: sled::Subscriber,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

impl<K: Bin, V: Bin> Subscriber<K, V> {
    pub fn next_timeout(
        &mut self,
        timeout: core::time::Duration,
    ) -> core::result::Result<Event<K, V>, std::sync::mpsc::RecvTimeoutError> {
        self.inner
            .next_timeout(timeout)
            .map(|e| Event::from_sled(&e))
    }

    pub fn from_sled(subscriber: sled::Subscriber) -> Self {
        Self {
            inner: subscriber,
            phantom_key: PhantomData,
            phantom_value: PhantomData,
        }
    }
}

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
impl<K: Bin + Unpin, V: Bin + Unpin> Future for Subscriber<K, V> {
    type Output = Option<Event<K, V>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner)
            .poll(cx)
            .map(|opt| opt.map(|e| Event::from_sled(&e)))
    }
}

impl<K: Bin, V: Bin> Iterator for Subscriber<K, V> {
    type Item = Event<K, V>;

    fn next(&mut self) -> Option<Event<K, V>> {
        self.inner.next().map(|e| Event::from_sled(&e))
    }
}

pub enum Event<K: Bin, V: Bin> {
    Insert { key: K, value: V },
    Remove { key: K },
}

impl<K: Bin, V: Bin> Event<K, V> {
    pub fn key(&self) -> &K {
        match self {
            Self::Insert { key, .. } | Self::Remove { key } => key,
        }
    }

    pub fn from_sled(event: &sled::Event) -> Self {
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

pub fn deserialize<'a, T>(bytes: &'a [u8]) -> T
where
    T: serde::de::Deserialize<'a>,
{
    bincode::deserialize(bytes).unwrap()
}

pub fn serialize<T>(value: &T) -> Vec<u8>
where
    T: serde::Serialize,
{
    bincode::serialize(value).unwrap()
}